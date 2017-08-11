// Copyright 2015 PingCAP, Inc.
//
// Licensed under the Apache License, Version 2.0 (the "License");
// you may not use this file except in compliance with the License.
// You may obtain a copy of the License at
//
//     http://www.apache.org/licenses/LICENSE-2.0
//
// Unless required by applicable law or agreed to in writing, software
// distributed under the License is distributed on an "AS IS" BASIS,
// See the License for the specific language governing permissions and
// limitations under the License.

package main

import (
	"flag"
	"fmt"
	"runtime"
	"time"

	"github.com/coreos/etcd/clientv3"
	"github.com/juju/errors"
	"github.com/ngaut/log"
	"github.com/pingcap/tidb"
	"github.com/pingcap/tidb/ddl"
	"github.com/pingcap/tidb/kv"
	"github.com/pingcap/tidb/meta"
	"github.com/pingcap/tidb/model"
	"github.com/pingcap/tidb/mysql"
	"github.com/pingcap/tidb/store/localstore/boltdb"
	"github.com/pingcap/tidb/store/tikv"
	"github.com/pingcap/tidb/structure"
)

var (
	store     = flag.String("store", "goleveldb", "registered store name, [memory, goleveldb, boltdb, tikv, mocktikv]")
	storePath = flag.String("path", "/tmp/tidb", "tidb storage path")
	logLevel  = flag.String("L", "info", "log level: info, debug, warn, error, fatal")
)

func help() {
	fmt.Println("tidb-ns [params] command ...")
	fmt.Println("commands:")
	fmt.Println("    list       list all namespaces")
	fmt.Println("    show [ns]  show namespace databases  and tables")
	fmt.Println("    jobs [ns]  show namespace jobs")
	fmt.Println("    rm   [ns]  remove namespace")
	fmt.Println("")
	flag.PrintDefaults()
}

func main() {
	log.SetLevelByString(*logLevel)
	tidb.RegisterLocalStore("boltdb", boltdb.Driver{})
	tidb.RegisterStore("tikv", tikv.Driver{})
	tidb.RegisterStore("mocktikv", tikv.MockDriver{})

	runtime.GOMAXPROCS(runtime.NumCPU())

	flag.Parse()
	args := flag.Args()
	if len(args) == 0 {
		help()
		return
	}
	store := createStore()
	switch args[0] {
	case "list", "ls":
		runCmdList(store)
	case "show", "cat":
		if len(args) != 2 {
			help()
			return
		}
		runCmdShow(store, []byte(args[1]))
	case "rm":
		if len(args) != 2 {
			help()
			return
		}
		runCmdRemove(store, []byte(args[1]))
	case "jobs":
		if len(args) != 2 {
			help()
			return
		}
		runCmdJobs(store, []byte(args[1]))
	default:
		help()
	}
}

func createStore() kv.Storage {
	fullPath := fmt.Sprintf("%s://%s", *store, *storePath)
	store, err := tidb.NewStore(fullPath)
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
	return store
}

func runWithMeta(store kv.Storage, f func(*meta.Meta) error) {
	err := kv.RunInNewTxn(store, false, func(txn kv.Transaction) error {
		var err error
		m := meta.NewMeta(txn)
		err = f(m)
		return errors.Trace(err)
	})
	if err != nil {
		log.Fatal(errors.ErrorStack(err))
	}
}

func updateNamespace(store kv.Storage, ns []byte) {
	if len(ns) > 0 {
		structure.Namespace = ns
		ddl.UpdateDDLOwnerKeyWithNS(ns)
		ddl.UpdateDDLSchemaVersionsWithNS(ns)
	} else {
		structure.Namespace = nil
	}
}

func runCmdList(store kv.Storage) {
	runWithMeta(store, func(m *meta.Meta) error {
		nss, err := m.GetNamespaces()
		if err != nil {
			return err
		}
		fmt.Println("Namespaces:")
		for _, ns := range nss {
			fmt.Println("\t", string(ns))
		}
		return nil
	})
}

func formatDB(db *model.DBInfo) string {
	return fmt.Sprintf("ID: %d, Name: %s, Charset: %s, Collate: %s, State: %v",
		db.ID, db.Name.String(), db.Charset, db.Collate, db.State)
}

func formatTable(tbl *model.TableInfo) string {
	return fmt.Sprintf("ID: %d, Name: %s, Charset: %s, Collate: %s, State: %v, Comment: %s, AutoIncID: %d",
		tbl.ID, tbl.Name.String(), tbl.Charset, tbl.Collate, tbl.State, tbl.Comment, tbl.AutoIncID)
}

func hasNamespace(store kv.Storage, ns []byte) bool {
	hasNS := false
	runWithMeta(store, func(m *meta.Meta) error {
		var err error
		hasNS, err = m.HasNamespace(ns)
		return err
	})
	return hasNS
}

func runCmdShow(store kv.Storage, ns []byte) {
	updateNamespace(store, ns)
	if !hasNamespace(store, ns) {
		fmt.Println("Do not has Namespace:", string(ns))
		return
	}
	runWithMeta(store, func(m *meta.Meta) error {
		dbs, err := m.ListDatabases()
		if err != nil {
			return err
		}
		for _, db := range dbs {
			fmt.Println("Database:", formatDB(db))
			tbls, err := m.ListTables(db.ID)
			if err == nil {
				fmt.Println("  Tables:")
				for _, tbl := range tbls {
					fmt.Println("    ", formatTable(tbl))
				}
			} else {
				log.Error(err)
			}
		}
		return nil
	})
}

func runCmdRemove(store kv.Storage, ns []byte) {
	updateNamespace(store, ns)
	cleanEtcd(store, true)
	if !hasNamespace(store, ns) {
		fmt.Println("Do not has Namespace:", string(ns))
		return
	}
	cleanNamespace(store)
	runWithMeta(store, func(m *meta.Meta) error {
		return m.ClearNamespace()
	})
	runWithMeta(store, func(m *meta.Meta) error {
		err := m.FinishBootstrap(0)
		if err != nil {
			return err
		}
		return m.DelNamespace(ns)
	})
	cleanEtcd(store, false)
}

func runCmdJobs(store kv.Storage, ns []byte) {
	updateNamespace(store, ns)
	if !hasNamespace(store, ns) {
		fmt.Println("Do not has Namespace:", string(ns))
		return
	}
	runWithMeta(store, func(m *meta.Meta) error {
		ddlJobs, err := m.DDLJobQueueLen()
		if err != nil {
			return err
		}
		bgJobs, err := m.BgJobQueueLen()
		if err != nil {
			return err
		}
		fmt.Println("DDL Jobs:", ddlJobs)
		fmt.Println("Background Jobs:", bgJobs)
		return nil
	})
}

// Below is clean Namespace codes

func cleanNamespace(store kv.Storage) {
	sess, err := tidb.CreateSession(store)
	if err != nil {
		log.Fatal(err)
	}
	var dbs []*model.DBInfo
	runWithMeta(store, func(m *meta.Meta) error {
		var err error
		dbs, err = m.ListDatabases()
		return err
	})
	var sysdb *model.DBInfo
	for _, db := range dbs {
		if db.Name.String() == mysql.SystemDB {
			sysdb = db
			continue
		}
		sql := fmt.Sprintf("DROP DATABASE %s", db.Name.String())
		mustExec(sess, sql)
	}
	cleanSystemDB(sess, store, sysdb)
}

func listTables(store kv.Storage, db *model.DBInfo) []*model.TableInfo {
	var tbls []*model.TableInfo
	runWithMeta(store, func(m *meta.Meta) error {
		var err error
		tbls, err = m.ListTables(db.ID)
		return err
	})
	return tbls
}

func cleanSystemDB(s tidb.Session, store kv.Storage, db *model.DBInfo) {
	var gcrangeTbl, tidbTbl *model.TableInfo
	for _, tbl := range listTables(store, db) {
		if tbl.Name.String() == "gc_delete_range" {
			gcrangeTbl = tbl
			continue
		}
		if tbl.Name.String() == "tidb" {
			tidbTbl = tbl
			continue
		}
		sql := fmt.Sprintf("DROP TABLE `%s`.`%s`", mysql.SystemDB, tbl.Name.String())
		mustExec(s, sql)
	}
	waitRangeDeleteFinish(s, gcrangeTbl)
	runWithMeta(store, func(m *meta.Meta) error {
		err := m.DropTable(db.ID, gcrangeTbl.ID, true)
		if err != nil {
			return err
		}
		err = m.DropTable(db.ID, tidbTbl.ID, true)
		if err != nil {
			return err
		}
		return nil
	})
}

func waitRangeDeleteFinish(s tidb.Session, tbl *model.TableInfo) {
	sql := fmt.Sprintf("SELECT COUNT(*) FROM `%s`.`%s`", mysql.SystemDB, tbl.Name.String())
	for {
		ret, err := s.Execute(sql)
		if err != nil || len(ret) == 0 {
			log.Fatal(err)
		}
		defer ret[0].Close()
		row, err := ret[0].Next()
		if err != nil {
			log.Fatal(err)
		}
		n := row.Data[0].GetInt64()
		if n == 0 {
			break
		}
		fmt.Printf("Waiting %d Delete Range Job Finished\n", n)
		time.Sleep(30 * time.Second)
	}
}

func waitJobFinish(store kv.Storage) {
	for {
		var bgJobs, ddlJobs int64
		runWithMeta(store, func(m *meta.Meta) error {
			var err error
			ddlJobs, err = m.DDLJobQueueLen()
			if err != nil {
				return err
			}
			bgJobs, err = m.BgJobQueueLen()
			if err != nil {
				return err
			}
			return nil
		})
		if bgJobs == 0 && ddlJobs == 0 {
			break
		}
		fmt.Printf("Waiting Jobs (DDLJobs: %d, BGJobs: %d) Finished", ddlJobs, bgJobs)
		time.Sleep(3 * time.Second)
	}
}

func mustExec(s tidb.Session, sql string) {
	_, err := s.Execute(sql)
	if err != nil {
		log.Fatal(err)
	}
}

type etcdAddrs interface {
	EtcdAddrs() []string
}

func cleanEtcd(store kv.Storage, ownerOnly bool) {
	if eaddrs, ok := store.(etcdAddrs); ok {
		if addrs := eaddrs.EtcdAddrs(); addrs != nil {
			cli, err := clientv3.New(clientv3.Config{
				Endpoints:   addrs,
				DialTimeout: 5 * time.Second,
			})
			if err != nil {
				return
			}
			defer cli.Close()
			doCleanEtcd(cli, ownerOnly)
		}
	}
}

func doCleanEtcd(cli *clientv3.Client, ownerOnly bool) {
	kvc := clientv3.NewKV(cli)
	kvc.Delete(cli.Ctx(), ddl.DDLOwnerKey)
	if !ownerOnly {
		kvc.Delete(cli.Ctx(), ddl.DDLAllSchemaVersions)
		kvc.Delete(cli.Ctx(), ddl.DDLGlobalSchemaVersion)
	}
}
