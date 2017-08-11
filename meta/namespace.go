package meta

import (
	"bytes"

	"github.com/juju/errors"
)

var (
	mNamespacesKey = []byte("NamespacesKey")
)

func (m *Meta) CreateNamespace(ns []byte) error {
	m.txn.RevertToOriginPrefix()
	err := m.txn.HSet(mNamespacesKey, ns, ns)
	return errors.Trace(err)
}

func (m *Meta) GetNamespaces() ([][]byte, error) {
	m.txn.RevertToOriginPrefix()
	nss, err := m.txn.HGetAll(mNamespacesKey)
	if err != nil {
		return nil, errors.Trace(err)
	}
	ret := [][]byte{}
	for _, item := range nss {
		ret = append(ret, item.Value)
	}
	return ret, nil
}

func (m *Meta) HasNamespace(ns []byte) (bool, error) {
	m.txn.RevertToOriginPrefix()
	keys, err := m.txn.HKeys(mNamespacesKey)
	if err != nil {
		return false, errors.Trace(err)
	}
	for _, key := range keys {
		if bytes.Equal(key, ns) {
			return true, nil
		}
	}
	return false, nil
}

func (m *Meta) DelNamespace(ns []byte) error {
	m.txn.RevertToOriginPrefix()
	err := m.txn.HDel(mNamespacesKey, ns)
	return errors.Trace(err)
}

func (m *Meta) ClearNamespace() error {
	var err error
	// Clean mDDLJobListKey
	err = m.txn.LClear(mDDLJobListKey)
	if err != nil {
		return errors.Trace(err)
	}
	// Clean mDDLJobHistoryKey
	err = m.txn.HClear(mDDLJobHistoryKey)
	if err != nil {
		return errors.Trace(err)
	}
	// Clean mDDLJobReorgKey
	err = m.txn.HClear(mDDLJobReorgKey)
	if err != nil {
		return errors.Trace(err)
	}
	// Clean mBgJobListKey
	err = m.txn.LClear(mBgJobListKey)
	if err != nil {
		return errors.Trace(err)
	}
	// Clean mBgJobHistoryKey
	err = m.txn.HClear(mBgJobHistoryKey)
	if err != nil {
		return errors.Trace(err)
	}
	// Clean mDBS
	err = m.txn.HClear(mDBs)
	if err != nil {
		return errors.Trace(err)
	}
	// Clean SchemaVersionKey
	err = m.txn.Clear(mSchemaVersionKey)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
