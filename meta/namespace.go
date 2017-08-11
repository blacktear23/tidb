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
	err = m.txn.LClear(mDDLJobListKey)
	if err != nil {
		return errors.Trace(err)
	}
	err = m.txn.HClear(mDDLJobHistoryKey)
	if err != nil {
		return errors.Trace(err)
	}
	err = m.txn.HClear(mDDLJobReorgKey)
	if err != nil {
		return errors.Trace(err)
	}
	return nil
}
