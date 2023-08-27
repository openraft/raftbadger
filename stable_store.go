/*
 * Copyright (c) 2023 Zander Schwid & Co. LLC.
 * SPDX-License-Identifier: BUSL-1.1
 */

package raftbadger

import (
	"encoding/binary"
	"github.com/dgraph-io/badger/v3"
	"github.com/hashicorp/raft"
	"github.com/pkg/errors"
)

type stableStore struct {
	db          *badger.DB
	prefix      []byte
}

func NewStableStore(db *badger.DB, prefix []byte) raft.StableStore {
	return &stableStore {
		db: db,
		prefix: prefix,
	}
}

func (t* stableStore) Set(key []byte, val []byte) error {

	tx := t.db.NewTransaction(true)
	defer tx.Discard()

	entry := &badger.Entry{ Key: append(t.prefix, key...), Value: val }

	err := tx.SetEntry(entry)
	if err != nil {
		return err
	}

	return tx.Commit()
}

// Get returns the value for key, or an empty byte slice if key was not found.
func (t* stableStore) Get(key []byte) ([]byte, error) {
	return t.getImpl(key, nil)
}

func (t* stableStore) getImpl(key, dst []byte) ([]byte, error) {

	tx := t.db.NewTransaction(false)
	defer tx.Discard()

	rawKey := append(t.prefix, key...)

	item, err := tx.Get(rawKey)
	if err != nil {

		if err == badger.ErrKeyNotFound {
			return dst, nil
		}

		return nil, err
	}

	data, err := item.ValueCopy(dst)
	if err != nil {
		return nil, errors.Errorf("badger fetch value failed '%v', %v", rawKey, err)
	}

	return data, nil

}

func (t* stableStore) SetUint64(key []byte, val uint64) error {

	var data [8]byte
	buf := data[:]

	binary.BigEndian.PutUint64(buf, val)

	return t.Set(key, buf)

}

// GetUint64 returns the uint64 value for key, or 0 if key was not found.
func (t* stableStore) GetUint64(key []byte) (uint64, error) {

	var data [8]byte

	buf, err := t.getImpl(key, data[:])
	if err != nil {
		return 0, err
	}

	return binary.BigEndian.Uint64(buf), nil
}
