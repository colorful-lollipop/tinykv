package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	db   *badger.DB
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	return &StandAloneStorage{conf: conf}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	s.db = engine_util.CreateDB(s.conf.DBPath, false)
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.db.Close()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.db.NewTransaction(false)
	return &standAloneStorageReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			switch modify.Data.(type) {
			case storage.Put:
				put := modify.Data.(storage.Put)
				err := txn.Set(engine_util.KeyWithCF(put.Cf, put.Key), put.Value)
				if err != nil {
					return err
				}
			case storage.Delete:
				del := modify.Data.(storage.Delete)
				err := txn.Delete(engine_util.KeyWithCF(del.Cf, del.Key))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}

// standAloneStorageReader is an implementation of StorageReader with standalone_storage
type standAloneStorageReader struct {
	txn *badger.Txn
}

func (r *standAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return val, err
}
func (r *standAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}
func (r *standAloneStorageReader) Close() {
	r.txn.Discard()
}
