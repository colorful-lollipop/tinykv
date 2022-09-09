package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

//s_StorageReader is an implementation of StorageReader with standalone_storage
type s_StorageReader struct {
	txn *badger.Txn
}

func (r *s_StorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	if err != nil && err.Error() == "Key not found" {
		return nil, nil
	}
	return val, err
}
func (r *s_StorageReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}
func (r *s_StorageReader) Close() {
	r.txn.Discard()
}

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
	return &s_StorageReader{txn}, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	return s.db.Update(func(txn *badger.Txn) error {
		for _, modify := range batch {
			switch modify.Data.(type) {
			case storage.Put:
				err := txn.Set(engine_util.KeyWithCF(modify.Cf(), modify.Key()), modify.Value())
				if err != nil {
					return err
				}
			case storage.Delete:
				err := txn.Delete(engine_util.KeyWithCF(modify.Cf(), modify.Key()))
				if err != nil {
					return err
				}
			}
		}
		return nil
	})
}
