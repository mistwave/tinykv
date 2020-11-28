package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/log"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engine *engine_util.Engines
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	db := engine_util.CreateDB(conf.DBPath, conf.Raft)
	engine := engine_util.NewEngines(db, nil, conf.DBPath, "")

	return &StandAloneStorage{
		engine: engine,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	log.Info("StandAloneStorage start")
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	log.Info("StandAloneStorage stop")
	if err := s.engine.Kv.Close(); err != nil {
		return err
	}
	return nil
}

type StandAloneStorageReader struct {
	txn       badger.Txn
	iterators []engine_util.DBIterator
}

// return nil when key not found
func (r StandAloneStorageReader) GetCF(cf string, key []byte) ([]byte, error) {
	item, err := r.txn.Get(engine_util.KeyWithCF(cf, key))
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	return item.Value()
}

func (r StandAloneStorageReader) IterCF(cf string) engine_util.DBIterator {
	iterator := engine_util.NewCFIterator(cf, &r.txn)
	r.iterators = append(r.iterators, iterator)
	return iterator
}

func (r StandAloneStorageReader) Close() {
	txn := r.txn
	for _, it := range r.iterators {
		it.Close()
	}
	if err := txn.Commit(); err != nil {
		txn.Discard()
	}
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(false)

	reader := StandAloneStorageReader{
		txn: *txn,
	}

	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	txn := s.engine.Kv.NewTransaction(true)
	for _, b := range batch {
		switch b.Data.(type) {
		case storage.Put:
			if err := txn.Set(engine_util.KeyWithCF(b.Cf(), b.Key()), b.Value()); err != nil {
				return err
			}
		case storage.Delete:
			if err := txn.Delete(engine_util.KeyWithCF(b.Cf(), b.Key())); err != nil {
				return err
			}
		}
	}

	if err := txn.Commit(); err != nil {
		txn.Discard()
		return err
	}
	return nil
}
