package standalone_storage

import (
	"github.com/Connor1996/badger"
	"github.com/pingcap-incubator/tinykv/kv/config"
	"github.com/pingcap-incubator/tinykv/kv/storage"
	"github.com/pingcap-incubator/tinykv/kv/util/engine_util"
	"github.com/pingcap-incubator/tinykv/proto/pkg/kvrpcpb"
	"path/filepath"
)

// StandAloneStorage is an implementation of `Storage` for a single-node TinyKV instance. It does not
// communicate with other nodes and all data is stored locally.
type StandAloneStorage struct {
	// Your Data Here (1).
	engines *engine_util.Engines
	conf *config.Config
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	dbPath := conf.DBPath
	kvPath := filepath.Join(dbPath, "kv")
	raftPath := filepath.Join(dbPath, "raft")

	raftDB := engine_util.CreateDB(raftPath, true)
	kvDB := engine_util.CreateDB(kvPath, false)

	return &StandAloneStorage{
		engines: engine_util.NewEngines(kvDB, raftDB, kvPath, raftPath),
		conf: conf,
	}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	return s.engines.Destroy()
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	txn := s.engines.Kv.NewTransaction(false)
	reader := NewStandAlongReader(txn)
	return reader, nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	for _, data := range batch {
		switch data.Data.(type) {
		case storage.Put:
			putData := data.Data.(storage.Put)
			err := engine_util.PutCF(s.engines.Kv, putData.Cf, putData.Key, putData.Value)
			return err
		case storage.Delete:
			delData := data.Data.(storage.Delete)
			err := engine_util.DeleteCF(s.engines.Kv, delData.Cf, delData.Key)
			return err
		}
	}
	return nil
}

type StandAlongReader struct {
	txn    *badger.Txn
}

func NewStandAlongReader(txn *badger.Txn) *StandAlongReader{
	return &StandAlongReader{
		txn: txn,
	}
}

func (r *StandAlongReader) GetCF(cf string, key []byte) ([]byte, error) {
	val, err := engine_util.GetCFFromTxn(r.txn, cf, key)
	// Once the key is not found, it is acceptable.....
	if err != badger.ErrKeyNotFound {
		return val, err
	}
	return val, nil
}

func (r *StandAlongReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, r.txn)
}

func (r *StandAlongReader) Close() {
	r.txn.Discard()
}
