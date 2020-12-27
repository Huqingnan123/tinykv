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
	//a standalone storage, using badger as Storage engine directly.
	aloneDB *badger.DB
}

func NewStandAloneStorage(conf *config.Config) *StandAloneStorage {
	// Your Code Here (1).
	//Creating a badger.DB to initialize a StandAloneStorage
	kvPath := filepath.Join(conf.DBPath,"kv")
	return &StandAloneStorage{aloneDB: engine_util.CreateDB(kvPath,false)}
}

func (s *StandAloneStorage) Start() error {
	// Your Code Here (1).
	//add nothing
	return nil
}

func (s *StandAloneStorage) Stop() error {
	// Your Code Here (1).
	//call badgerDB's Close()
	err := s.aloneDB.Close()
	if err != nil{
		return err
	} else{
		return nil
	}
}

func (s *StandAloneStorage) Reader(ctx *kvrpcpb.Context) (storage.StorageReader, error) {
	// Your Code Here (1).
	//For read-only transactions, set update to false
	return &StandAloneReader{Txn: s.aloneDB.NewTransaction(false)},nil
}

func (s *StandAloneStorage) Write(ctx *kvrpcpb.Context, batch []storage.Modify) error {
	// Your Code Here (1).
	writeBatch := new(engine_util.WriteBatch)
	//Distinguish the type of modify (Put or Delete)
	for _, modify := range batch{
		switch modify.Data.(type){
		case storage.Put:
			writeBatch.SetCF(modify.Cf(),modify.Key(),modify.Value())
		case storage.Delete:
			writeBatch.DeleteCF(modify.Cf(),modify.Key())
		}
	}
	//save modify
	return writeBatch.WriteToDB(s.aloneDB)
}

type StandAloneReader struct {
	Txn   *badger.Txn
}

//some methods' implement of StandAloneReader
func (reader *StandAloneReader) GetCF(cf string, key []byte) ([]byte, error) {
	value, err := engine_util.GetCFFromTxn(reader.Txn, cf, key)
	// When the key doesn't exist, return nil for the value
	if err == badger.ErrKeyNotFound {
		return nil, nil
	}
	return value, err
}

func (reader *StandAloneReader) IterCF(cf string) engine_util.DBIterator {
	return engine_util.NewCFIterator(cf, reader.Txn)
}

//call Discard() for badger.Txn and close all iterators
func (reader *StandAloneReader) Close() {
	reader.Txn.Discard()
}
