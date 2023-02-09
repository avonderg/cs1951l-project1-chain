package blockinfodatabase

import (
	"Chain/pkg/pro"
	// TODO: Uncomment for implementing StoreBlockRecord and GetBlockRecord
	// "Chain/pkg/pro"
	// "google.golang.org/protobuf/proto"
	"Chain/pkg/utils"
	"google.golang.org/protobuf/proto"

	"github.com/syndtr/goleveldb/leveldb"
)

// BlockInfoDatabase is a wrapper for a levelDB
type BlockInfoDatabase struct {
	db *leveldb.DB
}

// New returns a BlockInfoDatabase given a Config
func New(config *Config) *BlockInfoDatabase {
	db, err := leveldb.OpenFile(config.DatabasePath, nil)
	if err != nil {
		utils.Debug.Printf("Unable to initialize BlockInfoDatabase with path {%v}", config.DatabasePath)
	}
	return &BlockInfoDatabase{db: db}
}

// StoreBlockRecord stores a block record in the block info database.
func (blockInfoDB *BlockInfoDatabase) StoreBlockRecord(hash string, blockRecord *BlockRecord) {
	// TODO: Implement this function

	// first encode blockrecord info as a protobuf
	protobuf := EncodeBlockRecord(blockRecord)

	// then we must convert protobuf
	m_protobuf, _ := proto.Marshal(protobuf) // do i need to error check?? -> catch it somehow

	// put into database
	blockInfoDB.db.Put([]byte(hash), m_protobuf, nil)
}

// GetBlockRecord returns a BlockRecord from the BlockInfoDatabase given
// the relevant block's hash.
func (blockInfoDB *BlockInfoDatabase) GetBlockRecord(hash string) *BlockRecord {
	// TODO: Implement this function
	// retrieve block from database

	m_protobuf, _ := blockInfoDB.db.Get([]byte(hash), nil) // do i do something if err is not nil? do i return err
	var protobuf *pro.BlockRecord
	proto.Unmarshal(m_protobuf, protobuf) // result is placed in protobuf
	// convert protobuf back into block record
	block := DecodeBlockRecord(protobuf)

	return block
}
