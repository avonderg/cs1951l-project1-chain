package blockinfodatabase

import (
	// TODO: Uncomment for implementing StoreBlockRecord and GetBlockRecord
	// "Chain/pkg/pro"
	// "google.golang.org/protobuf/proto"
	"Chain/pkg/utils"

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
}

// GetBlockRecord returns a BlockRecord from the BlockInfoDatabase given
// the relevant block's hash.
func (blockInfoDB *BlockInfoDatabase) GetBlockRecord(hash string) *BlockRecord {
	// TODO: Implement this function
	return nil
}
