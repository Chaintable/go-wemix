package core

import (
	"encoding/json"
	"fmt"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/ethdb"
	"github.com/ethereum/go-ethereum/params"
)

// LiveTracer observes blocks while they are imported into the chain, such as
// the Chaintable pipeline tracer enabled with --vmtrace. It is installed as
// vm.Config.Tracer and detected by type assertion, so core does not depend on
// any implementation. Block import calls it from one goroutine at a time.
type LiveTracer interface {
	vm.EVMLogger

	// OnBlockchainInit is called once the chain has been loaded.
	OnBlockchainInit(chainConfig *params.ChainConfig) error
	// OnGenesisBlock is called after OnBlockchainInit when the head is the genesis block.
	OnGenesisBlock(block *types.Block, alloc GenesisAlloc)
	// OnClose is called when the chain stops, after block import has ended.
	OnClose()

	// OnBlockStart is called before a block is executed for import.
	OnBlockStart(block *types.Block)
	// OnTxStart and OnTxEnd wrap each transaction of the block. err is set
	// when the transaction could not be applied.
	OnTxStart(tx *types.Transaction, from common.Address)
	OnTxEnd(receipt *types.Receipt, err error)
	// OnLog is called for every log emitted by the current transaction.
	OnLog(log *types.Log)
	// OnCommit is called once the block's state root has been validated, with
	// the state changes as returned by state.StateDB.Output.
	OnCommit(parentRoot, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, codes map[common.Hash][]byte)
	// OnBlockEnd is called after OnCommit, before the block is written
	// (err == nil), or when executing or validating the block failed.
	OnBlockEnd(err error)
	// PushHead is called after the block has been written as the canonical head.
	PushHead(head *types.Header, getHeader func(common.Hash) *types.Header) error
}

// withoutLiveTracer returns cfg without the live tracer, for EVM executions
// other than block import such as prefetching, eth_call and mining.
func withoutLiveTracer(cfg vm.Config) vm.Config {
	if _, ok := cfg.Tracer.(LiveTracer); ok {
		cfg.Tracer = nil
		cfg.Debug = false
	}
	return cfg
}

// ReadGenesisAlloc returns the genesis allocation stored when the database was
// initialized.
func ReadGenesisAlloc(db ethdb.KeyValueReader, genesisHash common.Hash) (GenesisAlloc, error) {
	blob := rawdb.ReadGenesisState(db, genesisHash)
	if len(blob) == 0 {
		return nil, fmt.Errorf("genesis state of %s not found", genesisHash)
	}
	var alloc GenesisAlloc
	if err := json.Unmarshal(blob, &alloc); err != nil {
		return nil, fmt.Errorf("failed to decode genesis state: %w", err)
	}
	return alloc, nil
}
