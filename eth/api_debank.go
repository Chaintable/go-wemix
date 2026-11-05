package eth

import (
	"context"
	"fmt"

	ptracer "github.com/Chaintable/pipeline/tracer"
	ptypes "github.com/Chaintable/pipeline/types"
	"github.com/ethereum/go-ethereum/core"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/log"
	"github.com/ethereum/go-ethereum/rpc"
)

type DebankAPI struct {
	eth *Ethereum
}

func NewDebankAPI(eth *Ethereum) *DebankAPI {
	return &DebankAPI{eth: eth}
}

func (api *DebankAPI) DebankBlock(ctx context.Context, blockNr rpc.BlockNumber) (*ptypes.DebankOutPut, error) {
	blockchain := api.eth.BlockChain()
	chainConfig := blockchain.Config()

	if blockNr == 0 {
		return api.debankGenesisBlock(blockchain)
	}

	block := blockchain.GetBlockByNumber(uint64(blockNr))
	if block == nil {
		return nil, fmt.Errorf("block %d not found", blockNr)
	}
	parentBlock := blockchain.GetBlockByNumber(uint64(blockNr - 1))
	if parentBlock == nil {
		return nil, fmt.Errorf("parent block %d not found", blockNr-1)
	}

	statedb, err := blockchain.StateAt(parentBlock.Root())
	if err != nil {
		return nil, fmt.Errorf("cannot get state for block %v: %v", blockNr, err)
	}

	receipts := blockchain.GetReceiptsByHash(block.Hash())
	if receipts == nil {
		return nil, fmt.Errorf("cannot get receipts for block %v", blockNr)
	}

	tracer := ptracer.NewRPCTracer()
	tracer.OnBlockStart(block)

	statedb.OnLog = tracer.OnLog

	signer := types.MakeSigner(chainConfig, block.Number())
	blockContext := core.NewEVMBlockContext(block.Header(), blockchain, nil)
	gasPool := new(core.GasPool).AddGas(block.GasLimit())

	for i, tx := range block.Transactions() {
		msg, _ := tx.AsMessage(signer, block.BaseFee())
		statedb.Prepare(tx.Hash(), i)

		tracer.OnTxStart(tx, msg.From())

		vmConfig := vm.Config{
			Debug:     true,
			Tracer:    tracer.GetCallTracer(),
			NoBaseFee: true,
		}
		txContext := core.NewEVMTxContext(msg)
		vmenv := vm.NewEVM(blockContext, txContext, statedb, chainConfig, vmConfig)

		_, err := core.ApplyMessage(vmenv, msg, gasPool)
		if err != nil {
			log.Warn("EVM execution error", "block", block.NumberU64(), "tx", tx.Hash(), "err", err)
			return nil, fmt.Errorf("tx %d execution failed: %v", i, err)
		}

		tracer.OnTxEnd(receipts[i], nil)
		statedb.Finalise(chainConfig.IsEIP158(block.Number()))
	}

	blockchain.Engine().Finalize(blockchain, block.Header(), statedb, block.Transactions(), block.Uncles())

	root := statedb.IntermediateRoot(chainConfig.IsEIP158(block.Number()))
	if root != block.Root() {
		log.Warn("State root mismatch", "block", block.NumberU64(), "have", root, "want", block.Root())
		return nil, fmt.Errorf("state root mismatch: have %v, want %v", root, block.Root())
	}

	snapDestructs, snapAccounts, snapStorage, codes := statedb.Output()

	return tracer.GetOutPut(parentBlock.Root(), root, snapDestructs, snapAccounts, snapStorage, codes), nil
}

func (api *DebankAPI) debankGenesisBlock(blockchain *core.BlockChain) (*ptypes.DebankOutPut, error) {
	block := blockchain.GetBlockByNumber(0)
	if block == nil {
		return nil, fmt.Errorf("genesis block not found")
	}

	// 获取 genesis alloc：优先从数据库读取，回退到硬编码配置
	var alloc core.GenesisAlloc
	if len(rawdb.ReadGenesisState(api.eth.ChainDb(), block.Hash())) != 0 {
		stored, err := core.ReadGenesisAlloc(api.eth.ChainDb(), block.Hash())
		if err != nil {
			return nil, err
		}
		alloc = stored
	} else {
		alloc = core.DefaultGenesisBlock().Alloc
	}
	return ptracer.GenesisOutput(block, alloc), nil
}
