package core

import (
	"fmt"
	"math/big"
	"reflect"
	"strings"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/consensus/ethash"
	"github.com/ethereum/go-ethereum/core/rawdb"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/ethereum/go-ethereum/params"
)

type liveCommit struct {
	parentRoot, root common.Hash
	destructs        map[common.Hash]struct{}
	accounts         map[common.Hash][]byte
	storages         map[common.Hash]map[common.Hash][]byte
	codes            map[common.Hash][]byte
}

// recordingTracer records the live tracer callbacks of block import.
type recordingTracer struct {
	events   []string
	inits    int
	genesis  *types.Block
	alloc    GenesisAlloc
	closed   bool
	block    *types.Block
	commits  map[common.Hash]liveCommit
	ended    map[common.Hash]error
	heads    []common.Hash
	captures int
}

func newRecordingTracer() *recordingTracer {
	return &recordingTracer{commits: make(map[common.Hash]liveCommit), ended: make(map[common.Hash]error)}
}

func (r *recordingTracer) OnBlockchainInit(*params.ChainConfig) error { r.inits++; return nil }
func (r *recordingTracer) OnGenesisBlock(block *types.Block, alloc GenesisAlloc) {
	r.genesis, r.alloc = block, alloc
}
func (r *recordingTracer) OnClose() { r.closed = true }
func (r *recordingTracer) OnBlockStart(block *types.Block) {
	r.block = block
	r.events = append(r.events, fmt.Sprintf("start %d", block.NumberU64()))
}
func (r *recordingTracer) OnTxStart(tx *types.Transaction, from common.Address) {
	r.events = append(r.events, "tx start")
}
func (r *recordingTracer) OnTxEnd(receipt *types.Receipt, err error) {
	r.events = append(r.events, fmt.Sprintf("tx end status=%d err=%v", receipt.Status, err))
}
func (r *recordingTracer) OnLog(log *types.Log) { r.events = append(r.events, "log") }
func (r *recordingTracer) OnCommit(parentRoot, root common.Hash, destructs map[common.Hash]struct{}, accounts map[common.Hash][]byte, storages map[common.Hash]map[common.Hash][]byte, codes map[common.Hash][]byte) {
	r.events = append(r.events, "commit")
	c := liveCommit{
		parentRoot: parentRoot, root: root,
		destructs: make(map[common.Hash]struct{}), accounts: make(map[common.Hash][]byte),
		storages: make(map[common.Hash]map[common.Hash][]byte), codes: make(map[common.Hash][]byte),
	}
	for k := range destructs {
		c.destructs[k] = struct{}{}
	}
	for k, v := range accounts {
		c.accounts[k] = common.CopyBytes(v)
	}
	for k, slots := range storages {
		c.storages[k] = make(map[common.Hash][]byte)
		for s, v := range slots {
			c.storages[k][s] = common.CopyBytes(v)
		}
	}
	for k, v := range codes {
		c.codes[k] = common.CopyBytes(v)
	}
	r.commits[r.block.Hash()] = c
}
func (r *recordingTracer) OnBlockEnd(err error) {
	r.events = append(r.events, fmt.Sprintf("end err=%v", err != nil))
	r.ended[r.block.Hash()] = err
}
func (r *recordingTracer) PushHead(head *types.Header, getHeader func(common.Hash) *types.Header) error {
	if getHeader(head.Hash()) == nil {
		return fmt.Errorf("head %s not readable", head.Hash())
	}
	r.events = append(r.events, fmt.Sprintf("push %d", head.Number.Uint64()))
	r.heads = append(r.heads, head.Hash())
	return nil
}

func (r *recordingTracer) CaptureTxStart(uint64) {}
func (r *recordingTracer) CaptureTxEnd(uint64)   {}
func (r *recordingTracer) CaptureStart(*vm.EVM, common.Address, common.Address, bool, []byte, uint64, *big.Int) {
	r.captures++
}
func (r *recordingTracer) CaptureEnd([]byte, uint64, time.Duration, error) {}
func (r *recordingTracer) CaptureEnter(vm.OpCode, common.Address, common.Address, []byte, uint64, *big.Int) {
}
func (r *recordingTracer) CaptureExit([]byte, uint64, error) {}
func (r *recordingTracer) CaptureState(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, []byte, int, error) {
}
func (r *recordingTracer) CaptureFault(uint64, vm.OpCode, uint64, uint64, *vm.ScopeContext, int, error) {
}

// replayStateChanges re-executes block on its parent state the way
// trace_debankBlock does and returns the resulting state changes.
func replayStateChanges(t *testing.T, chain *BlockChain, block *types.Block) liveCommit {
	t.Helper()
	parent := chain.GetBlockByHash(block.ParentHash())
	statedb, err := chain.StateAt(parent.Root())
	if err != nil {
		t.Fatalf("no state for block %d: %v", block.NumberU64(), err)
	}
	config := chain.Config()
	signer := types.MakeSigner(config, block.Number())
	blockContext := NewEVMBlockContext(block.Header(), chain, nil)
	gasPool := new(GasPool).AddGas(block.GasLimit())
	for i, tx := range block.Transactions() {
		msg, err := tx.AsMessage(signer, block.BaseFee())
		if err != nil {
			t.Fatal(err)
		}
		statedb.Prepare(tx.Hash(), i)
		vmenv := vm.NewEVM(blockContext, NewEVMTxContext(msg), statedb, config, vm.Config{NoBaseFee: true})
		if _, err := ApplyMessage(vmenv, msg, gasPool); err != nil {
			t.Fatal(err)
		}
		statedb.Finalise(config.IsEIP158(block.Number()))
	}
	if err := chain.Engine().Finalize(chain, block.Header(), statedb, block.Transactions(), block.Uncles()); err != nil {
		t.Fatal(err)
	}
	root := statedb.IntermediateRoot(config.IsEIP158(block.Number()))
	if root != block.Root() {
		t.Fatalf("replayed root %s, want %s", root, block.Root())
	}
	destructs, accounts, storages, codes := statedb.Output()
	return liveCommit{parent.Root(), root, destructs, accounts, storages, codes}
}

func TestLiveTracerBlockImport(t *testing.T) {
	var (
		key, _  = crypto.HexToECDSA("b71c71a67e1177ad4e901695e1b4b9ee17ae16c6668d313eac2f96dbcda3f291")
		addr    = crypto.PubkeyToAddress(key.PublicKey)
		db      = rawdb.NewMemoryDatabase()
		gspec   = &Genesis{Config: params.TestChainConfig, Alloc: GenesisAlloc{addr: {Balance: big.NewInt(1e18)}}}
		genesis = gspec.MustCommit(db)
		signer  = types.LatestSigner(gspec.Config)
		// SSTORE(0, 42), LOG0, then deploy the one-byte runtime 0xff.
		initCode = common.Hex2Bytes("602a60005560006000a060ff60005360016000f3")
	)
	tracer := newRecordingTracer()
	chain, err := NewBlockChain(db, nil, gspec.Config, ethash.NewFaker(), vm.Config{Debug: true, Tracer: tracer}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	if tracer.inits != 1 || tracer.genesis.Hash() != genesis.Hash() || len(tracer.alloc) != 1 {
		t.Fatalf("init=%d genesis=%v alloc=%v, want one init with the genesis alloc", tracer.inits, tracer.genesis, tracer.alloc)
	}
	if cfg := chain.GetVMConfig(); cfg.Tracer != nil || cfg.Debug {
		t.Errorf("GetVMConfig exposes the live tracer: %+v", cfg)
	}

	blocks, _ := GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 3, func(i int, gen *BlockGen) {
		var tx *types.Transaction
		switch i {
		case 0:
			tx = types.NewTransaction(gen.TxNonce(addr), common.Address{0x01}, big.NewInt(1000), 21000, gen.header.BaseFee, nil)
		case 1:
			tx = types.NewContractCreation(gen.TxNonce(addr), new(big.Int), 1000000, gen.header.BaseFee, initCode)
		default:
			return
		}
		signed, err := types.SignTx(tx, signer, key)
		if err != nil {
			t.Fatal(err)
		}
		gen.AddTx(signed)
	})
	if _, err := chain.InsertChain(blocks); err != nil {
		t.Fatal(err)
	}

	want := []string{
		"start 1", "tx start", "tx end status=1 err=<nil>", "commit", "end err=false", "push 1",
		"start 2", "tx start", "log", "tx end status=1 err=<nil>", "commit", "end err=false", "push 2",
		"start 3", "commit", "end err=false", "push 3",
	}
	if !reflect.DeepEqual(tracer.events, want) {
		t.Errorf("events:\n got %s\nwant %s", strings.Join(tracer.events, ", "), strings.Join(want, ", "))
	}
	if tracer.captures != 2 {
		t.Errorf("EVM reported %d top-level calls, want 2", tracer.captures)
	}
	for _, block := range blocks {
		got, ok := tracer.commits[block.Hash()]
		if !ok {
			t.Fatalf("no state changes reported for block %d", block.NumberU64())
		}
		if want := replayStateChanges(t, chain, block); !reflect.DeepEqual(got, want) {
			t.Errorf("block %d state changes differ from trace_debankBlock replay:\n got %+v\nwant %+v", block.NumberU64(), got, want)
		}
	}
	if code := tracer.commits[blocks[1].Hash()].codes; len(code) != 1 {
		t.Errorf("contract creation reported %d codes, want 1", len(code))
	}

	chain.Stop()
	if !tracer.closed {
		t.Error("tracer not closed on stop")
	}
}

func TestLiveTracerFailedAndSideBlocks(t *testing.T) {
	var (
		db      = rawdb.NewMemoryDatabase()
		gspec   = &Genesis{Config: params.TestChainConfig}
		genesis = gspec.MustCommit(db)
	)
	tracer := newRecordingTracer()
	chain, err := NewBlockChain(db, nil, gspec.Config, ethash.NewFaker(), vm.Config{Debug: true, Tracer: tracer}, nil, nil)
	if err != nil {
		t.Fatal(err)
	}
	defer chain.Stop()

	canon, _ := GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 2, func(i int, gen *BlockGen) {})
	fork, _ := GenerateChain(gspec.Config, genesis, ethash.NewFaker(), db, 3, func(i int, gen *BlockGen) {
		gen.SetCoinbase(common.Address{0x0f})
	})
	if _, err := chain.InsertChain(canon); err != nil {
		t.Fatal(err)
	}

	// A block with a wrong state root is executed but never published.
	bad := canon[1].Header()
	bad.ParentHash = canon[0].Hash()
	bad.Number = big.NewInt(2)
	bad.Root = common.Hash{0xba, 0xd0}
	badBlock := types.NewBlockWithHeader(bad)
	if _, err := chain.InsertChain(types.Blocks{badBlock}); err == nil {
		t.Fatal("expected the bad block to be rejected")
	}
	if err := tracer.ended[badBlock.Hash()]; err == nil {
		t.Error("bad block ended without an error")
	}

	// The heavier fork becomes canonical only at its last block.
	if _, err := chain.InsertChain(fork); err != nil {
		t.Fatal(err)
	}
	if chain.CurrentBlock().Hash() != fork[2].Hash() {
		t.Fatalf("head = %d, want fork head", chain.CurrentBlock().NumberU64())
	}
	for _, block := range fork {
		if err, ok := tracer.ended[block.Hash()]; !ok || err != nil {
			t.Errorf("fork block %d ended=%v err=%v, want published", block.NumberU64(), ok, err)
		}
	}
	if got := tracer.heads[len(tracer.heads)-1]; got != fork[2].Hash() {
		t.Errorf("last pushed head %s, want fork head %s", got, fork[2].Hash())
	}
	for _, head := range tracer.heads {
		if head == fork[0].Hash() || head == badBlock.Hash() {
			t.Errorf("pushed non-canonical head %s", head)
		}
	}
}
