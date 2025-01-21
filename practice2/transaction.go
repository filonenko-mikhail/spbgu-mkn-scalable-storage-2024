package main

import (
	"context"
	"encoding/json"
	"errors"
	"log/slog"
	"os"
	"strconv"
	"sync"
	"sync/atomic"

	"github.com/paulmach/orb/geojson"
	"github.com/tidwall/rtree"
)

type Transaction struct {
	Action  string           `json:"action"`
	Name    string           `json:"name"`
	LSN     uint64           `json:"lsn"`
	Feature *geojson.Feature `json:"feature"`
}

type Engine struct {
	mu             sync.Mutex
	primary        map[string]*geojson.Feature
	spatial        *rtree.RTree
	lsn            atomic.Uint64
	logFile        *os.File
	checkpointPath string
	ctx            context.Context
	cancel         context.CancelFunc
}

func NewEngine(logPath, checkpointPath string) (*Engine, error) {
	logFile, err := os.OpenFile(logPath, os.O_RDWR|os.O_CREATE|os.O_APPEND, 0644)
	if err != nil {
		return nil, err
	}

	ctx, cancel := context.WithCancel(context.Background())

	engine := &Engine{
		primary:        make(map[string]*geojson.Feature),
		spatial:        &rtree.RTree{},
		logFile:        logFile,
		checkpointPath: checkpointPath,
		ctx:            ctx,
		cancel:         cancel,
	}

	// Load checkpoint and replay log
	if err := engine.loadCheckpoint(); err != nil {
		return nil, err
	}
	if err := engine.replayLog(); err != nil {
		return nil, err
	}

	return engine, nil
}

func (e *Engine) loadCheckpoint() error {
	file, err := os.Open(e.checkpointPath)
	if err != nil {
		if errors.Is(err, os.ErrNotExist) {
			return nil
		}
		return err
	}
	defer file.Close()

	decoder := json.NewDecoder(file)
	for {
		var txn Transaction
		if err := decoder.Decode(&txn); err != nil {
			break
		}
		e.applyTransaction(&txn)
	}
	return nil
}

func (e *Engine) replayLog() error {
	decoder := json.NewDecoder(e.logFile)
	for {
		var txn Transaction
		if err := decoder.Decode(&txn); err != nil {
			break
		}
		e.applyTransaction(&txn)
	}
	return nil
}

func (e *Engine) applyTransaction(txn *Transaction) ([]byte, error) {
	slog.Info("", slog.String("method", "transaction"), slog.String("action", txn.Action))
	switch txn.Action {
	case "insert", "replace":
		e.primary[txn.Feature.ID.(string)] = txn.Feature
		e.spatial.Insert(txn.Feature.BBox.Bound().Min, txn.Feature.BBox.Bound().Max, txn.Feature)
		return nil, nil
	case "delete":
		if feature, exists := e.primary[txn.Feature.ID.(string)]; exists {
			e.spatial.Delete(feature.BBox.Bound().Min, feature.BBox.Bound().Max, feature)
			delete(e.primary, txn.Feature.ID.(string))
			return nil, nil
		}
		return nil, errors.New("can't delete by id" + txn.Feature.ID.(string) + ": no such enrty")
	case "select":
		col := geojson.NewFeatureCollection()
		e.spatial.Scan(func(min, max [2]float64, data interface{}) bool {
			col.Append(data.(*geojson.Feature))
			return true
		})
		return col.MarshalJSON()
	case "checkpoint":
		f, err := os.Open(txn.Name + strconv.FormatUint(e.lsn.Load(), 10) + e.checkpointPath)
		if err != nil {
			slog.Error("can't open checkpoint file")
			return nil, err
		}
		defer func() { _ = f.Close() }()

		e.mu.Lock()
		col := drain(e.primary)
		e.mu.Unlock()
		data, err := col.MarshalJSON()
		if err != nil {
			slog.Error("error on marshaling collection", slog.String("error", err.Error()))
			return nil, err
		}
		if _, err := f.Write(data); err != nil {
			slog.Error("error on writing checkpoint", slog.String("error", err.Error()))
			return nil, err
		}
		return nil, nil
	default:
		panic("unknown action")
	}
}

func (e *Engine) saveTransaction(txn *Transaction) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	e.lsn.Add(1)
	txn.LSN = e.lsn.Load()

	data, err := json.Marshal(txn)
	if err != nil {
		return err
	}

	if _, err := e.logFile.Write(append(data, '\n')); err != nil {
		return err
	}
	e.applyTransaction(txn)
	return nil
}

func (e *Engine) check() error {
	e.mu.Lock()
	defer e.mu.Unlock()

	file, err := os.Create(e.checkpointPath)
	if err != nil {
		return err
	}
	defer file.Close()

	encoder := json.NewEncoder(file)
	for _, feature := range e.primary {
		txn := &Transaction{
			Action:  "insert",
			Feature: feature,
		}
		if err := encoder.Encode(txn); err != nil {
			return err
		}
	}

	e.logFile.Truncate(0)
	e.logFile.Seek(0, 0)
	return nil
}

func (e *Engine) Run(jobs chan *Transaction, resp chan struct {
	data []byte
	err  error
}) {
	go func() {
		for {
			select {
			case tnx := <-jobs:
				e.applyTransaction(tnx)
			case <-e.ctx.Done():
				e.logFile.Close()
			}
		}
	}()
}

func (e *Engine) Stop() {
	e.cancel()
}
