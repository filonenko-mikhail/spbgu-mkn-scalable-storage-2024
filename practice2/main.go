package main

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	jsoniter "github.com/json-iterator/go"
	"github.com/paulmach/orb/geojson"
)

var c = jsoniter.Config{
	EscapeHTML:              true,
	SortMapKeys:             false,
	MarshalFloatWith6Digits: true,
}.Froze()
var loadOnce = sync.Once{}

func init() {
	geojson.CustomJSONMarshaler = c
	geojson.CustomJSONUnmarshaler = c
}

type Router struct {
	mux   *http.ServeMux
	nodes [][]string
}

func NewRouter(mux *http.ServeMux, nodes [][]string) *Router {
	// is it replica??? why are nodes are in form of table of strings
	mux.Handle("/", http.FileServer(http.Dir("../front/dist")))
	for _, row := range nodes {
		for _, node := range row {
			mux.Handle("/insert", http.RedirectHandler("/"+node+"/insert", http.StatusTemporaryRedirect))
			mux.Handle("/replace", http.RedirectHandler("/"+node+"/replace", http.StatusTemporaryRedirect))
			mux.Handle("/delete", http.RedirectHandler("/"+node+"/delete", http.StatusTemporaryRedirect))
			mux.Handle("/select", http.RedirectHandler("/"+node+"/select", http.StatusTemporaryRedirect))
			mux.Handle("/checkpoint", http.RedirectHandler("/"+node+"/checkpoint", http.StatusTemporaryRedirect))
		}
	}
	return &Router{
		mux:   mux,
		nodes: nodes,
	}
}

func (r *Router) Run() {
	slog.Info("Router started")
}

func (r *Router) Stop() {
	slog.Info("Router stopped")
}

func drain(m map[string]*geojson.Feature) *geojson.FeatureCollection {
	col := geojson.NewFeatureCollection()
	for _, f := range m {
		col.Append(f)
	}
	return col
}

type Storage struct {
	name string

	dbFile string
	eng    *Engine

	jobs chan *Transaction
	resp chan struct {
		data []byte
		err  error
	}

	ctx    context.Context
	cancel context.CancelFunc
}

func NewStorage(mux *http.ServeMux, name string, dbFile string) *Storage {
	eng, err := NewEngine("engine.log", "engine.checkpoint")
	if err != nil {
		panic(err.Error())
	}

	ctx, cancel := context.WithCancel(context.Background())
	storage := &Storage{
		name: name,

		dbFile: dbFile,
		eng:    eng,

		jobs: make(chan *Transaction),
		resp: make(chan struct {
			data []byte
			err  error
		}),

		ctx:    ctx,
		cancel: cancel,
	}

	mux.HandleFunc("/"+name+"/insert", storage.insertHandler)
	mux.HandleFunc("/"+name+"/replace", storage.replaceHandler)
	mux.HandleFunc("/"+name+"/delete", storage.deleteHandler)
	mux.HandleFunc("/"+name+"/select", storage.selectHandler)
	mux.HandleFunc("/"+name+"/checkpoint", storage.checkpointHandler)

	return storage
}

func (s *Storage) Run() {
	s.loadFromFile()
	s.eng.Run(s.jobs, s.resp)
	slog.Info("Storage started", "name", s.name)
}

func (s *Storage) Stop() {
	s.saveToFile()
	slog.Info("Storage stopped", "name", s.name)
}

func (s *Storage) loadFromFile() {
	loadOnce.Do(func() {
		s.eng.mu.Lock()
		defer s.eng.mu.Unlock()
		data, err := os.ReadFile(s.dbFile)
		if err != nil {
			if os.IsNotExist(err) {
				return
			}
			slog.Error("Failed to load DB", "err", err)
			return
		}
		col, err := geojson.UnmarshalFeatureCollection(data)
		if err != nil {
			slog.Error("Failed to unmarshal DB", "err", err)
		}
		s.eng.primary = make(map[string]*geojson.Feature, len(col.Features))
		for _, feature := range col.Features {
			s.eng.primary[feature.ID.(string)] = feature
		}
	})
}

func (s *Storage) saveToFile() {
	s.eng.mu.Lock()
	defer s.eng.mu.Unlock()
	file, err := os.Create(s.dbFile)
	if err != nil {
		slog.Error("Failed to save DB", "err", err)
		return
	}
	defer file.Close()
	col := drain(s.eng.primary)
	buf, err := col.MarshalJSON()
	if err != nil {
		slog.Error("Failed to encode DB", "err", err)
	}
	if _, err = file.Write(buf); err != nil {
		slog.Error("Failed to write to file", "err", err)
	}
}

func (s *Storage) insertHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("insert method")
	buf, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		slog.Error("/insert read body", slog.Any("error", err.Error()))
	}
	feature, err := geojson.UnmarshalFeature(buf)
	if err != nil {
		http.Error(w, "invalid geojson", http.StatusBadRequest)
		return
	}
	s.jobs <- &Transaction{
		Action:  "insert",
		Name:    s.name,
		LSN:     s.eng.lsn.Load(),
		Feature: feature,
	}
	_ = <-s.resp
	w.WriteHeader(http.StatusOK)
}

func (s *Storage) replaceHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("replace method")
	buf, err := io.ReadAll(r.Body)
	defer r.Body.Close()
	if err != nil {
		slog.Error("/replace read body", slog.Any("error", err.Error()))
	}
	feature, err := geojson.UnmarshalFeature(buf)
	if err != nil {
		http.Error(w, "invalid geojson", http.StatusBadRequest)
		return
	}
	s.jobs <- &Transaction{
		Action:  "replace",
		Name:    s.name,
		LSN:     s.eng.lsn.Load(),
		Feature: feature,
	}
	_ = <-s.resp
	w.WriteHeader(http.StatusOK)
}

func (s *Storage) deleteHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("delete method")
	var data struct {
		ID string `json:"id"`
	}
	if err := json.NewDecoder(r.Body).Decode(&data); err != nil {
		http.Error(w, "invalid request", http.StatusBadRequest)
		return
	}
	feature := &geojson.Feature{}
	feature.ID = data.ID
	s.jobs <- &Transaction{
		Action:  "replace",
		Name:    s.name,
		LSN:     s.eng.lsn.Load(),
		Feature: feature,
	}
	res := <-s.resp
	if res.err != nil {
		http.Error(w, "can't delete", http.StatusBadRequest)
	}
	w.WriteHeader(http.StatusOK)
}

func (s *Storage) selectHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("select method")
	rect := r.URL.Query().Get("rect")
	if len(rect) < 4 {
		http.Error(w, "need 4 values for rect", http.StatusBadRequest)
		return
	}

	s.jobs <- &Transaction{
		Action:  "select",
		Name:    s.name,
		LSN:     s.eng.lsn.Load(),
		Feature: nil,
	}
	res := <-s.resp
	if res.err != nil {
		http.Error(w, "can't select", http.StatusBadRequest)
	}
	slog.Debug(string(res.data))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(res.data)
}

func (s *Storage) checkpointHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("checkpoint method")
	s.jobs <- &Transaction{
		Action:  "replace",
		Name:    s.name,
		LSN:     s.eng.lsn.Load(),
		Feature: nil,
	}
	res := <-s.resp
	if res.err != nil {
		http.Error(w, "can't checkpoint", http.StatusBadRequest)
	}
	w.WriteHeader(http.StatusOK)
}

func main() {
	mux := http.NewServeMux()

	storage := NewStorage(mux, "storage", "geo.db.json")
	router := NewRouter(mux, [][]string{{"storage"}})

	storage.Run()
	router.Run()

	server := &http.Server{
		Addr:    "127.0.0.1:8080",
		Handler: mux,
	}

	go func() {
		sigs := make(chan os.Signal, 1)
		signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
		<-sigs

		ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()

		server.Shutdown(ctx)
		storage.Stop()
		router.Stop()
	}()

	slog.Info("Server running at http://127.0.0.1:8080")
	if err := server.ListenAndServe(); !errors.Is(err, http.ErrServerClosed) {
		slog.Error("Server error", "err", err)
	}
}
