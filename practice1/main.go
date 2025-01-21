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
	"slices"
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

type Storage struct {
	mux      *http.ServeMux
	name     string
	geoCache map[string]*geojson.Feature
	geoDB    *geojson.FeatureCollection
	mutex    sync.RWMutex
	dbFile   string
}

func NewStorage(mux *http.ServeMux, name string, dbFile string) *Storage {
	storage := &Storage{
		mux:      mux,
		name:     name,
		geoCache: make(map[string]*geojson.Feature),
		geoDB:    geojson.NewFeatureCollection(),
		dbFile:   dbFile,
	}

	mux.HandleFunc("/"+name+"/insert", storage.insertHandler)
	mux.HandleFunc("/"+name+"/replace", storage.replaceHandler)
	mux.HandleFunc("/"+name+"/delete", storage.deleteHandler)
	mux.HandleFunc("/"+name+"/select", storage.selectHandler)

	return storage
}

func (s *Storage) Run() {
	s.loadFromFile()
	slog.Info("Storage started", "name", s.name)
}

func (s *Storage) Stop() {
	s.saveToFile()
	slog.Info("Storage stopped", "name", s.name)
}

func (s *Storage) loadFromFile() {
	s.mutex.Lock()
	defer s.mutex.Unlock()
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
	s.geoDB = col
	s.geoCache = make(map[string]*geojson.Feature, len(col.Features))
	for _, feature := range col.Features {
		s.geoCache[feature.ID.(string)] = feature
	}
}

func (s *Storage) saveToFile() {
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	file, err := os.Create(s.dbFile)
	if err != nil {
		slog.Error("Failed to save DB", "err", err)
		return
	}
	defer file.Close()
	buf, err := s.geoDB.MarshalJSON()
	if err != nil {
		slog.Error("Failed to encode DB", "err", err)
	}
	if _, err = file.Write(buf); err != nil {
		slog.Error("Failed to write to file", "err", err)
	}
}

func hasID(w http.ResponseWriter, feature *geojson.Feature) (string, bool) {
	id, ok := feature.ID.(string)
	if !ok {
		slog.Error("ID is wrong type, string expected")
		w.WriteHeader(http.StatusInternalServerError)
		w.Write([]byte("ID is wrong type, string expected"))
		return "", false
	}
	return id, true
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
	s.mutex.Lock()
	defer s.mutex.Unlock()
	id, ok := hasID(w, feature)
	if !ok {
		//fixme
		return
	}
	if _, ok := s.geoCache[id]; ok {
		return
	}
	s.geoCache[id] = feature
	s.geoDB.Append(feature)
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
	s.mutex.Lock()
	defer s.mutex.Unlock()
	id, ok := hasID(w, feature)
	if !ok {
		return
	}
	if _, exists := s.geoCache[id]; !exists {
		http.Error(w, "feature not found", http.StatusNotFound)
		return
	}
	s.geoCache[id] = feature
	i := slices.IndexFunc(s.geoDB.Features, func(f *geojson.Feature) bool {
		return f.ID == id
	})
	if i == -1 {
		slog.Error("id %s couldn't be found in db", slog.String("point-id", id))
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
	s.geoDB.Features[i] = s.geoDB.Features[len(s.geoDB.Features)-1]
	s.geoDB.Features = s.geoDB.Features[:len(s.geoDB.Features)-1]
	s.geoDB.Append(feature)
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
	s.mutex.Lock()
	defer s.mutex.Unlock()
	delete(s.geoCache, data.ID)
	i := slices.IndexFunc(s.geoDB.Features, func(f *geojson.Feature) bool {
		return f.ID == data.ID
	})
	if i == -1 {
		slog.Error("id %s couldn't be found in db", slog.String("point-id", data.ID))
		http.Error(w, "internal error", http.StatusInternalServerError)
	}
	// order is insignificant
	s.geoDB.Features[i] = s.geoDB.Features[len(s.geoDB.Features)-1]
	s.geoDB.Features = s.geoDB.Features[:len(s.geoDB.Features)-1]
	w.WriteHeader(http.StatusOK)
}

func (s *Storage) selectHandler(w http.ResponseWriter, r *http.Request) {
	slog.Info("select method")
	s.mutex.RLock()
	defer s.mutex.RUnlock()
	data, err := s.geoDB.MarshalJSON()
	if err != nil {
		slog.Error("can't marshal db", slog.String("error", err.Error()))
	}
	slog.Debug(string(data))
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(http.StatusOK)
	w.Write(data)
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
