package main

import (
	"bytes"
	"encoding/json"
	"log/slog"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"testing"

	"github.com/paulmach/orb"
	"github.com/paulmach/orb/geojson"
	"github.com/stretchr/testify/require"
)

func encodePoint(point *geojson.Feature) []byte {
	data, _ := point.MarshalJSON()
	return data
}

func TestAPI(t *testing.T) {
	slog.SetLogLoggerLevel(slog.LevelDebug)
	mux := http.NewServeMux()

	if err := os.Remove("test_geo.db.json"); err != nil && !os.IsNotExist(err) {
		t.Fatal("remove error")
	}

	storage := NewStorage(mux, "test", "test_geo.db.json")
	router := NewRouter(mux, [][]string{{"test"}})
	storage.Run()
	router.Run()
	t.Cleanup(func() {
		storage.Stop()
		router.Stop()
	})

	firstPoint := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	firstPoint.ID = "test-id-1"
	firstPointBytes := encodePoint(firstPoint)
	slog.Debug("first", slog.Any("point", firstPoint))

	secondPoint := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	secondPoint.ID = "test-id-2"
	secondPointBytes := encodePoint(secondPoint)
	slog.Debug("second", slog.Any("point", secondPoint))

	replacementPoint := geojson.NewFeature(orb.Point{rand.Float64(), rand.Float64()})
	replacementPoint.ID = "test-id-2"
	replacementPointBytes := encodePoint(replacementPoint)
	slog.Debug("replacement", slog.Any("point", replacementPoint))

	firstCollection := geojson.NewFeatureCollection()
	firstCollection.Append(firstPoint)
	firstCollectionBytes, _ := firstCollection.MarshalJSON()

	secondCollection := geojson.NewFeatureCollection()
	secondCollection.Append(firstPoint)
	secondCollection.Append(secondPoint)
	secondCollectionBytes, _ := secondCollection.MarshalJSON()

	replacedCollection := geojson.NewFeatureCollection()
	replacedCollection.Append(firstPoint)
	replacedCollection.Append(replacementPoint)
	replacedCollectionBytes, _ := replacedCollection.MarshalJSON()

	tests := []struct {
		name       string
		method     string
		url        string
		body       []byte
		statusCode int
		response   []byte
	}{
		{
			name:       "Insert feature",
			method:     "POST",
			url:        "/insert",
			body:       firstPointBytes,
			statusCode: http.StatusOK,
			response:   nil,
		},
		{
			name:       "Select all features",
			method:     "GET",
			url:        "/select",
			body:       nil,
			statusCode: http.StatusOK,
			response:   firstCollectionBytes,
		},
		{
			name:       "Insert feature",
			method:     "POST",
			url:        "/insert",
			body:       secondPointBytes,
			statusCode: http.StatusOK,
			response:   nil,
		},
		{
			name:       "Select all features",
			method:     "GET",
			url:        "/select",
			body:       nil,
			statusCode: http.StatusOK,
			response:   secondCollectionBytes,
		},
		{
			name:       "Replace feature",
			method:     "POST",
			url:        "/replace",
			body:       replacementPointBytes,
			statusCode: http.StatusOK,
		},
		{
			name:       "Select all features",
			method:     "GET",
			url:        "/select",
			body:       nil,
			statusCode: http.StatusOK,
			response:   replacedCollectionBytes,
		},
		{
			name:   "Delete feature",
			method: "POST",
			url:    "/delete",
			body: func() []byte {
				data, _ := json.Marshal(map[string]string{"id": "test-id-2"})
				return data
			}(),
			statusCode: http.StatusOK,
		},
		{
			name:       "Select all features",
			method:     "GET",
			url:        "/select",
			body:       nil,
			statusCode: http.StatusOK,
			response:   firstCollectionBytes,
		},
	}

	for _, test := range tests {
		t.Run(test.name, func(t *testing.T) {
			body := bytes.NewReader(test.body)

			req, err := http.NewRequest(test.method, test.url, body)
			require.NoError(t, err)

			rec := httptest.NewRecorder()
			mux.ServeHTTP(rec, req)

			if rec.Code == http.StatusTemporaryRedirect {
				location := rec.Header().Get("Location")
				req, err = http.NewRequest(test.method, location, body)
				require.NoError(t, err)
				rec = httptest.NewRecorder()
				mux.ServeHTTP(rec, req)
			}

			require.Equal(t, test.statusCode, rec.Code, "wrong status code")

			if test.name == "Select all features" && rec.Code == http.StatusOK {
				// var collection geojson.FeatureCollection
				// require.NoError(t, json.NewDecoder(rec.Body).Decode(&collection))
				require.Equal(t, string(test.response), string(rec.Body.Bytes()))
			}
		})
	}
}
