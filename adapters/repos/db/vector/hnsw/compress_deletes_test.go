//                           _       _
// __      _____  __ ___   ___  __ _| |_ ___
// \ \ /\ / / _ \/ _` \ \ / / |/ _` | __/ _ \
//  \ V  V /  __/ (_| |\ V /| | (_| | ||  __/
//   \_/\_/ \___|\__,_| \_/ |_|\__,_|\__\___|
//
//  Copyright © 2016 - 2023 Weaviate B.V. All rights reserved.
//
//  CONTACT: hello@weaviate.io
//

//go:build !race

package hnsw_test

import (
	"context"
	"testing"

	"github.com/stretchr/testify/assert"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/hnsw/distancer"
	"github.com/weaviate/weaviate/entities/cyclemanager"
	ent "github.com/weaviate/weaviate/entities/vectorindex/hnsw"

	"github.com/weaviate/weaviate/adapters/repos/db/vector/ssdhelpers"
	"github.com/weaviate/weaviate/adapters/repos/db/vector/testinghelpers"
)

func Test_NoRaceCompressDoesNotCrash(t *testing.T) {
	efConstruction := 64
	ef := 32
	maxNeighbors := 32
	dimensions := 20
	vectors_size := 10000
	queries_size := 100
	k := 100
	delete_indices := make([]uint64, 0, 1000)
	for i := 0; i < 1000; i++ {
		delete_indices = append(delete_indices, uint64(i+10))
	}
	delete_indices = append(delete_indices, uint64(1))

	vectors, queries := testinghelpers.RandomVecs(vectors_size, queries_size, dimensions)
	distancer := distancer.NewL2SquaredProvider()

	uc := ent.UserConfig{}
	uc.MaxConnections = maxNeighbors
	uc.EFConstruction = efConstruction
	uc.EF = ef
	uc.VectorCacheMaxObjects = 10e12
	uc.PQ = ent.PQConfig{Enabled: true, Encoder: ent.PQEncoder{Type: "title", Distribution: "normal"}}

	index, _ := hnsw.New(
		hnsw.Config{
			RootPath:              t.TempDir(),
			ID:                    "recallbenchmark",
			MakeCommitLoggerThunk: hnsw.MakeNoopCommitLogger,
			DistanceProvider:      distancer,
			VectorForIDThunk: func(ctx context.Context, id uint64) ([]float32, error) {
				return vectors[int(id)], nil
			},
			TempVectorForIDThunk: func(ctx context.Context, id uint64, container *hnsw.VectorSlice) ([]float32, error) {
				copy(container.Slice, vectors[int(id)])
				return container.Slice, nil
			},
		}, uc,
		cyclemanager.NewCycleCallbacksNoop(), cyclemanager.NewCycleCallbacksNoop(), cyclemanager.NewCycleCallbacksNoop())
	defer index.Shutdown(context.Background())
	ssdhelpers.Concurrently(uint64(len(vectors)), func(id uint64) {
		index.Add(uint64(id), vectors[id])
	})
	index.Delete(delete_indices...)

	cfg := ent.PQConfig{
		Enabled: true,
		Encoder: ent.PQEncoder{
			Type:         ent.PQEncoderTypeKMeans,
			Distribution: ent.PQEncoderDistributionLogNormal,
		},
		Segments:  dimensions,
		Centroids: 256,
	}
	index.Compress(cfg)
	for _, v := range queries {
		_, _, err := index.SearchByVector(v, k, nil)
		assert.Nil(t, err)
	}
}
