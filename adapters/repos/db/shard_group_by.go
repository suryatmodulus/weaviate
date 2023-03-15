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

package db

import (
	"context"
	"encoding/binary"
	"fmt"
	"time"

	"github.com/davecgh/go-spew/spew"
	"github.com/go-openapi/strfmt"
	"github.com/pkg/errors"
	"github.com/weaviate/weaviate/adapters/repos/db/helpers"
	"github.com/weaviate/weaviate/adapters/repos/db/lsmkv"
	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/models"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

func (s *Shard) groupResults(ctx context.Context, ids []uint64,
	dists []float32, params *searchparams.GroupBy,
	additional additional.Properties,
) ([]*storobj.Object, []float32, error) {
	objsBucket := s.store.Bucket(helpers.ObjectsBucketLSM)
	objs, err := newGrouper(ids, dists, params, objsBucket, additional).Do(ctx)

	return objs, dists, err
}

type grouper struct {
	ids        []uint64
	dists      []float32
	params     *searchparams.GroupBy
	additional additional.Properties
	objBucket  *lsmkv.Bucket
}

func newGrouper(ids []uint64, dists []float32,
	params *searchparams.GroupBy, objBucket *lsmkv.Bucket,
	additional additional.Properties,
) *grouper {
	return &grouper{
		ids:        ids,
		dists:      dists,
		params:     params,
		objBucket:  objBucket,
		additional: additional,
	}
}

func (g *grouper) Do(ctx context.Context) ([]*storobj.Object, error) {
	before := time.Now()
	docIDBytes := make([]byte, 8)

	groups := map[string][]uint64{}
	distances := map[string][]float32{}
	rawObjectData := map[uint64][]byte{}

DOCS_LOOP:
	for i, docID := range g.ids {
		binary.LittleEndian.PutUint64(docIDBytes, docID)
		objData, err := g.objBucket.GetBySecondary(0, docIDBytes)
		if err != nil {
			return nil, errors.Wrapf(err, "lsm sorter - could not get obj by doc id %d", docID)
		}
		if objData == nil {
			continue
		}
		value, ok, _ := storobj.ParseAndExtractProperty(objData, g.params.Property)
		if len(value) == 0 || !ok {
			// TODO: we need to explicitly handle null values
			fmt.Printf("null value == val: %v ok: %v\n", value, ok)
			continue
		}

		for _, val := range g.getValues(value) {
			current, ok := groups[val]
			if !ok && len(groups) >= g.params.Groups {
				continue DOCS_LOOP
			}

			if len(current) >= g.params.ObjectsPerGroup {
				continue DOCS_LOOP
			}

			groups[val] = append(current, docID)
			distances[val] = append(distances[val], g.dists[i])
		}

		rawObjectData[docID] = objData
	}

	i := 0
	objs := make([]*storobj.Object, len(groups))
	for val, docIDs := range groups {
		fmt.Printf("-----Group: val: %v docIDs: %v\n", val, docIDs)
		hits := []additional.GroupHit{}
		for j, docID := range docIDs {
			objData, ok := rawObjectData[docID]
			if !ok {
				fmt.Printf("ERROR no obj found for %v docID\n", docID)
				continue
			}
			prop, _, err := storobj.ParseAndExtractProperty(objData, "id")
			if err != nil {
				fmt.Printf("ERROR no obj found for %v docID\n", docID)
				continue
			}
			hits = append(hits, additional.GroupHit{
				Content: strfmt.UUID(prop[0]).String(),
				Additional: &additional.GroupHitAdditional{
					Distance: distances[val][j],
				},
			})
		}
		group := additional.Group{
			ID:          i,
			Value:       val,
			Count:       len(hits),
			Hits:        hits,
			MaxDistance: distances[val][0],
			MinDistance: distances[val][len(distances[val])-1],
		}

		// add group
		objData := rawObjectData[docIDs[0]]
		unmarshalled, err := storobj.FromBinaryOptional(objData, g.additional)
		if err != nil {
			return nil, errors.Wrapf(err, "unmarshal data object at position %d", i)
		}

		if unmarshalled.AdditionalProperties() == nil {
			unmarshalled.Object.Additional = models.AdditionalProperties{}
		}
		unmarshalled.AdditionalProperties()["group"] = group
		objs[i] = unmarshalled
		i++
	}

	fmt.Printf("retrieve, partial parse, group objects took %s\n", time.Since(before))
	spew.Dump(groups)

	return objs, nil
}

func (g *grouper) getValues(values []string) []string {
	vals := make([]string, len(values))
	for i := range values {
		vals[i] = values[i]
	}
	return vals
}
