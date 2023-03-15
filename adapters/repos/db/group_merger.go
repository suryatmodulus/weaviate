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
	"sort"

	"github.com/weaviate/weaviate/entities/additional"
	"github.com/weaviate/weaviate/entities/searchparams"
	"github.com/weaviate/weaviate/entities/storobj"
)

type groupMerger struct {
	objects []*storobj.Object
	dists   []float32
	groupBy *searchparams.GroupBy
	limit   int
}

func newGroupMerger(objects []*storobj.Object, dists []float32,
	groupBy *searchparams.GroupBy, limit int,
) *groupMerger {
	return &groupMerger{objects, dists, groupBy, limit}
}

func (gm *groupMerger) Do() ([]*storobj.Object, []float32, error) {
	groups := map[string][]additional.Group{}
	objects := map[string][]int{}

	for i, obj := range gm.objects {
		g, ok := obj.AdditionalProperties()["group"]
		if !ok {
			continue
		}
		group, ok := g.(additional.Group)
		if !ok {
			continue
		}
		groups[group.Value] = append(groups[group.Value], group)
		objects[group.Value] = append(objects[group.Value], i)
	}

	i := 0
	objs := make([]*storobj.Object, len(groups))
	dists := make([]float32, len(groups))
	for val, group := range groups {
		if i > gm.groupBy.Groups {
			break
		}
		count := 0
		hits := []additional.GroupHit{}
		for _, g := range group {
			count += g.Count
			hits = append(hits, g.Hits...)
		}

		sort.Slice(hits, func(i, j int) bool {
			return hits[i].Additional.Distance < hits[j].Additional.Distance
		})

		if len(hits) > gm.groupBy.ObjectsPerGroup {
			hits = hits[:gm.groupBy.ObjectsPerGroup]
		}

		indx := objects[val][0]
		obj, dist := gm.objects[indx], gm.dists[indx]
		obj.AdditionalProperties()["group"] = additional.Group{
			ID:          i,
			Count:       count,
			MaxDistance: hits[0].Additional.Distance,
			MinDistance: hits[len(hits)-1].Additional.Distance,
			Hits:        hits,
			Value:       val,
		}
		objs[i], dists[i] = obj, dist
		i++
	}

	// TODO: check if it's needed
	objs, dists = newDistancesSorter().sort(objs[:i], dists[:i])
	return objs, dists, nil
}
