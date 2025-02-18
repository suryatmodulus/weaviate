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
	"strings"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type shardCycleCallbacks struct {
	compactionCallbacks     cyclemanager.CycleCallbacks
	compactionCallbacksCtrl cyclemanager.CycleCallbackCtrl

	flushCallbacks     cyclemanager.CycleCallbacks
	flushCallbacksCtrl cyclemanager.CycleCallbackCtrl

	vectorCommitLoggerCallbacks     cyclemanager.CycleCallbacks
	vectorTombstoneCleanupCallbacks cyclemanager.CycleCallbacks
	vectorCombinedCallbacksCtrl     cyclemanager.CycleCallbackCtrl

	geoPropsCommitLoggerCallbacks     cyclemanager.CycleCallbacks
	geoPropsTombstoneCleanupCallbacks cyclemanager.CycleCallbacks
	geoPropsCombinedCallbacksCtrl     cyclemanager.CycleCallbackCtrl
}

func (s *Shard) initCycleCallbacks() {
	id := func(elems ...string) string {
		elems = append([]string{"shard", s.index.ID(), s.name}, elems...)
		return strings.Join(elems, "/")
	}

	compactionId := id("compaction")
	compactionCallbacks := cyclemanager.NewCycleCallbacks(compactionId, s.index.logger, 1)
	compactionCallbacksCtrl := s.index.cycleCallbacks.compactionCallbacks.Register(
		compactionId, true, compactionCallbacks.CycleCallback)

	flushId := id("flush")
	flushCallbacks := cyclemanager.NewCycleCallbacks(flushId, s.index.logger, 1)
	flushCallbacksCtrl := s.index.cycleCallbacks.flushCallbacks.Register(
		flushId, true, flushCallbacks.CycleCallback)

	vectorCommitLoggerId := id("vector", "commit_logger")
	vectorCommitLoggerCallbacks := cyclemanager.NewCycleCallbacks(vectorCommitLoggerId, s.index.logger, 1)
	vectorCommitLoggerCallbacksCtrl := s.index.cycleCallbacks.vectorCommitLoggerCallbacks.Register(
		vectorCommitLoggerId, true, vectorCommitLoggerCallbacks.CycleCallback)

	vectorTombstoneCleanupId := id("vector", "tombstone_cleanup")
	vectorTombstoneCleanupCallbacks := cyclemanager.NewCycleCallbacks(vectorTombstoneCleanupId, s.index.logger, 1)
	vectorTombstoneCleanupCallbacksCtrl := s.index.cycleCallbacks.vectorTombstoneCleanupCallbacks.Register(
		vectorTombstoneCleanupId, true, vectorTombstoneCleanupCallbacks.CycleCallback)

	vectorCombinedCallbacksCtrl := cyclemanager.NewCycleCombinedCallbackCtrl(2,
		vectorCommitLoggerCallbacksCtrl, vectorTombstoneCleanupCallbacksCtrl)

	geoPropsCommitLoggerId := id("geo_props", "commit_logger")
	geoPropsCommitLoggerCallbacks := cyclemanager.NewCycleCallbacks(geoPropsCommitLoggerId, s.index.logger, 1)
	geoPropsCommitLoggerCallbacksCtrl := s.index.cycleCallbacks.geoPropsCommitLoggerCallbacks.Register(
		geoPropsCommitLoggerId, true, geoPropsCommitLoggerCallbacks.CycleCallback)

	geoPropsTombstoneCleanupId := id("geoProps", "tombstone_cleanup")
	geoPropsTombstoneCleanupCallbacks := cyclemanager.NewCycleCallbacks(geoPropsTombstoneCleanupId, s.index.logger, 1)
	geoPropsTombstoneCleanupCallbacksCtrl := s.index.cycleCallbacks.geoPropsTombstoneCleanupCallbacks.Register(
		geoPropsTombstoneCleanupId, true, geoPropsTombstoneCleanupCallbacks.CycleCallback)

	geoPropsCombinedCallbacksCtrl := cyclemanager.NewCycleCombinedCallbackCtrl(2,
		geoPropsCommitLoggerCallbacksCtrl, geoPropsTombstoneCleanupCallbacksCtrl)

	s.cycleCallbacks = &shardCycleCallbacks{
		compactionCallbacks:     compactionCallbacks,
		compactionCallbacksCtrl: compactionCallbacksCtrl,

		flushCallbacks:     flushCallbacks,
		flushCallbacksCtrl: flushCallbacksCtrl,

		vectorCommitLoggerCallbacks:     vectorCommitLoggerCallbacks,
		vectorTombstoneCleanupCallbacks: vectorTombstoneCleanupCallbacks,
		vectorCombinedCallbacksCtrl:     vectorCombinedCallbacksCtrl,

		geoPropsCommitLoggerCallbacks:     geoPropsCommitLoggerCallbacks,
		geoPropsTombstoneCleanupCallbacks: geoPropsTombstoneCleanupCallbacks,
		geoPropsCombinedCallbacksCtrl:     geoPropsCombinedCallbacksCtrl,
	}
}
