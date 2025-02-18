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

package lsmkv

import (
	"path/filepath"
	"strings"

	"github.com/weaviate/weaviate/entities/cyclemanager"
)

type storeCycleCallbacks struct {
	compactionCallbacks     cyclemanager.CycleCallbacks
	compactionCallbacksCtrl cyclemanager.CycleCallbackCtrl

	flushCallbacks     cyclemanager.CycleCallbacks
	flushCallbacksCtrl cyclemanager.CycleCallbackCtrl
}

func (s *Store) initCycleCallbacks(classCompactionCallbacks, classFlushCallbacks cyclemanager.CycleCallbacks) {
	id := func(elems ...string) string {
		path, err := filepath.Rel(s.dir, s.rootDir)
		if err != nil {
			path = s.dir
		}
		elems = append([]string{"store"}, elems...)
		elems = append(elems, path)
		return strings.Join(elems, "/")
	}

	compactionId := id("compaction")
	compactionCallbacks := cyclemanager.NewCycleCallbacks(compactionId, s.logger, 1)
	compactionCallbacksCtrl := classCompactionCallbacks.Register(
		compactionId, true, compactionCallbacks.CycleCallback)

	flushId := id("flush")
	flushCallbacks := cyclemanager.NewCycleCallbacks(flushId, s.logger, 1)
	flushCallbacksCtrl := classFlushCallbacks.Register(
		flushId, true, flushCallbacks.CycleCallback)

	s.cycleCallbacks = &storeCycleCallbacks{
		compactionCallbacks:     compactionCallbacks,
		compactionCallbacksCtrl: compactionCallbacksCtrl,

		flushCallbacks:     flushCallbacks,
		flushCallbacksCtrl: flushCallbacksCtrl,
	}
}
