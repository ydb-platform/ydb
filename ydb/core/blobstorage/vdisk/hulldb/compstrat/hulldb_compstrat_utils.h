#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TUtils
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TUtils {
        public:
            using TTask = ::NKikimr::NHullComp::TTask<TKey, TMemRec>;
            using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
            using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
            using TLevelIndex = ::NKikimr::TLevelIndex<TKey, TMemRec>;
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
            using TSstIterator = typename TLevelSliceSnapshot::TSstIterator;
            using TSortedLevelsIter = typename TLevelSliceSnapshot::TSortedLevelsIter;
            using TLevelSstPtr = typename TLevelSegment::TLevelSstPtr;

            // rewrite one SST (compact it). All references to huge blobs would be removed
            static void SqueezeOneSst(
                    const TLevelSliceSnapshot &sliceSnap,
                    const TLevelSstPtr &sstPtr,
                    typename TTask::TCompactSsts &compactSsts)
            {
                // compact one sst
                compactSsts.TargetLevel = sstPtr.Level;
                compactSsts.PushOneSst(sstPtr.Level, sstPtr.SstPtr);

                // keep LastCompactedKey untouched (so find current value and set it)
                // by default compactSsts.LastCompactedKey is set to TKey::First()
                TSortedLevelsIter sortedLevelsIt(&sliceSnap);
                sortedLevelsIt.SeekToFirst();
                while (sortedLevelsIt.Valid()) {
                    auto r = sortedLevelsIt.Get();
                    if (r.Level == sstPtr.Level) {
                        compactSsts.LastCompactedKey = r.SortedLevel.LastCompactedKey;
                        break;
                    }
                    sortedLevelsIt.Next();
                }
            }
        };

    } // NHullComp
} // NKikimr

