#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h>

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
            using TSegments = TVector<TLevelSegmentPtr>;
            using TLeveledSsts = ::NKikimr::TLeveledSsts<TKey, TMemRec>;
            using TLeveledSstsIterator = typename TLeveledSsts::TIterator;

            static ui32 SstInputChunks(const TLevelSegment &sst) {
                if (!sst.AllChunks.empty()) {
                    return sst.AllChunks.size();
                }
                return sst.Info.Chunks ? sst.Info.Chunks : 1;
            }

            static ui64 SstKeepBytes(const TLevelSegment &sst) {
                if (TSstRatioPtr ratio = sst.StorageRatio.Get()) {
                    return ratio->IndexBytesKeep + ratio->InplacedDataKeep;
                }
                return ui64(sst.Info.IdxTotalSize) + sst.Info.InplaceDataTotalSize;
            }

            static ui64 SstHugeGarbageBytes(const TLevelSegment &sst) {
                if (TSstRatioPtr ratio = sst.StorageRatio.Get()) {
                    return ratio->HugeDataTotal > ratio->HugeDataKeep
                        ? ratio->HugeDataTotal - ratio->HugeDataKeep
                        : 0;
                }
                return 0;
            }

            // Conservative estimate of output index chunks from live index+inplaced bytes.
            static ui32 EstimateOutputChunks(ui64 keepBytes, ui32 chunkSize) {
                if (keepBytes == 0 || chunkSize == 0) {
                    return 0;
                }
                const ui32 suffix = sizeof(TIdxDiskPlaceHolder);
                const ui32 usable = chunkSize > suffix ? chunkSize - suffix : chunkSize;
                const ui64 withSlack = keepBytes + keepBytes / 10; // ~1.1x for alignment/outbound
                return static_cast<ui32>((withSlack + usable - 1) / usable);
            }

            static ui32 EstimateCompactSstsOutputChunks(
                    const typename TTask::TCompactSsts &compactSsts,
                    ui32 chunkSize)
            {
                ui64 keepBytes = 0;
                TLeveledSstsIterator it(&compactSsts.TablesToDelete);
                it.SeekToFirst();
                while (it.Valid()) {
                    keepBytes += SstKeepBytes(*it.Get().SstPtr);
                    it.Next();
                }
                return EstimateOutputChunks(keepBytes, chunkSize);
            }

            static void PreserveLastCompactedKey(
                    const TLevelSliceSnapshot &sliceSnap,
                    ui32 level,
                    typename TTask::TCompactSsts &compactSsts)
            {
                TSortedLevelsIter sortedLevelsIt(&sliceSnap);
                sortedLevelsIt.SeekToFirst();
                while (sortedLevelsIt.Valid()) {
                    auto r = sortedLevelsIt.Get();
                    if (r.Level == level) {
                        compactSsts.LastCompactedKey = r.SortedLevel.LastCompactedKey;
                        break;
                    }
                    sortedLevelsIt.Next();
                }
            }

            // Compact a contiguous run of SSTs on the same sorted level (packing / squeeze).
            static void CompactContiguousSsts(
                    const TLevelSliceSnapshot &sliceSnap,
                    ui32 level,
                    typename TSegments::const_iterator first,
                    typename TSegments::const_iterator last,
                    typename TTask::TCompactSsts &compactSsts)
            {
                compactSsts.TargetLevel = level;
                compactSsts.PushSstFromLevelX(level, first, last);
                PreserveLastCompactedKey(sliceSnap, level, compactSsts);
            }

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
                PreserveLastCompactedKey(sliceSnap, sstPtr.Level, compactSsts);
            }
        };

    } // NHullComp
} // NKikimr

