#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_glue.h>
#include <type_traits>
// TLevelIndex / TLevelIndexSnapshot: this header used to get them only from whatever
// included it first, which broke as soon as the selector header started including it.
#include <ydb/core/blobstorage/vdisk/hulldb/generic/hullds_idx.h>

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

            // Blocks and Barriers go into the stripe heap; LogoBlobs keep exclusive chunks.
            // stripeSstBytes is HeapAllocatorMaxSstInBytes, 0 when that placement is off.
            static bool PlaceOutputInStripe(ui32 appendBlockSize, ui32 stripeSstBytes) {
                if constexpr (std::is_same_v<TKey, TKeyLogoBlob>) {
                    return false;
                }
                return appendBlockSize > 0 && stripeSstBytes > 0;
            }

            static ui32 StripeBlocksForBytes(ui64 bytes, ui32 appendBlockSize) {
                if (bytes == 0 || appendBlockSize == 0) {
                    return 0;
                }
                return static_cast<ui32>((bytes + appendBlockSize - 1) / appendBlockSize);
            }

            // The heap aligns the reservation up to an append block, and that aligned
            // size is the chunk the SST writer packs into.
            static ui32 AlignStripeCapacity(ui32 bytes, ui32 appendBlockSize) {
                if (bytes == 0 || appendBlockSize == 0) {
                    return bytes;
                }
                const ui64 aligned = (ui64(bytes) + appendBlockSize - 1) / appendBlockSize * appendBlockSize;
                return static_cast<ui32>(Min<ui64>(aligned, Max<ui32>()));
            }

            static ui32 SstInputChunks(const TLevelSegment &sst) {
                // The stripe chunk is shared. Deleting the SST returns its extent, not
                // the chunk; AllChunks still names that chunk, so it must not be counted.
                if (!sst.HeapStripe.Empty()) {
                    return 0;
                }
                if (!sst.AllChunks.empty()) {
                    return sst.AllChunks.size();
                }
                return sst.Info.Chunks ? sst.Info.Chunks : 1;
            }

            // Append blocks the stripe heap frees when this SST is deleted. The heap
            // rounds the extent up on free, same as TStripeHeap::Free.
            static ui32 SstReleasedStripeBlocks(const TLevelSegment &sst, ui32 appendBlockSize) {
                if (sst.HeapStripe.Empty()) {
                    return 0;
                }
                return StripeBlocksForBytes(sst.HeapStripe.Size, appendBlockSize);
            }

            static ui64 SstKeepBytes(const TLevelSegment &sst) {
                // ManyHugeBlobs keep their payload in the HugeKeeper, but the rewritten
                // SST still stores one TDiskPart per live erasure part in its outbound
                // array. StorageRatio counts the payload and index bytes only; include the
                // source outbound cardinality as a conservative upper bound for this
                // metadata, otherwise high-part-count blobs can exceed the broker grant.
                const ui64 outboundBytes = ui64(sst.Info.OutboundItems) * sizeof(TDiskPart);
                if (TSstRatioPtr ratio = sst.StorageRatio.Get()) {
                    return ratio->IndexBytesKeep + ratio->InplacedDataKeep + outboundBytes;
                }
                return ui64(sst.Info.IdxTotalSize) + sst.Info.InplaceDataTotalSize + outboundBytes;
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

            // Exclusive chunks the job will reserve. Zero when the output SST is written
            // into the stripe heap instead: that allocation is stripe blocks, not a
            // TEvChunkReserve of an index chunk.
            static ui32 EstimateJobOutputChunks(ui64 keepBytes, ui32 chunkSize,
                    ui32 appendBlockSize, ui32 stripeSstBytes)
            {
                if (PlaceOutputInStripe(appendBlockSize, stripeSstBytes)) {
                    return 0;
                }
                return EstimateOutputChunks(keepBytes, chunkSize);
            }

            // Append blocks the rewritten SST will occupy after the stripe reservation
            // is shrunk to the bytes actually written. Same slack and placeholder as
            // EstimateOutputChunks; a record that does not fit starts another stripe.
            static ui32 EstimateOutputStripeBlocks(ui64 keepBytes, ui32 appendBlockSize, ui32 maxStripeBytes) {
                if (!PlaceOutputInStripe(appendBlockSize, maxStripeBytes) || keepBytes == 0) {
                    return 0;
                }
                const ui32 capacity = AlignStripeCapacity(maxStripeBytes, appendBlockSize);
                const ui32 suffix = sizeof(TIdxDiskPlaceHolder);
                const ui32 usable = capacity > suffix ? capacity - suffix : 0;
                const ui64 withSlack = keepBytes + keepBytes / 10;
                if (usable == 0) {
                    return StripeBlocksForBytes(withSlack + suffix, appendBlockSize);
                }
                ui32 blocks = 0;
                ui64 left = withSlack;
                while (left) {
                    const ui64 piece = Min<ui64>(left, usable);
                    blocks += StripeBlocksForBytes(piece + suffix, appendBlockSize);
                    left -= piece;
                }
                return blocks;
            }

            static ui32 EstimateCompactSstsOutputChunks(
                    const typename TTask::TCompactSsts &compactSsts,
                    ui32 chunkSize,
                    ui32 appendBlockSize = 0,
                    ui32 stripeSstBytes = 0)
            {
                if (PlaceOutputInStripe(appendBlockSize, stripeSstBytes)) {
                    return 0;
                }
                ui64 keepBytes = 0;
                TLeveledSstsIterator it(&compactSsts.TablesToDelete);
                it.SeekToFirst();
                while (it.Valid()) {
                    keepBytes += SstKeepBytes(*it.Get().SstPtr);
                    it.Next();
                }
                return EstimateOutputChunks(keepBytes, chunkSize);
            }

            // What the job costs and what it gives back, for the compaction broker. The
            // output figure is the same conservative estimate the selection strategies
            // budget against, so a job that was admitted can also be reserved for.
            static typename TTask::TSpaceForecast ForecastCompactSsts(
                    const typename TTask::TCompactSsts &compactSsts,
                    ui32 chunkSize,
                    ui32 appendBlockSize = 0,
                    ui32 stripeSstBytes = 0)
            {
                typename TTask::TSpaceForecast forecast;
                ui64 keepBytes = 0;
                TLeveledSstsIterator it(&compactSsts.TablesToDelete);
                it.SeekToFirst();
                while (it.Valid()) {
                    const TLevelSegment &sst = *it.Get().SstPtr;
                    keepBytes += SstKeepBytes(sst);
                    forecast.InputChunks += SstInputChunks(sst);
                    forecast.StripeBlocksReleased += SstReleasedStripeBlocks(sst, appendBlockSize);
                    forecast.HugeGarbageBytes += SstHugeGarbageBytes(sst);
                    it.Next();
                }
                forecast.OutputChunks = EstimateJobOutputChunks(keepBytes, chunkSize,
                    appendBlockSize, stripeSstBytes);
                forecast.StripeBlocksAllocated = EstimateOutputStripeBlocks(keepBytes,
                    appendBlockSize, stripeSstBytes);
                forecast.Valid = true;
                return forecast;
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
