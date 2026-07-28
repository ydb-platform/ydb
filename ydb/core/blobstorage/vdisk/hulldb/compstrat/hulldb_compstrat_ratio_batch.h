#pragma once

#include "defs.h"
#include "hulldb_compstrat_ratio_iterators.h"
#include "hulldb_compstrat_ratio_stat.h"
#include "hulldb_compstrat_yield.h"
#include <ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_essence.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullrecmerger.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <util/digest/numeric.h>

#include <optional>
#include <type_traits>
#include <utility>

namespace NKikimr::NHullComp {

    // Full-snapshot calculation, independent of the legacy scheduling/scan.
    // The caller supplies the per-SST calculator for the fallback cases.
    template <class TKey, class TMemRec>
    class TStorageRatioFullBatch {
        using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
        using TLevelSliceSnapshot = ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec>;
        using TSstIterator = typename TLevelSliceSnapshot::TSstIterator;
        using TLevelSegment = ::NKikimr::TLevelSegment<TKey, TMemRec>;
        using TLevelSegmentPtr = TIntrusivePtr<TLevelSegment>;
        using TIndexRecordMerger = ::NKikimr::TIndexRecordMerger<TKey, TMemRec>;

    public:
        TStorageRatioFullBatch(
                const THullCtxPtr& hullCtx,
                const TLevelIndexSnapshot& levelSnap,
                const TBarriersSnapshot::TBarriersEssence& barriersEssence,
                bool allowGarbageCollection,
                TCompactionYield* yield = nullptr)
            : HullCtx(hullCtx)
            , LevelSnap(levelSnap)
            , BarriersEssence(barriersEssence)
            , AllowGarbageCollection(allowGarbageCollection)
            , Yield(yield)
        {}

        static bool IsCalculationDue(THullCtx& hullCtx, TInstant now) {
            auto& nextCalculationTime =
                hullCtx.StorageRatioFullBatchNextCalculationTime;
            if (!nextCalculationTime) {
                // Ratios are not persisted, so every VDisk starts without them
                // after a restart. The first full scan is deliberately delayed
                // by the VDisk's phase so that VDisks restarted together do not
                // all scan their databases at once.
                nextCalculationTime = GetNextCalculationTime(
                    hullCtx,
                    now,
                    hullCtx.HullCompStorageRatioCalcPeriod);
            }
            return now >= *nextCalculationTime;
        }

        static void ScheduleNextCalculation(THullCtx& hullCtx, TInstant now) {
            hullCtx.StorageRatioFullBatchNextCalculationTime = GetNextCalculationTime(
                hullCtx, now, hullCtx.HullCompStorageRatioCalcPeriod);
        }

        template <class TCalculateSstRatio>
        void Calculate(
                TInstant startTime,
                TStorageRatioStat& stat,
                const TCalculateSstRatio& calculateSstRatio)
        {
            Calculate(startTime, stat, calculateSstRatio, TStorageRatioIteratorFactory<TKey, TMemRec>{});
        }

        template <class TCalculateSstRatio, class TIteratorFactory>
        void Calculate(
                TInstant startTime,
                TStorageRatioStat& stat,
                const TCalculateSstRatio& calculateSstRatio,
                const TIteratorFactory& iterators)
        {
            TVector<TLevelSegmentPtr> ssts;
            ssts.reserve(1000u);
            TSstIterator it(&LevelSnap.SliceSnap);
            it.SeekToFirst();
            while (it.Valid()) {
                ssts.push_back(it.Get().SstPtr);
                it.Next();
            }

            // Preserve the per-SST fast path for disjoint key ranges.
            if (!HaveOverlappingKeyRanges(ssts)) {
                stat.NonOverlappingFallback = ssts.size() > 1;
                for (const auto& sst : ssts) {
                    CheckCompactionYield(Yield);
                    auto ratio = calculateSstRatio(sst);
                    sst->StorageRatio.Set(ratio, ratio->Time);
                    ++stat.SstsChecked;
                }
            } else {
                stat.UsedBatchAlgorithm = true;
                auto ratios = CalculateSstRatios(ssts, startTime, iterators);
                for (auto& [sst, ratio] : ratios) {
                    sst->StorageRatio.Set(ratio, ratio->Time);
                    ++stat.SstsChecked;
                }
            }
        }

    private:
        // All borrowed state outlives the synchronous Calculate() call.
        const THullCtxPtr HullCtx;
        const TLevelIndexSnapshot& LevelSnap;
        const TBarriersSnapshot::TBarriersEssence& BarriersEssence;
        const bool AllowGarbageCollection;
        TCompactionYield* const Yield;

        static TInstant GetNextCalculationTime(
                const THullCtx& hullCtx,
                TInstant now,
                TDuration calcPeriod)
        {
            const ui64 periodUs = calcPeriod.MicroSeconds();
            if (!periodUs) {
                return now;
            }

            const ui64 vdiskHash = CombineHashes(
                IntHash<ui64>(hullCtx.VCtx->GroupId.GetRawId()),
                IntHash<ui64>(hullCtx.VCtx->ShortSelfVDisk.GetRaw()));
            const ui64 phaseUs = vdiskHash % periodUs;
            const ui64 nowUs = now.MicroSeconds();
            const ui64 cycleStartUs = nowUs - nowUs % periodUs;

            ui64 nextCalculationUs = cycleStartUs + phaseUs;
            if (nextCalculationUs <= nowUs) {
                nextCalculationUs += periodUs;
            }

            return TInstant::MicroSeconds(nextCalculationUs);
        }

        static bool HaveOverlappingKeyRanges(
                const TVector<TLevelSegmentPtr>& ssts)
        {
            TVector<std::pair<TKey, TKey>> ranges;
            ranges.reserve(ssts.size());
            for (const TLevelSegmentPtr& sst : ssts) {
                if (sst->Elements()) {
                    ranges.emplace_back(sst->FirstKey(), sst->LastKey());
                }
            }

            Sort(ranges.begin(), ranges.end());
            for (size_t i = 1; i < ranges.size(); ++i) {
                if (!(ranges[i - 1].second < ranges[i].first)) {
                    return true;
                }
            }
            return false;
        }

        struct TSourceRecord {
            size_t AccumulatorIndex;
            ui64 InplacedDataSize;
            ui64 HugeDataSize;
            ui32 NumKeepFlags;
            ui32 NumDoNotKeepFlags;
        };

        using TAccumulatorIndices = THashMap<const TLevelSegment*, size_t>;

        class TStorageRatioMerger {
            TIndexRecordMerger DbMerger;
            const TAccumulatorIndices& AccumulatorIndices;
            TVector<TSourceRecord> Sources;

        public:
            TStorageRatioMerger(const TBlobStorageGroupType& gtype, const TAccumulatorIndices& accumulatorIndices)
                : DbMerger(gtype)
                , AccumulatorIndices(accumulatorIndices)
            {}

            void AddFromSegment(
                    const TMemRec& memRec,
                    const TDiskPart* outbound,
                    const TKey& key,
                    ui64 lsn,
                    const void* sst)
            {
                const ui32 keepBefore = DbMerger.GetNumKeepFlags();
                const ui32 doNotKeepBefore = DbMerger.GetNumDoNotKeepFlags();
                DbMerger.AddFromSegment(memRec, outbound, key, lsn, sst);

                const auto it = AccumulatorIndices.find(static_cast<const TLevelSegment*>(sst));
                if (it == AccumulatorIndices.end()) {
                    return;
                }

                TDiskDataExtractor extractor;
                memRec.GetDiskData(&extractor, outbound);
                Sources.push_back({
                    it->second,
                    extractor.GetInplacedDataSize(),
                    extractor.GetHugeDataSize(),
                    DbMerger.GetNumKeepFlags() - keepBefore,
                    DbMerger.GetNumDoNotKeepFlags() - doNotKeepBefore,
                });
            }

            void AddFromFresh(
                    const TMemRec& memRec,
                    const TRope *data,
                    const TKey& key,
                    ui64 lsn)
            {
                DbMerger.AddFromFresh(memRec, data, key, lsn);
                // Fresh contributes to the merged database record, but it
                // does not have a StorageRatio of its own.
            }

            const TVector<TSourceRecord>& GetSources() const {
                return Sources;
            }

            const TIndexRecordMerger& GetDbMerger() const {
                return DbMerger;
            }

            bool HaveToMergeData() const {
                return DbMerger.HaveToMergeData();
            }

            void Finish() {
                DbMerger.Finish();
            }

            void Clear() {
                DbMerger.Clear();
                Sources.clear();
            }
        };

        struct TAccumulator {
            TLevelSegmentPtr Sst;
            TSstRatioPtr Ratio;
            ui64 ProcessedItems = 0;
            ui64 TotalItems = 0;

            bool Complete() const {
                return ProcessedItems == TotalItems;
            }
        };

        ui64 UpdateRatios(
                const TKey& key,
                const TStorageRatioMerger& merger,
                TVector<TAccumulator>& accumulators)
        {
            const TIndexRecordMerger& dbMerger = merger.GetDbMerger();

            constexpr ui64 indexItemByteSize = sizeof(TKey) + sizeof(TMemRec);
            ui64 sourceRecordsProcessed = 0;

            for (const TSourceRecord& source : merger.GetSources()) {
                TAccumulator& accumulator = accumulators[source.AccumulatorIndex];
                TSstRatio& ratio = *accumulator.Ratio;
                ++sourceRecordsProcessed;

                const ui64 inplacedDataSize = source.InplacedDataSize;
                const ui64 hugeDataSize = source.HugeDataSize;

                // Account for the physical record stored in this SST.
                ratio.IndexItemsTotal++;
                ratio.IndexBytesTotal += indexItemByteSize;
                ratio.InplacedDataTotal += inplacedDataSize;
                ratio.HugeDataTotal += hugeDataSize;

                const NGc::TKeepStatus keep = BarriersEssence.Keep(
                    key,
                    dbMerger.GetMemRec(),
                    {
                        source.NumKeepFlags,
                        source.NumDoNotKeepFlags,
                        dbMerger.GetNumKeepFlags(),
                        dbMerger.GetNumDoNotKeepFlags(),
                    },
                    HullCtx->AllowKeepFlags,
                    AllowGarbageCollection);

                if (keep.KeepIndex) {
                    ratio.IndexItemsKeep++;
                    ratio.IndexBytesKeep += indexItemByteSize;
                }

                if (keep.KeepData) {
                    ratio.InplacedDataKeep += inplacedDataSize;
                    ratio.HugeDataKeep += hugeDataSize;
                }

                ++accumulator.ProcessedItems;
                Y_ABORT_UNLESS(accumulator.ProcessedItems <= accumulator.TotalItems);
            }

            return sourceRecordsProcessed;
        }

        template <class TIterator>
        void Scan(
                TIterator& heapIt,
                const TKey& firstKey,
                ui64 remainingItems,
                const TAccumulatorIndices& accumulatorIndices,
                TVector<TAccumulator>& accumulators)
        {
            TStorageRatioMerger merger(HullCtx->VCtx->Top->GType, accumulatorIndices);

            // Skip the Fresh-only prefix and stop after all SST records,
            // without scanning a Fresh-only tail.
            heapIt.Seek(firstKey);
            while (heapIt.Valid() && remainingItems) {
                CheckCompactionYield(Yield);
                const TKey key = heapIt.GetCurKey();
                heapIt.PutToMergerAndAdvance(&merger);
                // A key present only in Fresh contributes to no SST ratio.
                if (!merger.GetSources().empty()) {
                    merger.Finish();
                    remainingItems -= UpdateRatios(key, merger, accumulators);
                }
                merger.Clear();
            }
        }

        template <class TIteratorFactory>
        TVector<std::pair<TLevelSegmentPtr, TSstRatioPtr>> CalculateSstRatios(
                const TVector<TLevelSegmentPtr>& ssts,
                TInstant now,
                const TIteratorFactory& iterators)
        {
            TVector<TAccumulator> accumulatorStorage;
            accumulatorStorage.reserve(ssts.size());

            TAccumulatorIndices accumulatorIndices;
            accumulatorIndices.reserve(ssts.size());

            std::optional<TKey> firstKey;
            ui64 remainingItems = 0;

            for (const TLevelSegmentPtr& sst : ssts) {
                const ui64 totalItems = sst->Elements();

                const size_t accumulatorIndex = accumulatorStorage.size();
                const bool inserted = accumulatorIndices.emplace(
                    sst.Get(),
                    accumulatorIndex).second;
                // An SST belongs to exactly one level of this snapshot.
                Y_ABORT_UNLESS(inserted,
                    "%s StorageRatio: duplicate SST in slice snapshot: "
                    "sst# %p assignedSstId# %" PRIu64,
                    HullCtx->VCtx->VDiskLogPrefix.data(),
                    static_cast<const void*>(sst.Get()),
                    sst->AssignedSstId);

                accumulatorStorage.push_back({
                    .Sst = sst,
                    .Ratio = MakeIntrusive<TSstRatio>(now),
                    .ProcessedItems = 0,
                    .TotalItems = totalItems,
                });

                if (totalItems) {
                    const TKey key = sst->FirstKey();
                    if (!firstKey || key < *firstKey) {
                        firstKey = key;
                    }
                    remainingItems += totalItems;
                }
            }

            if (firstKey) {
                iterators.WithHeapIterator(HullCtx, LevelSnap, [&](auto& heapIt) {
                    Scan(heapIt, *firstKey, remainingItems, accumulatorIndices, accumulatorStorage);
                });
            }

            TVector<std::pair<TLevelSegmentPtr, TSstRatioPtr>> result;
            result.reserve(ssts.size());
            for (const TAccumulator& accumulator : accumulatorStorage) {
                Y_ABORT_UNLESS(
                    accumulator.Complete(),
                    "%s StorageRatio calculation did not process all SST items: "
                    "sst# %p processed# %" PRIu64 " total# %" PRIu64,
                    HullCtx->VCtx->VDiskLogPrefix.data(),
                    static_cast<const void*>(accumulator.Sst.Get()),
                    accumulator.ProcessedItems,
                    accumulator.TotalItems);
                result.emplace_back(accumulator.Sst, accumulator.Ratio);
            }

            return result;
        }
    };

} // NKikimr::NHullComp
