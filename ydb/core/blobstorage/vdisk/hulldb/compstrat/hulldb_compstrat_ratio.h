#pragma once

#include "defs.h"
#include "hulldb_compstrat_defs.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_heap_it.h>
#include <ydb/core/blobstorage/vdisk/hulldb/hull_ds_all_snap.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullmergeits.h>
#include <util/digest/numeric.h>

#include <type_traits>
#include <utility>

namespace NKikimr {
    namespace NHullComp {

        ////////////////////////////////////////////////////////////////////////////
        // NHullComp::TStrategyStorageRatio
        ////////////////////////////////////////////////////////////////////////////
        template <class TKey, class TMemRec>
        class TStrategyStorageRatio {
        public:
            using TLevelIndexSnapshot = ::NKikimr::TLevelIndexSnapshot<TKey, TMemRec>;
            typedef ::NKikimr::TLevelSliceSnapshot<TKey, TMemRec> TLevelSliceSnapshot;
            typedef typename TLevelSliceSnapshot::TSstIterator TSstIterator;
            typedef ::NKikimr::TLevelSegment<TKey, TMemRec> TLevelSegment;
            typedef TIntrusivePtr<TLevelSegment> TLevelSegmentPtr;
            typedef typename TLevelSegment::TLevelSstPtr TLevelSstPtr;
            typedef typename TLevelSegment::TMemIterator TMemIterator;
            typedef typename TLevelIndexSnapshot::TForwardIterator TLevelIt;
            typedef ::NKikimr::TIndexRecordMerger<TKey, TMemRec> TIndexRecordMerger;

            struct TCalcStat {
                // Logical keys for which a ratio contribution was evaluated.
                ui64 KeysProcessed = 0;
                // SST records attributed to ratio accumulators.
                ui64 SourceRecordsProcessed = 0;
                // Fresh and SST records put into whole-database mergers.
                ui64 DbRecordsMerged = 0;
                // Explicit Seek calls made by the calculation.
                ui64 Seeks = 0;
            };

            TStrategyStorageRatio(TIntrusivePtr<THullCtx> hullCtx,
                                  const TLevelIndexSnapshot &levelSnap,
                                  TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> &&barriersEssence,
                                  bool allowGarbageCollection,
                                  TCalcStat* calcStat = nullptr)
                : HullCtx(std::move(hullCtx))
                , LevelSnap(levelSnap)
                , BarriersEssence(std::move(barriersEssence))
                , AllowGarbageCollection(allowGarbageCollection)
                , CalcStat(calcStat)
            {}


            void Work() {
                const bool useBatchAlgorithm =
                    HullCtx->VCfg->FeatureFlags.GetEnableHullCompStorageRatioOptimization();

                if (CalcStat) {
                    *CalcStat = {};
                }

                TInstant startTime(TAppData::TimeProvider->Now());
                TStat stat;
                if (CalcStat) {
                    if (useBatchAlgorithm) {
                        UpdateStorageRatioForDbBatch<true>(startTime, stat);
                    } else {
                        UpdateStorageRatioForDb<true>(startTime, stat);
                    }
                } else {
                    if (useBatchAlgorithm) {
                        UpdateStorageRatioForDbBatch<false>(startTime, stat);
                    } else {
                        UpdateStorageRatioForDb<false>(startTime, stat);
                    }
                }
                TInstant finishTime(TAppData::TimeProvider->Now());
                if (HullCtx->VCtx->ActorSystem) {
                    YDB_LOG_DEBUG_CTX_COMP(*HullCtx->VCtx->ActorSystem, NKikimrServices::BS_HULLCOMP, VDISKP(HullCtx->VCtx->VDiskLogPrefix, "%s: StorageRatio: timeSpent# %s stat# %s", PDiskSignatureForHullDbKey<TKey>().ToString().data(), (finishTime - startTime).ToString().data(), stat.ToString().data()));
                }

                BarriersEssence.Reset();
            }

        private:
            TIntrusivePtr<THullCtx> HullCtx;
            const TLevelIndexSnapshot &LevelSnap;
            TIntrusivePtr<TBarriersSnapshot::TBarriersEssence> BarriersEssence;
            const bool AllowGarbageCollection;
            TCalcStat* const CalcStat;

            struct TStat {
                ui32 SstsChecked = 0;
                bool BreakedActualRatio = false;
                bool BreakedTimeout = false;

                TString ToString() const {
                    auto bool2str = [] (bool v) { return v ? "true" : "false"; };
                    return Sprintf("{SstsChecked# %" PRIu32 " BreakedActualRatio# %s "
                                   "BreakedTimeout# %s}", SstsChecked, bool2str(BreakedActualRatio),
                                   bool2str(BreakedTimeout));
                }
            };

            struct TTimeSst {
                TInstant NextCalculationTime;
                TLevelSstPtr LevelSstPtr;

                TTimeSst(TInstant nextCalculationTime, const TLevelSstPtr &p)
                    : NextCalculationTime(nextCalculationTime)
                    , LevelSstPtr(p)
                {}

                bool operator < (const TTimeSst &s) const {
                    return NextCalculationTime < s.NextCalculationTime;
                }
            };

            static ui64 GetSstId(const TLevelSstPtr &p) {
                return p.SstPtr->AssignedSstId ? p.SstPtr->AssignedSstId : p.SstPtr->VolatileOrderId;
            }

            static TDuration GetInitialRecalculationAge(const TLevelSstPtr &p, TDuration calcPeriod) {
                const ui64 calcPeriodSeconds = calcPeriod.Seconds();
                if (!calcPeriodSeconds) {
                    return calcPeriod;
                }

                return TDuration::Seconds(IntHash(GetSstId(p)) % calcPeriodSeconds);
            }

            static TInstant GetInitialCalculationTime(const TLevelSstPtr &p, TInstant startTime, TDuration calcPeriod) {
                const TInstant createTime = p.SstPtr->Info.CTime;
                if (createTime != TInstant::Zero() && startTime < createTime + calcPeriod) {
                    return createTime;
                }

                return startTime - GetInitialRecalculationAge(p, calcPeriod);
            }

            static TInstant GetCalculationTime(const TLevelSstPtr &p, TInstant startTime, TDuration calcPeriod) {
                TInstant calculationTime = p.SstPtr->StorageRatio.GetCalculationTime();
                if (calculationTime != TInstant::Zero()) {
                    return calculationTime;
                }

                TSstRatioPtr ratio = p.SstPtr->StorageRatio.Get();
                const TInstant initialCalculationTime = GetInitialCalculationTime(p, startTime, calcPeriod);
                if (!ratio) {
                    p.SstPtr->StorageRatio.SetCalculationTime(initialCalculationTime);
                    return initialCalculationTime;
                }

                if (ratio->Time == TInstant::Zero()) {
                    return TInstant::Zero();
                }

                p.SstPtr->StorageRatio.SetCalculationTime(ratio->Time);
                return ratio->Time;
            }

            static TInstant GetNextCalculationTime(const TLevelSstPtr &p, TInstant startTime, TDuration calcPeriod) {
                return GetCalculationTime(p, startTime, calcPeriod) + calcPeriod;
            }

            void OrderSstByStorageRatioTime(TVector<TTimeSst> &vec, TInstant startTime, TDuration calcPeriod) {
                vec.clear();
                TSstIterator it(&LevelSnap.SliceSnap);
                it.SeekToFirst();
                while (it.Valid()) {
                    TLevelSstPtr p = it.Get();
                    vec.push_back(TTimeSst(GetNextCalculationTime(p, startTime, calcPeriod), p));
                    it.Next();
                }
                Sort(vec.begin(), vec.end());
            }

            static bool HaveOverlappingKeyRanges(
                    const TVector<TTimeSst>& orderedSsts,
                    size_t sstsToCalculate)
            {
                TVector<std::pair<TKey, TKey>> ranges;
                ranges.reserve(sstsToCalculate);
                for (size_t i = 0; i < sstsToCalculate; ++i) {
                    const TLevelSegmentPtr& sst = orderedSsts[i].LevelSstPtr.SstPtr;
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

            static size_t CountSstsToCalculate(
                    const TVector<TTimeSst>& orderedSsts,
                    TInstant startTime)
            {
                size_t sstsToCalculate = 0;
                while (sstsToCalculate < orderedSsts.size() &&
                        startTime >= orderedSsts[sstsToCalculate].NextCalculationTime) {
                    ++sstsToCalculate;
                }
                return sstsToCalculate;
            }

            template <bool CollectStats>
            void UpdateStorageRatioForOrderedSsts(
                    const TVector<TTimeSst>& orderedSsts,
                    size_t sstsToCalculate,
                    TInstant startTime,
                    TInstant deadline,
                    TStat& stat)
            {
                for (size_t i = 0; i < sstsToCalculate; ++i) {
                    const TTimeSst& x = orderedSsts[i];
                    TSstRatioPtr newRatio =
                        CalculateSstRatio<CollectStats>(x.LevelSstPtr.SstPtr, startTime);
                    x.LevelSstPtr.SstPtr->StorageRatio.Set(newRatio, newRatio->Time);
                    ++stat.SstsChecked;

                    // Preserve the old boundary: the timeout is checked after
                    // publishing each complete SST ratio.
                    if (TAppData::TimeProvider->Now() > deadline) {
                        stat.BreakedTimeout = true;
                        return;
                    }
                }

                if (sstsToCalculate < orderedSsts.size()) {
                    stat.BreakedActualRatio = true;
                }
            }

            template <bool CollectStats>
            void UpdateStorageRatioForDb(TInstant startTime, TStat &stat) {
                const TDuration &calcPeriod = HullCtx->HullCompStorageRatioCalcPeriod;
                const TDuration &calcDuration = HullCtx->HullCompStorageRatioMaxCalcDuration;

                // order all ssts (including level 0) by storage ratio calculation time
                TVector<TTimeSst> vec;
                vec.reserve(1000u);
                OrderSstByStorageRatioTime(vec, startTime, calcPeriod);

                // Keep the legacy path as a single pass: checking whether an
                // SST is due and calculating its ratio stay in the same loop.
                for (const auto &x : vec) {
                    if (startTime >= x.NextCalculationTime) {
                        TSstRatioPtr newRatio =
                            CalculateSstRatio<CollectStats>(x.LevelSstPtr.SstPtr, startTime);
                        x.LevelSstPtr.SstPtr->StorageRatio.Set(newRatio, newRatio->Time);
                        stat.SstsChecked++;
                    } else {
                        stat.BreakedActualRatio = true;
                        break;
                    }

                    // avoid spending too much time on storage ratio calculation
                    TInstant now = TAppData::TimeProvider->Now();
                    if (now > startTime + calcDuration) {
                        stat.BreakedTimeout = true;
                        break;
                    }
                }

                BarriersEssence.Reset();
            }

            template <bool CollectStats>
            void UpdateStorageRatioForDbBatch(TInstant startTime, TStat &stat) {
                const TDuration &calcPeriod = HullCtx->HullCompStorageRatioCalcPeriod;
                const TDuration &calcDuration = HullCtx->HullCompStorageRatioMaxCalcDuration;

                // order all ssts (including level 0) by storage ratio calculation time
                TVector<TTimeSst> vec;
                vec.reserve(1000u);
                OrderSstByStorageRatioTime(vec, startTime, calcPeriod);

                const size_t sstsToCalculate = CountSstsToCalculate(vec, startTime);

                // Without overlapping key ranges there is no whole-database
                // merge work to amortize between due SSTs.
                if (!HaveOverlappingKeyRanges(vec, sstsToCalculate)) {
                    UpdateStorageRatioForOrderedSsts<CollectStats>(
                        vec,
                        sstsToCalculate,
                        startTime,
                        startTime + calcDuration,
                        stat);
                    BarriersEssence.Reset();
                    return;
                }

                bool duplicateSst = false;
                bool timedOut = false;
                // Accumulator construction detects a duplicate SST before the
                // database scan starts. Such an invalid snapshot cannot be
                // attributed by pointer to separate accumulators.
                auto ratios = CalculateSstRatios<CollectStats>(
                    vec,
                    sstsToCalculate,
                    startTime,
                    startTime + calcDuration,
                    duplicateSst,
                    timedOut);

                if (duplicateSst) {
                    UpdateStorageRatioForOrderedSsts<CollectStats>(
                        vec,
                        sstsToCalculate,
                        startTime,
                        startTime + calcDuration,
                        stat);
                    BarriersEssence.Reset();
                    return;
                }

                for (auto& [sst, ratio] : ratios) {
                    sst->StorageRatio.Set(ratio, ratio->Time);
                }

                stat.SstsChecked = static_cast<ui32>(ratios.size());
                stat.BreakedTimeout = timedOut;
                stat.BreakedActualRatio = !timedOut && sstsToCalculate < vec.size();

                BarriersEssence.Reset();
            }

            struct TSourceRecord {
                const TLevelSegment* Sst;
                TMemRec MemRec;
                const TDiskPart* Outbound;
                ui32 NumKeepFlags;
                ui32 NumDoNotKeepFlags;
            };

            class TStorageRatioMerger {
                const TIngress::EMode IngressMode;
                TIndexRecordMerger DbMerger;
                TVector<TSourceRecord> Sources;

            public:
                explicit TStorageRatioMerger(const TBlobStorageGroupType& gtype)
                    : IngressMode(TIngress::IngressMode(gtype))
                    , DbMerger(gtype)
                {}

                void AddFromSegment(
                        const TMemRec& memRec,
                        const TDiskPart* outbound,
                        const TKey& key,
                        ui64 lsn,
                        const void* sst)
                {
                    DbMerger.AddFromSegment(memRec, outbound, key, lsn, sst);

                    ui32 numKeepFlags = 0;
                    ui32 numDoNotKeepFlags = 0;
                    if constexpr (std::is_same_v<TMemRec, TMemRecLogoBlob>) {
                        static_assert(CollectModeKeep == 1);
                        static_assert(CollectModeDoNotKeep == 2);

                        const int mode = memRec.GetIngress().GetCollectMode(IngressMode);
                        numKeepFlags = mode & CollectModeKeep;
                        numDoNotKeepFlags = (mode & CollectModeDoNotKeep) >> 1;
                    }

                    Sources.push_back({
                        static_cast<const TLevelSegment*>(sst),
                        memRec,
                        outbound,
                        numKeepFlags,
                        numDoNotKeepFlags,
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
                bool Collected = false;

                bool Complete() const {
                    return ProcessedItems == TotalItems;
                }
            };

            using TAccumulatorIndices = THashMap<const TLevelSegment*, size_t>;

            ui64 UpdateRatios(
                    const TKey& key,
                    const TStorageRatioMerger& merger,
                    const TAccumulatorIndices& accumulatorIndices,
                    TVector<TAccumulator>& accumulators,
                    TVector<size_t>& completedAccumulators)
            {
                const TIndexRecordMerger& dbMerger = merger.GetDbMerger();

                constexpr ui64 indexItemByteSize = sizeof(TKey) + sizeof(TMemRec);
                ui64 sourceRecordsProcessed = 0;

                for (const TSourceRecord& source : merger.GetSources()) {
                    auto it = accumulatorIndices.find(source.Sst);

                    // The merger also sees records from SSTs that were not
                    // selected for recalculation.
                    if (it == accumulatorIndices.end()) {
                        continue;
                    }

                    const size_t accumulatorIndex = it->second;
                    TAccumulator& accumulator = accumulators[accumulatorIndex];
                    TSstRatio& ratio = *accumulator.Ratio;
                    ++sourceRecordsProcessed;

                    TDiskDataExtractor extractor;
                    source.MemRec.GetDiskData(&extractor, source.Outbound);
                    const ui64 inplacedDataSize = extractor.GetInplacedDataSize();
                    const ui64 hugeDataSize = extractor.GetHugeDataSize();

                    // Account for the physical record stored in this SST.
                    ratio.IndexItemsTotal++;
                    ratio.IndexBytesTotal += indexItemByteSize;
                    ratio.InplacedDataTotal += inplacedDataSize;
                    ratio.HugeDataTotal += hugeDataSize;

                    const NGc::TKeepStatus keep = BarriersEssence->Keep(
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
                    if (accumulator.Complete()) {
                        completedAccumulators.push_back(accumulatorIndex);
                    }
                }

                return sourceRecordsProcessed;
            }

            template <bool CollectStats>
            TVector<std::pair<TLevelSegmentPtr, TSstRatioPtr>> CalculateSstRatios(
                    const TVector<TTimeSst>& orderedSsts,
                    size_t sstsToCalculate,
                    TInstant now,
                    TInstant deadline,
                    bool& duplicateSst,
                    bool& timedOut)
            {
                duplicateSst = false;

                TVector<TAccumulator> accumulatorStorage;
                accumulatorStorage.reserve(sstsToCalculate);

                TAccumulatorIndices accumulatorIndices;
                accumulatorIndices.reserve(sstsToCalculate);

                TVector<TMemIterator> dueIterators;
                dueIterators.reserve(sstsToCalculate);

                for (size_t i = 0; i < sstsToCalculate; ++i) {
                    const TLevelSegmentPtr& sst = orderedSsts[i].LevelSstPtr.SstPtr;
                    const ui64 totalItems = sst->Elements();

                    const size_t accumulatorIndex = accumulatorStorage.size();
                    const bool inserted = accumulatorIndices.emplace(
                        sst.Get(),
                        accumulatorIndex).second;
                    if (!inserted) {
                        duplicateSst = true;
                        return {};
                    }

                    accumulatorStorage.push_back({
                        .Sst = sst,
                        .Ratio = MakeIntrusive<TSstRatio>(now),
                        .ProcessedItems = 0,
                        .TotalItems = totalItems,
                        .Collected = false,
                    });

                    if (totalItems) {
                        dueIterators.emplace_back(sst.Get());
                    }
                }

                TVector<std::pair<TLevelSegmentPtr, TSstRatioPtr>> result;
                result.reserve(sstsToCalculate);

                auto collectCompleted = [&](size_t accumulatorIndex) {
                    TAccumulator& accumulator = accumulatorStorage[accumulatorIndex];
                    Y_ABORT_UNLESS(accumulator.Complete());
                    if (!accumulator.Collected) {
                        // Completion order follows the last key of an SST, not
                        // its recalculation time. A fully computed ratio is
                        // valid even when an earlier due SST is still partial.
                        accumulator.Collected = true;
                        result.emplace_back(accumulator.Sst, accumulator.Ratio);
                        if (!timedOut && TAppData::TimeProvider->Now() > deadline) {
                            timedOut = true;
                        }
                    }
                };

                for (size_t i = 0; i < accumulatorStorage.size(); ++i) {
                    if (accumulatorStorage[i].Complete()) {
                        collectCompleted(i);
                    }
                }

                if (!timedOut && result.size() < accumulatorStorage.size()) {
                    THeapIterator<TKey, TMemRec, true> dueIt;
                    for (TMemIterator& it : dueIterators) {
                        it.PutToHeap(dueIt);
                    }
                    dueIt.SeekToFirst();

                    TLevelIt dbIt(HullCtx, &LevelSnap);
                    TStorageRatioMerger merger(HullCtx->VCtx->Top->GType);
                    TVector<size_t> completedAccumulators;
                    completedAccumulators.reserve(sstsToCalculate);
                    bool dbItPositioned = false;

                    auto crashReport = [&](const TKey& expectedKey) {
                        TStringStream str;
                        str << MergeIteratorWithWholeDbDefaultCrashReport(
                            HullCtx->VCtx->VDiskLogPrefix,
                            dueIt,
                            dbIt);
                        str << " ExpectedKey: " << expectedKey.ToString() << "\n";
                        return str.Str();
                    };

                    constexpr ui32 skipBeforeSeek = 6;
                    auto positionDbIterator = [&](const TKey& key) {
                        if (!dbItPositioned) {
                            dbItPositioned = true;
                            if constexpr (CollectStats) {
                                ++CalcStat->Seeks;
                            }
                            dbIt.Seek(key);
                        } else {
                            ui32 seenItems = 0;
                            while (dbIt.Valid() && dbIt.GetCurKey() < key) {
                                ++seenItems;
                                if (seenItems < skipBeforeSeek) {
                                    dbIt.Next();
                                } else {
                                    if constexpr (CollectStats) {
                                        ++CalcStat->Seeks;
                                    }
                                    dbIt.Seek(key);
                                }
                            }
                        }

                        Y_ABORT_UNLESS(
                            dbIt.Valid(),
                            "%s",
                            crashReport(key).data());
                        Y_ABORT_UNLESS(
                            dbIt.GetCurKey() == key,
                            "%s",
                            crashReport(key).data());
                    };

                    while (dueIt.Valid() && !timedOut &&
                            result.size() < accumulatorStorage.size()) {
                        const TKey key = dueIt.GetCurKey();
                        positionDbIterator(key);

                        dbIt.PutToMerger(&merger);
                        merger.Finish();

                        completedAccumulators.clear();
                        const ui64 sourceRecordsProcessed = UpdateRatios(
                            key,
                            merger,
                            accumulatorIndices,
                            accumulatorStorage,
                            completedAccumulators);

                        if constexpr (CollectStats) {
                            ++CalcStat->KeysProcessed;
                            CalcStat->SourceRecordsProcessed += sourceRecordsProcessed;
                            CalcStat->DbRecordsMerged +=
                                merger.GetDbMerger().GetNumMergedRecords();
                        }

                        merger.Clear();
                        dueIt.Next();

                        for (size_t accumulatorIndex : completedAccumulators) {
                            collectCompleted(accumulatorIndex);
                        }
                    }
                }

                if (!timedOut) {
                    for (const TAccumulator& accumulator : accumulatorStorage) {
                        Y_ABORT_UNLESS(
                            accumulator.Complete(),
                            "StorageRatio calculation did not process all SST items");
                    }
                    Y_ABORT_UNLESS(result.size() == accumulatorStorage.size());
                }

                return result;
            }

            template <bool CollectStats>
            TSstRatioPtr CalculateSstRatio(TLevelSegmentPtr sst, TInstant now) {
                TSstRatioPtr r = MakeIntrusive<TSstRatio>(now);
                TSstRatio *ratio = r.Get();

                // the subset we processing
                TMemIterator subsIt(sst.Get());
                subsIt.SeekToFirst();
                // for the whole level index
                TLevelIt dbIt(HullCtx, &LevelSnap);

                auto newItem = [] (const TMemIterator &subsIt, const TIndexRecordMerger &subsMerger) {
                    Y_UNUSED(subsIt);
                    Y_UNUSED(subsMerger);
                };

                auto doMerge = [this, ratio] (const TMemIterator &subsIt,
                                              const TLevelIt &dbIt,
                                              const TIndexRecordMerger &subsMerger,
                                              const TIndexRecordMerger &dbMerger) {
                    Y_UNUSED(subsIt);
                    Y_UNUSED(subsMerger);

                    TDiskDataExtractor extr;
                    subsIt.GetDiskData(&extr);
                    // calculate item's parameters
                    const ui64 indexItemByteSize = sizeof(TKey) + sizeof(TMemRec);
                    const ui64 inplacedDataSize = extr.GetInplacedDataSize();
                    const ui64 hugeDataSize = extr.GetHugeDataSize();
                    // update ratio
                    ratio->IndexItemsTotal++;
                    ratio->IndexBytesTotal += indexItemByteSize;
                    ratio->InplacedDataTotal += inplacedDataSize;
                    ratio->HugeDataTotal += hugeDataSize;
                    // calculate keep status
                    bool allowKeepFlags = HullCtx->AllowKeepFlags;
                    NGc::TKeepStatus keep = BarriersEssence->Keep(dbIt.GetCurKey(), dbMerger.GetMemRec(),
                        {subsMerger.GetNumKeepFlags(), subsMerger.GetNumDoNotKeepFlags(), dbMerger.GetNumKeepFlags(),
                        dbMerger.GetNumDoNotKeepFlags()}, allowKeepFlags, AllowGarbageCollection);
                    if (keep.KeepIndex) {
                        // calculate index overhead
                        ratio->IndexItemsKeep++;
                        ratio->IndexBytesKeep += indexItemByteSize;
                    }
                    if (keep.KeepData) {
                        // calculate data overhead
                        ratio->InplacedDataKeep += inplacedDataSize;
                        ratio->HugeDataKeep += hugeDataSize;
                    }
                };

                auto crash = [ratio, this] (const TMemIterator &subsIt, const TLevelIt &dbIt) {
                    TStringStream str;
                    str << MergeIteratorWithWholeDbDefaultCrashReport(HullCtx->VCtx->VDiskLogPrefix,
                                                                      subsIt, dbIt);
                    str << " Ratio:   " << ratio->ToString() << "\n";
                    return str.Str();
                };

                if constexpr (CollectStats) {
                    MergeIteratorWithWholeDbWithStat<TMemIterator, TLevelIt, TIndexRecordMerger>(
                            HullCtx->VCtx->Top->GType,
                            subsIt,
                            dbIt,
                            newItem,
                            doMerge,
                            crash,
                            CalcStat->KeysProcessed,
                            CalcStat->SourceRecordsProcessed,
                            CalcStat->DbRecordsMerged,
                            CalcStat->Seeks);
                } else {
                    MergeIteratorWithWholeDb<TMemIterator, TLevelIt, TIndexRecordMerger>(
                            HullCtx->VCtx->Top->GType,
                            subsIt,
                            dbIt,
                            newItem,
                            doMerge,
                            crash);
                }
                return r;
            }
        };

    } // NHullComp
} // NKikimr
