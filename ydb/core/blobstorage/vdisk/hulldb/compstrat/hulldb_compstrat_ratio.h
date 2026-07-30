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
                // Whole-database keys put into a merger and finished.
                ui64 KeysProcessed = 0;
                // SST records attributed to ratio accumulators.
                ui64 SourceRecordsProcessed = 0;
                // Fresh and SST records put into whole-database mergers.
                ui64 DbRecordsMerged = 0;
                // Explicit Seek calls made by the calculation.
                ui64 Seeks = 0;
                // Next calls made while positioning the whole-database iterator.
                ui64 DbIteratorNexts = 0;
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
                , ExternalCalcStat(calcStat)
            {}


            void Work() {
                GetCalcStat() = {};

                const bool optimizationEnabled = HullCtx->VCfg->FeatureFlags
                    .GetEnableHullCompStorageRatioOptimization();
                const TInstant startTime(TAppData::TimeProvider->Now());
                TStat stat;
                bool fullBatchScheduled = false;
                if (optimizationEnabled) {
                    if (IsFullBatchCalculationDue(startTime)) {
                        fullBatchScheduled = true;
                        UpdateStorageRatioForDbFullBatch<true>(startTime, stat);
                    }
                } else {
                    HullCtx->StorageRatioFullBatchNextCalculationTime.reset();
                    UpdateStorageRatioForDb<true>(startTime, stat);
                }

                const TInstant finishTime(TAppData::TimeProvider->Now());
                if (fullBatchScheduled) {
                    HullCtx->StorageRatioFullBatchNextCalculationTime =
                        GetNextFullBatchCalculationTime(
                            finishTime,
                            HullCtx->HullCompStorageRatioCalcPeriod);
                }
                AccountMetrics(
                    optimizationEnabled,
                    stat,
                    finishTime - startTime);
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
            TCalcStat OwnedCalcStat;
            TCalcStat* const ExternalCalcStat;

            TCalcStat& GetCalcStat() {
                return ExternalCalcStat ? *ExternalCalcStat : OwnedCalcStat;
            }

            const TCalcStat& GetCalcStat() const {
                return ExternalCalcStat ? *ExternalCalcStat : OwnedCalcStat;
            }

            struct TStat {
                ui32 SstsChecked = 0;
                bool BreakedActualRatio = false;
                bool BreakedTimeout = false;
                bool UsedBatchAlgorithm = false;
                bool NonOverlappingFallback = false;
                bool DuplicateSstFallback = false;

                TString ToString() const {
                    auto bool2str = [] (bool v) { return v ? "true" : "false"; };
                    return Sprintf(
                        "{SstsChecked# %" PRIu32 " "
                        "BreakedActualRatio# %s BreakedTimeout# %s "
                        "UsedBatchAlgorithm# %s NonOverlappingFallback# %s "
                        "DuplicateSstFallback# %s}",
                        SstsChecked,
                        bool2str(BreakedActualRatio),
                        bool2str(BreakedTimeout),
                        bool2str(UsedBatchAlgorithm),
                        bool2str(NonOverlappingFallback),
                        bool2str(DuplicateSstFallback));
                }
            };

            void AccountMetrics(
                    bool optimizationEnabled,
                    const TStat& stat,
                    TDuration elapsed)
            {
                auto& totals = HullCtx->StorageRatioGroup;
                ++totals.StorageRatioInvocations();
                totals.StorageRatioTotalElapsedMicroseconds() += elapsed.MicroSeconds();

                if (stat.BreakedTimeout) {
                    ++totals.StorageRatioTimeouts();
                }
                if (stat.NonOverlappingFallback) {
                    ++totals.StorageRatioNonOverlappingFallbacks();
                }
                if (stat.DuplicateSstFallback) {
                    ++totals.StorageRatioDuplicateSstFallbacks();
                }
                const TCalcStat& calcStat = GetCalcStat();
                const bool hadCalculation =
                    stat.SstsChecked ||
                    calcStat.KeysProcessed ||
                    calcStat.SourceRecordsProcessed;
                if (!hadCalculation) {
                    ++totals.StorageRatioNoCalculationInvocations();
                    return;
                }

                if (optimizationEnabled) {
                    ++totals.StorageRatioFullRecalculations();
                }

                if (!optimizationEnabled) {
                    ++totals.StorageRatioFeatureDisabledCalculations();
                }

                auto& algorithm = stat.UsedBatchAlgorithm
                    ? HullCtx->StorageRatioBatchGroup
                    : HullCtx->StorageRatioLegacyGroup;
                ++algorithm.StorageRatioCalculations();
                algorithm.StorageRatioSstsCalculated() += stat.SstsChecked;
                algorithm.StorageRatioDbKeysMerged() += calcStat.KeysProcessed;
                algorithm.StorageRatioSourceRecordsProcessed() +=
                    calcStat.SourceRecordsProcessed;
                algorithm.StorageRatioDbRecordsMerged() += calcStat.DbRecordsMerged;
                algorithm.StorageRatioDbIteratorNexts() += calcStat.DbIteratorNexts;
                algorithm.StorageRatioSeeks() += calcStat.Seeks;
                algorithm.StorageRatioElapsedMicroseconds() += elapsed.MicroSeconds();
            }

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

            TInstant GetNextFullBatchCalculationTime(
                    TInstant now,
                    TDuration calcPeriod) const
            {
                const ui64 periodUs = calcPeriod.MicroSeconds();
                if (!periodUs) {
                    return now;
                }

                const ui64 vdiskHash = CombineHashes(
                    IntHash<ui64>(HullCtx->VCtx->GroupId.GetRawId()),
                    IntHash<ui64>(HullCtx->VCtx->ShortSelfVDisk.GetRaw()));
                const ui64 phaseUs = vdiskHash % periodUs;
                const ui64 nowUs = now.MicroSeconds();
                const ui64 cycleStartUs = nowUs - nowUs % periodUs;

                ui64 nextCalculationUs = cycleStartUs + phaseUs;
                if (nextCalculationUs <= nowUs) {
                    nextCalculationUs += periodUs;
                }

                return TInstant::MicroSeconds(nextCalculationUs);
            }

            bool IsFullBatchCalculationDue(TInstant now) {
                auto& nextCalculationTime =
                    HullCtx->StorageRatioFullBatchNextCalculationTime;
                if (!nextCalculationTime) {
                    nextCalculationTime = GetNextFullBatchCalculationTime(
                        now,
                        HullCtx->HullCompStorageRatioCalcPeriod);
                }
                return now >= *nextCalculationTime;
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

            template <bool CollectStats>
            void UpdateStorageRatioForSsts(
                    const TVector<TLevelSegmentPtr>& ssts,
                    TInstant startTime,
                    TStat& stat)
            {
                for (const TLevelSegmentPtr& sst : ssts) {
                    TSstRatioPtr newRatio =
                        CalculateSstRatio<CollectStats>(sst, startTime);
                    sst->StorageRatio.Set(newRatio, newRatio->Time);
                    ++stat.SstsChecked;
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
            void UpdateStorageRatioForDbFullBatch(
                    TInstant startTime,
                    TStat &stat)
            {
                TVector<TLevelSegmentPtr> ssts;
                ssts.reserve(1000u);
                TSstIterator it(&LevelSnap.SliceSnap);
                it.SeekToFirst();
                while (it.Valid()) {
                    ssts.push_back(it.Get().SstPtr);
                    it.Next();
                }

                // Without overlapping key ranges there is no whole-database
                // merge work to amortize between SSTs.
                if (!HaveOverlappingKeyRanges(ssts)) {
                    stat.NonOverlappingFallback = ssts.size() > 1;
                    UpdateStorageRatioForSsts<CollectStats>(
                        ssts,
                        startTime,
                        stat);
                    BarriersEssence.Reset();
                    return;
                }

                bool duplicateSst = false;
                // Accumulator construction detects a duplicate SST before the
                // database scan starts. Such an invalid snapshot cannot be
                // attributed by pointer to separate accumulators.
                auto ratios = CalculateSstRatios<CollectStats>(
                    ssts,
                    startTime,
                    duplicateSst);

                if (duplicateSst) {
                    stat.DuplicateSstFallback = true;
                    UpdateStorageRatioForSsts<CollectStats>(
                        ssts,
                        startTime,
                        stat);
                    BarriersEssence.Reset();
                    return;
                }

                stat.UsedBatchAlgorithm = true;
                for (auto& [sst, ratio] : ratios) {
                    sst->StorageRatio.Set(ratio, ratio->Time);
                }

                stat.SstsChecked = static_cast<ui32>(ratios.size());

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

                    // A record without an accumulator still contributes to
                    // the merged database value, but not to an SST ratio.
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
                    const TVector<TLevelSegmentPtr>& ssts,
                    TInstant now,
                    bool& duplicateSst)
            {
                duplicateSst = false;

                TVector<TAccumulator> accumulatorStorage;
                accumulatorStorage.reserve(ssts.size());

                TAccumulatorIndices accumulatorIndices;
                accumulatorIndices.reserve(ssts.size());

                TVector<TMemIterator> sstIterators;
                sstIterators.reserve(ssts.size());

                for (const TLevelSegmentPtr& sst : ssts) {
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
                        sstIterators.emplace_back(sst.Get());
                    }
                }

                TVector<std::pair<TLevelSegmentPtr, TSstRatioPtr>> result;
                result.reserve(ssts.size());

                auto collectCompleted = [&](size_t accumulatorIndex) {
                    TAccumulator& accumulator = accumulatorStorage[accumulatorIndex];
                    Y_ABORT_UNLESS(accumulator.Complete());
                    if (!accumulator.Collected) {
                        // Completion order follows the last key of an SST, not
                        // the slice order. A fully computed ratio is valid even
                        // when another SST is still partial.
                        accumulator.Collected = true;
                        result.emplace_back(accumulator.Sst, accumulator.Ratio);
                    }
                };

                for (size_t i = 0; i < accumulatorStorage.size(); ++i) {
                    if (accumulatorStorage[i].Complete()) {
                        collectCompleted(i);
                    }
                }

                if (result.size() < accumulatorStorage.size()) {
                    THeapIterator<TKey, TMemRec, true> sstIt;
                    for (TMemIterator& it : sstIterators) {
                        it.PutToHeap(sstIt);
                    }
                    sstIt.SeekToFirst();

                    TLevelIt dbIt(HullCtx, &LevelSnap);
                    TStorageRatioMerger merger(HullCtx->VCtx->Top->GType);
                    TVector<size_t> completedAccumulators;
                    completedAccumulators.reserve(ssts.size());
                    bool dbItPositioned = false;

                    auto crashReport = [&](const TKey& expectedKey) {
                        TStringStream str;
                        str << MergeIteratorWithWholeDbDefaultCrashReport(
                            HullCtx->VCtx->VDiskLogPrefix,
                            sstIt,
                            dbIt);
                        str << " ExpectedKey: " << expectedKey.ToString() << "\n";
                        return str.Str();
                    };

                    constexpr ui32 skipBeforeSeek = 6;
                    auto positionDbIterator = [&](const TKey& key) {
                        if (!dbItPositioned) {
                            dbItPositioned = true;
                            if constexpr (CollectStats) {
                                ++GetCalcStat().Seeks;
                            }
                            dbIt.Seek(key);
                        } else {
                            ui32 seenItems = 0;
                            while (dbIt.Valid() && dbIt.GetCurKey() < key) {
                                ++seenItems;
                                if (seenItems < skipBeforeSeek) {
                                    if constexpr (CollectStats) {
                                        ++GetCalcStat().DbIteratorNexts;
                                    }
                                    dbIt.Next();
                                } else {
                                    if constexpr (CollectStats) {
                                        ++GetCalcStat().Seeks;
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

                    while (sstIt.Valid() &&
                            result.size() < accumulatorStorage.size()) {
                        const TKey key = sstIt.GetCurKey();
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
                            ++GetCalcStat().KeysProcessed;
                            GetCalcStat().SourceRecordsProcessed += sourceRecordsProcessed;
                            GetCalcStat().DbRecordsMerged +=
                                merger.GetDbMerger().GetNumMergedRecords();
                        }

                        merger.Clear();
                        sstIt.Next();

                        for (size_t accumulatorIndex : completedAccumulators) {
                            collectCompleted(accumulatorIndex);
                        }
                    }
                }

                for (const TAccumulator& accumulator : accumulatorStorage) {
                    Y_ABORT_UNLESS(
                        accumulator.Complete(),
                        "StorageRatio calculation did not process all SST items");
                }
                Y_ABORT_UNLESS(result.size() == accumulatorStorage.size());

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
                            GetCalcStat().KeysProcessed,
                            GetCalcStat().SourceRecordsProcessed,
                            GetCalcStat().DbRecordsMerged,
                            GetCalcStat().Seeks,
                            GetCalcStat().DbIteratorNexts);
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
