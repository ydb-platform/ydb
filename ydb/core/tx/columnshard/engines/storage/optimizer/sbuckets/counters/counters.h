#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>
#include <ydb/core/tx/columnshard/counters/common/agent.h>
#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

namespace NKikimr::NOlap::NStorageOptimizer::NSBuckets {

class TPortionCategoryCounterAgents: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
public:
    const std::shared_ptr<NColumnShard::TValueAggregationAgent> RecordsCount;
    const std::shared_ptr<NColumnShard::TValueAggregationAgent> Count;
    const std::shared_ptr<NColumnShard::TValueAggregationAgent> Bytes;
    TPortionCategoryCounterAgents(NColumnShard::TCommonCountersOwner& base, const TString& categoryName)
        : TBase(base, "category", categoryName)
        , RecordsCount(TBase::GetValueAutoAggregations("ByGranule/Portions/RecordsCount"))
        , Count(TBase::GetValueAutoAggregations("ByGranule/Portions/Count"))
        , Bytes(TBase::GetValueAutoAggregations("ByGranule/Portions/Bytes"))
    {
    }
};

class TPortionCategoryCounters {
private:
    std::shared_ptr<NColumnShard::TValueAggregationClient> RecordsCount;
    std::shared_ptr<NColumnShard::TValueAggregationClient> Count;
    std::shared_ptr<NColumnShard::TValueAggregationClient> Bytes;
public:
    TPortionCategoryCounters(TPortionCategoryCounterAgents& agents)
    {
        RecordsCount = agents.RecordsCount->GetClient();
        Count = agents.Count->GetClient();
        Bytes = agents.Bytes->GetClient();
    }

    void AddPortion(const std::shared_ptr<TPortionInfo>& p) {
        RecordsCount->Add(p->NumRows());
        Count->Add(1);
        Bytes->Add(p->GetTotalBlobBytes());
    }

    void RemovePortion(const std::shared_ptr<TPortionInfo>& p) {
        RecordsCount->Remove(p->NumRows());
        Count->Remove(1);
        Bytes->Remove(p->GetTotalBlobBytes());
    }
};

class TGlobalCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    NMonitoring::THistogramPtr HistogramDiffSnapshots;

    std::shared_ptr<TPortionCategoryCounterAgents> PortionsForMerge;
    std::shared_ptr<TPortionCategoryCounterAgents> PortionsAlone;
    std::shared_ptr<TPortionCategoryCounterAgents> SmallPortions;
    std::shared_ptr<TPortionCategoryCounterAgents> ActualPortions;
    std::shared_ptr<TPortionCategoryCounterAgents> FuturePortions;

    std::shared_ptr<NColumnShard::TValueAggregationAgent> OldestCriticalActuality;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> MergeCoefficient;
public:
    NMonitoring::TDynamicCounters::TCounterPtr FinalBucketTaskCounter;
    NMonitoring::TDynamicCounters::TCounterPtr MiddleBucketTaskCounter;
    NMonitoring::THistogramPtr HistogramFinalBucketTask;
    NMonitoring::THistogramPtr HistogramMiddleBucketTask;
    NMonitoring::THistogramPtr HistogramFinalBucketTaskSnapshotsDiff;
    NMonitoring::THistogramPtr HistogramMiddleBucketTaskSnapshotsDiff;
    std::shared_ptr<NColumnShard::TValueAggregationAgent> BucketsForMerge;
    NMonitoring::TDynamicCounters::TCounterPtr OptimizersCount;

    TGlobalCounters()
        : TBase("SBucketsOptimizer")
    {
        PortionsForMerge = std::make_shared<TPortionCategoryCounterAgents>(*this, "for_merge");
        PortionsAlone = std::make_shared<TPortionCategoryCounterAgents>(*this, "alone");
        SmallPortions = std::make_shared<TPortionCategoryCounterAgents>(*this, "small");
        ActualPortions = std::make_shared<TPortionCategoryCounterAgents>(*this, "actual");
        FuturePortions = std::make_shared<TPortionCategoryCounterAgents>(*this, "future");

        FinalBucketTaskCounter = TBase::GetDeriviative("FinalBucketTasks/Count");
        MiddleBucketTaskCounter = TBase::GetDeriviative("MiddleBucketTasks/Count");

        HistogramFinalBucketTask = TBase::GetHistogram("FinalBucketTasks/Count", NMonitoring::ExponentialHistogram(15, 2, 1));
        HistogramMiddleBucketTask = TBase::GetHistogram("MiddleBucketTasks/Count", NMonitoring::ExponentialHistogram(15, 2, 1));
        HistogramFinalBucketTaskSnapshotsDiff = TBase::GetHistogram("FinalBucketTasksDiff/Count", NMonitoring::ExponentialHistogram(15, 2, 1));
        HistogramMiddleBucketTaskSnapshotsDiff = TBase::GetHistogram("MiddleBucketTasksDiff/Count", NMonitoring::ExponentialHistogram(15, 2, 1));

        HistogramDiffSnapshots = TBase::GetHistogram("DiffSnapshots", NMonitoring::ExponentialHistogram(15, 2, 1000));
        OldestCriticalActuality = TBase::GetValueAutoAggregations("Granule/ActualityMs");
        MergeCoefficient = TBase::GetValueAutoAggregations("Granule/MergeCoefficient");
        BucketsForMerge = TBase::GetValueAutoAggregations("BucketsForMerge/Count");
        OptimizersCount = TBase::GetValue("Optimizers/Count");
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildOldestCriticalActualityAggregation() {
        return Singleton<TGlobalCounters>()->OldestCriticalActuality->GetClient();
    }

    static std::shared_ptr<NColumnShard::TValueAggregationClient> BuildMergeCoefficientAggregation() {
        return Singleton<TGlobalCounters>()->MergeCoefficient->GetClient();
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildPortionsForMergeAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->PortionsForMerge);
    }

    static NMonitoring::THistogramPtr BuildHistogramDiffSnapshots() {
        return Singleton<TGlobalCounters>()->HistogramDiffSnapshots;
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildSmallPortionsAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->SmallPortions);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildPortionsAloneAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->PortionsAlone);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildActualPortionsAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->ActualPortions);
    }

    static std::shared_ptr<TPortionCategoryCounters> BuildFuturePortionsAggregation() {
        return std::make_shared<TPortionCategoryCounters>(*Singleton<TGlobalCounters>()->FuturePortions);
    }

};

class TCounters {
private:
    std::shared_ptr<NColumnShard::TValueAggregationClient> OldestCriticalActuality;
public:
    const std::shared_ptr<TPortionCategoryCounters> PortionsForMerge;
    const std::shared_ptr<TPortionCategoryCounters> PortionsAlone;
    const std::shared_ptr<TPortionCategoryCounters> SmallPortions;
    const std::shared_ptr<TPortionCategoryCounters> ActualPortions;
    const std::shared_ptr<TPortionCategoryCounters> FuturePortions;
    const std::shared_ptr<NColumnShard::TValueAggregationClient> MergeCoefficient;
    const NMonitoring::THistogramPtr HistogramDiffSnapshots;
    const std::shared_ptr<NColumnShard::TValueAggregationClient> BucketsForMerge;
    const std::shared_ptr<NColumnShard::TValueGuard> OptimizersCount;

    void OnNewTask(const bool isFinalBucket, const TInstant youngestSnapshot, const TInstant oldestSnapshot) {
        if (isFinalBucket) {
            Singleton<TGlobalCounters>()->FinalBucketTaskCounter->Add(1);
            Singleton<TGlobalCounters>()->HistogramFinalBucketTask->Collect((Now() - youngestSnapshot).MilliSeconds() * 0.001, 1);
            Singleton<TGlobalCounters>()->HistogramFinalBucketTaskSnapshotsDiff->Collect((youngestSnapshot - oldestSnapshot).MilliSeconds() * 0.001, 1);
        } else {
            Singleton<TGlobalCounters>()->MiddleBucketTaskCounter->Add(1);
            Singleton<TGlobalCounters>()->HistogramMiddleBucketTask->Collect((Now() - youngestSnapshot).MilliSeconds() * 0.001, 1);
            Singleton<TGlobalCounters>()->HistogramMiddleBucketTaskSnapshotsDiff->Collect((youngestSnapshot - oldestSnapshot).MilliSeconds() * 0.001, 1);
        }
    }

    TCounters()
        : PortionsForMerge(TGlobalCounters::BuildPortionsForMergeAggregation())
        , PortionsAlone(TGlobalCounters::BuildPortionsAloneAggregation())
        , SmallPortions(TGlobalCounters::BuildSmallPortionsAggregation())
        , ActualPortions(TGlobalCounters::BuildActualPortionsAggregation())
        , FuturePortions(TGlobalCounters::BuildFuturePortionsAggregation())
        , MergeCoefficient(TGlobalCounters::BuildMergeCoefficientAggregation())
        , HistogramDiffSnapshots(TGlobalCounters::BuildHistogramDiffSnapshots())
        , BucketsForMerge(Singleton<TGlobalCounters>()->BucketsForMerge->GetClient())
        , OptimizersCount(std::make_shared<NColumnShard::TValueGuard>(Singleton<TGlobalCounters>()->OptimizersCount))
    {
        OldestCriticalActuality = TGlobalCounters::BuildOldestCriticalActualityAggregation();
    }

    void OnMinProblemSnapshot(const TDuration d) {
        OldestCriticalActuality->SetValue(d.MilliSeconds(), TInstant::Now() + TDuration::Seconds(10));
    }

};

}
