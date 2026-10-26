#pragma once
#include <ydb/core/tx/columnshard/counters/histogram_borders.h>
#include <ydb/core/tx/columnshard/counters/portions.h>

#include <ydb/library/actors/core/log.h>
#include <ydb/library/signals/owner.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/system/mutex.h>

namespace NKikimr::NOlap::NChanges {

/// The counters of one compaction task reason, under the subgroup reason=<name> of GeneralCompaction: accumulator, level,
/// snapshot_merge, snapshot_prefix and snapshot_move for tiling-s, generic for planners that give none. Rates as
/// derivative counters, distributions as histograms with exponential borders, so that the monitoring system can take
/// percentiles (histogram_percentile over Histogram/Tasks/...).
class TCompactionTaskReasonCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    const NMonitoring::TDynamicCounters::TCounterPtr TasksStarted;
    const NMonitoring::TDynamicCounters::TCounterPtr TasksFinished;
    const NMonitoring::TDynamicCounters::TCounterPtr TasksFailed;
    const NMonitoring::TDynamicCounters::TCounterPtr PortionsIn;
    const NMonitoring::TDynamicCounters::TCounterPtr BytesIn;
    const NMonitoring::TDynamicCounters::TCounterPtr PortionsOut;
    const NMonitoring::TDynamicCounters::TCounterPtr BytesOut;
    const NMonitoring::THistogramPtr HistogramPortionsIn;
    const NMonitoring::THistogramPtr HistogramBytesIn;
    const NMonitoring::THistogramPtr HistogramPortionsOut;
    const NMonitoring::THistogramPtr HistogramBytesOut;
    const NMonitoring::THistogramPtr HistogramDurationMs;
    const NMonitoring::THistogramPtr HistogramWaitMs;

public:
    TCompactionTaskReasonCounters(const NColumnShard::TCommonCountersOwner& owner)
        : TBase(owner)
        , TasksStarted(TBase::GetDeriviative("Tasks/Started/Count"))
        , TasksFinished(TBase::GetDeriviative("Tasks/Finished/Count"))
        , TasksFailed(TBase::GetDeriviative("Tasks/Failed/Count"))
        , PortionsIn(TBase::GetDeriviative("Tasks/PortionsIn/Count"))
        , BytesIn(TBase::GetDeriviative("Tasks/BytesIn/Bytes"))
        , PortionsOut(TBase::GetDeriviative("Tasks/PortionsOut/Count"))
        , BytesOut(TBase::GetDeriviative("Tasks/BytesOut/Bytes"))
        , HistogramPortionsIn(TBase::GetHistogram("Tasks/PortionsIn", NMonitoring::ExponentialHistogram(15, 2)))
        , HistogramBytesIn(TBase::GetHistogram("Tasks/BytesIn", NMonitoring::ExponentialHistogram(25, 2, 1024)))
        , HistogramPortionsOut(TBase::GetHistogram("Tasks/PortionsOut", NMonitoring::ExponentialHistogram(15, 2)))
        , HistogramBytesOut(TBase::GetHistogram("Tasks/BytesOut", NMonitoring::ExponentialHistogram(25, 2, 1024)))
        , HistogramDurationMs(TBase::GetHistogram("Tasks/DurationMs", NMonitoring::ExponentialHistogram(25, 2)))
        , HistogramWaitMs(TBase::GetHistogram("Tasks/WaitMs", NMonitoring::ExponentialHistogram(25, 2)))
    {
    }

    /// The task starts: its inputs, and the wait between its generation by the planner and its start.
    void OnStart(const ui64 portionsIn, const ui64 bytesIn, const TDuration wait) const {
        TasksStarted->Inc();
        PortionsIn->Add(portionsIn);
        BytesIn->Add(bytesIn);
        HistogramPortionsIn->Collect(portionsIn);
        HistogramBytesIn->Collect(bytesIn);
        HistogramWaitMs->Collect(wait.MilliSeconds());
    }

    /// The task's outputs reached the index.
    void OnWritten(const ui64 portionsOut, const ui64 bytesOut) const {
        PortionsOut->Add(portionsOut);
        BytesOut->Add(bytesOut);
        HistogramPortionsOut->Collect(portionsOut);
        HistogramBytesOut->Collect(bytesOut);
    }

    /// The task ended, in success or in failure, after this long since its start.
    void OnFinish(const bool success, const TDuration duration) const {
        (success ? TasksFinished : TasksFailed)->Inc();
        HistogramDurationMs->Collect(duration.MilliSeconds());
    }

    i64 GetTasksStarted() const {
        return TasksStarted->Val();
    }

    i64 GetTasksFinished() const {
        return TasksFinished->Val();
    }
};

class TGeneralCompactionCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;
    TPortionGroupCounters RepackPortions;
    TPortionGroupCounters RepackInsertedPortions;
    TPortionGroupCounters RepackCompactedPortions;
    THashMap<ui32, TPortionGroupCounters> RepackPortionsFromLevel;
    THashMap<ui32, TPortionGroupCounters> RepackPortionsToLevel;
    THashMap<ui32, TPortionGroupCounters> MovePortionsFromLevel;
    THashMap<ui32, TPortionGroupCounters> MovePortionsToLevel;
    NMonitoring::THistogramPtr HistogramRepackPortionsRawBytes;
    NMonitoring::THistogramPtr HistogramRepackPortionsBlobBytes;
    NMonitoring::THistogramPtr HistogramRepackPortionsCount;
    NMonitoring::THistogramPtr HistogramRepackPortionsRows;
    NMonitoring::THistogramPtr HistogramBlobsWrittenCount;
    NMonitoring::THistogramPtr HistogramBlobsWrittenBytes;
    NMonitoring::THistogramPtr HistogramCompactionDuration;
    NMonitoring::THistogramPtr HistogramTaskGenerationDuration;
    NMonitoring::THistogramPtr HistogramTaskGenerationCount;
    TMutex ReasonsMutex;
    THashMap<TString, std::shared_ptr<TCompactionTaskReasonCounters>> ByReason;

public:
    TGeneralCompactionCounters()
        : TBase("GeneralCompaction")
        , RepackPortions("ALL", CreateSubGroup("action", "repack"))
        , RepackInsertedPortions("INSERTED", CreateSubGroup("action", "repack"))
        , RepackCompactedPortions("COMPACTED", CreateSubGroup("action", "repack"))
    {
        for (ui32 i = 0; i < 10; ++i) {
            RepackPortionsFromLevel.emplace(
                i, TPortionGroupCounters("level=" + ::ToString(i), CreateSubGroup("action", "repack").CreateSubGroup("direction", "from")));
            RepackPortionsToLevel.emplace(
                i, TPortionGroupCounters("level=" + ::ToString(i), CreateSubGroup("action", "repack").CreateSubGroup("direction", "to")));
            MovePortionsFromLevel.emplace(
                i, TPortionGroupCounters("level=" + ::ToString(i), CreateSubGroup("action", "move").CreateSubGroup("direction", "from")));
            MovePortionsToLevel.emplace(
                i, TPortionGroupCounters("level=" + ::ToString(i), CreateSubGroup("action", "move").CreateSubGroup("direction", "to")));
        }
        // Histograms with exponential borders, so that the monitoring system can take percentiles; the bucket counters
        // these replace could not.
        HistogramRepackPortionsRawBytes = TBase::GetHistogram("RepackPortions/Raw/Bytes", NMonitoring::ExponentialHistogram(25, 2, 1024));
        HistogramRepackPortionsBlobBytes = TBase::GetHistogram("RepackPortions/Blob/Bytes", NMonitoring::ExponentialHistogram(25, 2, 1024));
        HistogramRepackPortionsCount = TBase::GetHistogram("RepackPortions/Count", NMonitoring::ExponentialHistogram(15, 2));
        HistogramRepackPortionsRows = TBase::GetHistogram("RepackPortions/Rows", NMonitoring::ExponentialHistogram(25, 2));
        HistogramBlobsWrittenCount = TBase::GetHistogram("BlobsWritten/Count", NMonitoring::ExponentialHistogram(15, 2));
        HistogramBlobsWrittenBytes = TBase::GetHistogram("BlobsWritten/Bytes", NMonitoring::ExponentialHistogram(25, 2, 1024));
        HistogramCompactionDuration = TBase::GetHistogram("Compaction/DurationMs", NMonitoring::ExponentialHistogram(25, 2));
        HistogramTaskGenerationDuration = TBase::GetHistogram("TaskGeneration/DurationUs", NMonitoring::ExponentialHistogram(25, 2));
        HistogramTaskGenerationCount = TBase::GetHistogram("TaskGeneration/Count", NMonitoring::LinearHistogram(20, 0, 1));
    }

    /// The counters of a task reason, created on first use; the set of reasons is the planners' to define.
    static const TCompactionTaskReasonCounters& ForReason(const TString& reason) {
        auto* self = Singleton<TGeneralCompactionCounters>();
        TGuard<TMutex> guard(self->ReasonsMutex);
        auto it = self->ByReason.find(reason);
        if (it == self->ByReason.end()) {
            it = self->ByReason.emplace(reason, std::make_shared<TCompactionTaskReasonCounters>(self->CreateSubGroup("reason", reason))).first;
        }
        return *it->second;
    }

    static void OnRepackPortions(const TSimplePortionsGroupInfo& portions) {
        Singleton<TGeneralCompactionCounters>()->RepackPortions.OnData(portions);
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsCount->Collect(portions.GetCount());
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsBlobBytes->Collect(portions.GetBlobBytes());
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsRawBytes->Collect(portions.GetRawBytes());
        Singleton<TGeneralCompactionCounters>()->HistogramRepackPortionsRows->Collect(portions.GetRecordsCount());
    }

    static void OnRepackPortionsByLevel(const THashMap<ui32, TSimplePortionsGroupInfo>& portions, const ui32 targetLevelIdx) {
        for (auto&& i : portions) {
            auto& counters = (i.first == targetLevelIdx) ? Singleton<TGeneralCompactionCounters>()->RepackPortionsToLevel
                                                         : Singleton<TGeneralCompactionCounters>()->RepackPortionsFromLevel;
            auto it = counters.find(i.first);
            AFL_VERIFY(it != counters.end());
            it->second.OnData(i.second);
        }
    }

    static void OnMovePortionsByLevel(const THashMap<ui32, TSimplePortionsGroupInfo>& portions, const ui32 targetLevelIdx) {
        for (auto&& i : portions) {
            auto& counters = (i.first == targetLevelIdx) ? Singleton<TGeneralCompactionCounters>()->MovePortionsToLevel
                                                         : Singleton<TGeneralCompactionCounters>()->MovePortionsFromLevel;
            auto it = counters.find(i.first);
            AFL_VERIFY(it != counters.end());
            it->second.OnData(i.second);
        }
    }

    static void OnRepackInsertedPortions(const TSimplePortionsGroupInfo& portions) {
        Singleton<TGeneralCompactionCounters>()->RepackInsertedPortions.OnData(portions);
    }

    static void OnRepackCompactedPortions(const TSimplePortionsGroupInfo& portions) {
        Singleton<TGeneralCompactionCounters>()->RepackCompactedPortions.OnData(portions);
    }

    static void OnCompactionWriteIndexCompleted(const ui64 blobsWritten, const ui64 bytesWritten) {
        Singleton<TGeneralCompactionCounters>()->HistogramBlobsWrittenCount->Collect(blobsWritten);
        Singleton<TGeneralCompactionCounters>()->HistogramBlobsWrittenBytes->Collect(bytesWritten);
    }

    static void OnCompactionFinished(const ui64 durationMicroSeconds) {
        Singleton<TGeneralCompactionCounters>()->HistogramCompactionDuration->Collect(durationMicroSeconds / 1000);
    }

    static void OnTasksGeneratred(const ui64 durationMicroSeconds, const ui64 count) {
        Singleton<TGeneralCompactionCounters>()->HistogramTaskGenerationDuration->Collect(durationMicroSeconds);
        Singleton<TGeneralCompactionCounters>()->HistogramTaskGenerationCount->Collect(count);
    }
};

}   // namespace NKikimr::NOlap::NChanges
