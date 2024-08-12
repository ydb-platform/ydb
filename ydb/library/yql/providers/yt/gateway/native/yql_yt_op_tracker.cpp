#include "yql_yt_op_tracker.h"

#include <ydb/library/yql/providers/common/provider/yql_provider_names.h>

#include <yt/cpp/mapreduce/interface/operation.h>
#include <yt/cpp/mapreduce/interface/job_statistics.h>

#include <util/datetime/base.h>
#include <util/system/guard.h>
#include <util/system/yassert.h>

namespace NYql {

namespace NNative {

using namespace NThreading;

// see https://yt.yandex-team.ru/docs/problems/jobstatistics.html for full list
const static TStringBuf YT_STATISTICS[] = {
    "job_proxy/cpu/system",
    "job_proxy/cpu/user",
    "data/input/chunk_count",
    "data/input/row_count",
    "data/input/data_weight",
    "time/exec",
    "time/total",
    "time/prepare",
    "time/artifact_download",
    "user_job/cpu/user",
    "user_job/cpu/system",
    "user_job/max_memory",
    "user_job/woodpecker",
};

const static TStringBuf CUSTOM_STATISTICS[] = {
    "CodeGen_CompileTime",
    "CodeGen_GenerateTime",
    "CodeGen_FullTime",
    "Combine_FlushesCount",
    "Combine_MaxRowsCount",
    "Job_ElapsedTime",
    "Job_InputBytes",
    "Job_InputDecodeTime",
    "Job_InputReadTime",
    "Job_OutputBytes",
    "Job_OutputEncodeTime",
    "Job_OutputWriteTime",
    "Job_SystemTime",
    "Job_UserTime",
    "Job_InitTime",
    "Job_CalcTime",
    "Join_Spill_Count",
    "Join_Spill_MaxFileSize",
    "Join_Spill_MaxRowsCount",
    "PagePool_AllocCount",
    "PagePool_PageAllocCount",
    "PagePool_PageHitCount",
    "PagePool_PageMissCount",
    "PagePool_PeakAllocated",
    "PagePool_PeakUsed",
    "Switch_FlushesCount",
    "Switch_MaxRowsCount",
    "Udf_AppliesCount",
};

TOperationTracker::TOperationTracker()
    : Thread_(TThread::TParams{Tracker, (void*)this}.SetName("yt_op_tracker"))
{
    Running_ = true;
    Thread_.Start();
}

TOperationTracker::~TOperationTracker() {
    Y_ABORT_UNLESS(!Thread_.Running());
}

void TOperationTracker::Stop() {
    if (Running_) {
        Running_ = false;
        Thread_.Join();
    }
}

TFuture<void> TOperationTracker::MakeOperationWaiter(const NYT::IOperationPtr& operation, TMaybe<ui32> publicId,
    const TString& ytServer, const TString& ytClusterName, const TOperationProgressWriter& progressWriter, const TStatWriter& statWriter)
{
    auto future = operation->GetStartedFuture().Apply([operation](const auto& f) {
        f.GetValue();
        return operation->Watch();
    });
    if (!publicId) {
        return future;
    }

    TOperationProgress progress(TString(YtProviderName), *publicId,
        TOperationProgress::EState::InProgress);

    auto filter = NYT::TOperationAttributeFilter();
    filter.Add(NYT::EOperationAttribute::State);

    auto checker = [future, operation, ytServer, progress, progressWriter, filter, ytClusterName] () mutable {
        bool done = future.Wait(TDuration::Zero());

        if (!done) {
            TString stage;
            bool writeProgress = true;
            if (operation->IsStarted()) {
                if (!progress.RemoteId) {
                    progress.RemoteId = ytServer + "/" + GetGuidAsString(operation->GetId());
                }
                progress.RemoteData["cluster_name"] = ytClusterName;
                if (auto briefProgress = operation->GetBriefProgress()) {
                    progress.Counters.ConstructInPlace();
                    progress.Counters->Completed = briefProgress->Completed;
                    progress.Counters->Running = briefProgress->Running;
                    progress.Counters->Total = briefProgress->Total;
                    progress.Counters->Aborted = briefProgress->Aborted;
                    progress.Counters->Failed = briefProgress->Failed;
                    progress.Counters->Lost = briefProgress->Lost;
                    progress.Counters->Pending = briefProgress->Pending;
                    stage = "Running";
                } else {
                    auto state = operation->GetAttributes(NYT::TGetOperationOptions().AttributeFilter(filter)).State;
                    if (state) {
                        stage = *state;
                        stage.to_upper(0, 1);
                    }
                }
            } else {
                // Not started yet
                writeProgress = false;
                stage = operation->GetStatus();
            }
            if (!stage.empty() && stage != progress.Stage.first) {
                progress.Stage = TOperationProgress::TStage(stage, TInstant::Now());
                writeProgress = true;
            }
            if (writeProgress) {
                progressWriter(progress);
            }
        }
        return !done;
    };

    with_lock(Mutex_) {
        // TODO: limit number of running operations
        RunningOperations_.push_back(checker);
    }

    // Make a final progress write
    return future.Apply([operation, progress, progressWriter, statWriter, ytServer, ytClusterName] (const TFuture<void>& f) mutable {
        f.GetValue();
        if (auto briefProgress = operation->GetBriefProgress()) {
            progress.Counters.ConstructInPlace();
            progress.Counters->Completed = briefProgress->Completed;
            progress.Counters->Running = briefProgress->Running;
            progress.Counters->Total = briefProgress->Total;
            progress.Counters->Aborted = briefProgress->Aborted;
            progress.Counters->Failed = briefProgress->Failed;
            progress.Counters->Lost = briefProgress->Lost;
            progress.Counters->Pending = briefProgress->Pending;
        }
        auto operationStatistic = operation->GetJobStatistics();

        TVector<TOperationStatistics::TEntry> statEntries;
        for (auto statName : YT_STATISTICS) {
            if (operationStatistic.HasStatistics(statName)) {
                auto st = operationStatistic.GetStatistics(statName);
                statEntries.emplace_back(TString{statName}, st.Sum(), st.Max(), st.Min(), st.Avg(), st.Count());
            }
        }

        for (auto statName : CUSTOM_STATISTICS) {
            if (operationStatistic.HasCustomStatistics(statName)) {
                auto st = operationStatistic.GetCustomStatistics(statName);
                statEntries.emplace_back(TString{statName}, st.Sum(), st.Max(), st.Min(), st.Avg(), st.Count());
            }
        }

        statEntries.emplace_back("_cluster", ytServer);
        statEntries.emplace_back("_cluster_name", ytClusterName);
        statEntries.emplace_back("_id", GetGuidAsString(operation->GetId()));
        statWriter(progress.Id, statEntries);
        progressWriter(progress);
    });
}

void* TOperationTracker::Tracker(void* param) {
    static_cast<TOperationTracker*>(param)->Tracker();
    return nullptr;

}

void TOperationTracker::Tracker() {
    while (Running_) {
        decltype(RunningOperations_) ops;
        with_lock(Mutex_) {
            ops.reserve(RunningOperations_.size());
            ops.swap(RunningOperations_);
        }
        decltype(RunningOperations_) activeOps(Reserve(ops.size()));
        for (auto& op: ops) {
            try {
                if (op()) {
                    activeOps.push_back(op);
                }
            } catch (...) {
            }
            if (!Running_) {
                break;
            }
        }
        if (!Running_) {
            break;
        }
        with_lock(Mutex_) {
            RunningOperations_.insert(RunningOperations_.end(), activeOps.begin(), activeOps.end());
        }

        Sleep(TDuration::Seconds(1));
    }
}

} // NNative

} // NYql
