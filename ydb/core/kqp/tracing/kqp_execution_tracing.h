#pragma once

#include "kqp_query_tracing.h"
#include "kqp_task_tracing.h"

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <ydb/library/yql/dq/actors/protos/dq_events.pb.h>

#include <map>
#include <optional>
#include <tuple>
#include <vector>

namespace NKikimr::NKqp {

// Consumes terminal task reports once; the executer's planner deduplicates them.
class TExecutionTrace {
public:
    explicit TExecutionTrace(ui8 verbosity);

    ui64 StartStage(const NWilson::TSpan& parent, std::pair<ui64, ui32> stageId,
        const TTaskTraceDescription& description, ui64 taskCount);
    void OnTaskFinished(std::pair<ui64, ui32> stageId, const TTaskTraceDescription& description,
        ui64 taskCount, const NYql::NDqProto::TEvComputeActorState& state, ui32 nodeId);
    void AddTask(ui64 txIndex, const TTaskTraceDescription& description, ui64 taskCount,
        const NYql::NDqProto::TDqTaskStats& task, std::optional<ui64> durationUs, ui32 nodeId,
        Ydb::StatusIds::StatusCode status);
    void Finish(NWilson::TSpan& span, NYql::NDqProto::TDqExecutionStats& stats,
        Ydb::StatusIds::StatusCode status);

private:
    struct TTask {
        ui64 Id = 0;
        ui32 Node = 0;
        ui64 DurationUs = 0;
        ui64 CpuUs = 0;
        ui64 InputRows = 0;
        ui64 OutputRows = 0;
        ui64 WaitUs = 0;
        ui64 SpilledBytes = 0;
        ui64 Retries = 0;
        bool Failed = false;

        auto Rank() const {
            return std::tuple(Failed, SpilledBytes > 0 || Retries > 0
                || (DurationUs > 0 && WaitUs >= DurationUs / 2), DurationUs, Id);
        }
    };

    struct TStage {
        NWilson::TSpan Span;
        Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::SUCCESS;
        TTaskTraceDescription Description;
        ui64 TaskCount = 0;
        ui64 Reports = 0;
        ui64 FailedTasks = 0;
        ui64 CpuUs = 0;
        ui64 InputRows = 0;
        ui64 OutputRows = 0;
        ui64 WaitUs = 0;
        ui64 SpilledBytes = 0;
        ui64 MinDurationUs = 0;
        ui64 MaxDurationUs = 0;
        ui64 SumDurationUs = 0;
        ui64 Durations = 0;
        ui32 FastestNode = 0;
        ui32 SlowestNode = 0;
        ui64 UnrepresentedNodeTasks = 0;
        std::map<ui32, ui64> TasksByNode;
        std::vector<TTask> Tasks;
    };

    static void FinishStage(TStage& stage, Ydb::StatusIds::StatusCode status);

    const bool CollectDetails;
    std::map<std::pair<ui64, ui32>, TStage> Stages;
    ui64 WaitUs = 0;
    ui64 SpilledBytes = 0;
    ui64 UnrepresentedStageTasks = 0;
};

}
