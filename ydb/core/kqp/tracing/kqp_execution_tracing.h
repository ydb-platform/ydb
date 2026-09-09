#pragma once

#include "kqp_task_tracing.h"

#include <ydb/public/api/protos/ydb_status_codes.pb.h>

#include <map>
#include <optional>
#include <tuple>
#include <utility>
#include <vector>

namespace NYql::NDqProto {
class TDqExecutionStats;
class TDqTaskStats;
class TEvComputeActorState;
} // namespace NYql::NDqProto

namespace NKikimr::NKqp {

class TBatchExecutionTrace {
public:
    void AddExecution(const NYql::NDqProto::TDqExecutionStats& stats);
    void Export(NYql::NDqProto::TDqExecutionStats& stats) const;

private:
    ui64 CpuUs_ = 0;
    ui64 WaitUs_ = 0;
    ui64 SpilledBytes_ = 0;
    double MaxTaskSkew_ = 0;
    bool TaskStatsIncomplete_ = false;
};

// Consumes terminal task reports once; the executer's planner deduplicates them.
class TExecutionTrace {
public:
    explicit TExecutionTrace(ui8 verbosity);

    ui64 StartStage(const NWilson::TSpan& parent, std::pair<ui64, ui32> stageId,
        const NKqpProto::TKqpPhyStage& physicalStage, ui64 taskCount);
    void AnnotateTask(std::pair<ui64, ui32> stageId, NYql::NDqProto::TDqTask& task) const;
    void OnTaskFinished(std::pair<ui64, ui32> stageId, ui64 taskCount, const NYql::NDqProto::TEvComputeActorState& state, ui32 nodeId);
    void AddTask(ui64 txIndex, ui64 taskCount,
        const NYql::NDqProto::TDqTaskStats& task, std::optional<ui64> durationUs, ui32 nodeId,
        Ydb::StatusIds::StatusCode status);
    void Finish(NWilson::TSpan& span, NYql::NDqProto::TDqExecutionStats& stats,
        Ydb::StatusIds::StatusCode status);

private:
    class TTask {
    public:
        auto Rank() const {
            return std::tuple(Failed, SpilledBytes > 0 || Retries > 0
                || (DurationUs > 0 && WaitUs >= DurationUs / 2), DurationUs, Id);
        }

    public:
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

private:
    const bool CollectDetails_;
    std::map<std::pair<ui64, ui32>, TStage> Stages_;
    ui64 WaitUs_ = 0;
    ui64 SpilledBytes_ = 0;
    ui64 UnrepresentedStageTasks_ = 0;
};

} // namespace NKikimr::NKqp
