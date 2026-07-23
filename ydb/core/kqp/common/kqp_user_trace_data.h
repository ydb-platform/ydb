#pragma once

#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <array>
#include <unordered_map>

namespace NKikimr::NKqp {

// Source data for the user-facing trace of one execution. The executer never creates user-facing
// spans: it stamps plain timestamps here (and retains per-task stats), ships the struct to the
// session in the local TEvTxResponse, and the single renderer in kqp_user_facing_tracing.cpp
// builds the whole span tree at reply time.

// stageId -> taskId -> compute actor stats snapshot. Deliberately separate from the exported
// stats proto — a trace-sampled query must produce the same plan as an unsampled one.
using TUserTraceTaskStats = std::unordered_map<ui32, std::unordered_map<ui64, NYql::NDqProto::TDqComputeActorStats>>;

// Cap on retained tasks per stage (first-come): bounds executer memory on wide OLAP stages;
// the stage span gets ydb.tasks_truncated when hit.
constexpr size_t MaxUserTraceTasksPerStage = 128;

// Operational phases the executer passes through. The renderer decides presentation: which are
// grouped under "Prepare", and their user-facing names.
enum class EUserTracePhase : size_t {
    ResolveTables = 0,
    ResolveShards,
    Snapshot,
    RunTasks,
    Count,
};

struct TUserTraceTimeline {
    struct TWindow {
        TInstant Start;
        TInstant End;

        explicit operator bool() const {
            return Start != TInstant::Zero() && End > Start;
        }
    };

    TWindow Execute;
    std::array<TWindow, static_cast<size_t>(EUserTracePhase::Count)> Phases;

    TWindow& Phase(EUserTracePhase phase) {
        return Phases[static_cast<size_t>(phase)];
    }
    const TWindow& Phase(EUserTracePhase phase) const {
        return Phases[static_cast<size_t>(phase)];
    }
};

struct TUserTraceExecutionData {
    TUserTraceTimeline Timeline;
    TUserTraceTaskStats TaskStats;
};

} // namespace NKikimr::NKqp
