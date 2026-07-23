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
using TUserFacingTraceTaskStats = std::unordered_map<ui32, std::unordered_map<ui64, NYql::NDqProto::TDqComputeActorStats>>;

// Cap on retained tasks per stage (top-N by duration once full — stragglers matter most);
// bounds executer memory on wide OLAP stages, the stage span gets ydb.tasks_truncated when hit.
constexpr size_t MaxUserFacingTraceTasksPerStage = 128;

// Operational phases the executer passes through. The renderer decides presentation: which are
// grouped under "Prepare", and their user-facing names.
enum class EUserFacingTracePhase : size_t {
    ResolveTables = 0,
    ResolveShards,
    Snapshot,
    RunTasks,
    Count,
};

struct TUserFacingTraceTimeline {
    struct TWindow {
        TInstant Start;
        TInstant End;

        explicit operator bool() const {
            return Start != TInstant::Zero() && End > Start;
        }
    };

    TWindow Execute;
    std::array<TWindow, static_cast<size_t>(EUserFacingTracePhase::Count)> Phases;

    TWindow& Phase(EUserFacingTracePhase phase) {
        return Phases[static_cast<size_t>(phase)];
    }
    const TWindow& Phase(EUserFacingTracePhase phase) const {
        return Phases[static_cast<size_t>(phase)];
    }
};

struct TUserFacingTraceExecutionData {
    TUserFacingTraceTimeline Timeline;
    TUserFacingTraceTaskStats TaskStats;
    // Stats exported at collection depth for the trace; the response's stats stay at the
    // client-requested mode and must not be used for rendering.
    NYql::NDqProto::TDqExecutionStats ExecStats;
};

} // namespace NKikimr::NKqp
