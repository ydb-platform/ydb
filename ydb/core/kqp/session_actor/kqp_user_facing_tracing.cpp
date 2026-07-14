#include "kqp_user_facing_tracing.h"
#include "kqp_query_state.h"
#include "kqp_query_stats.h"

#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <util/generic/utility.h>

namespace NKikimr::NKqp {

namespace {

void EmitPhase(const NWilson::TTraceId& parentId, TInstant start, TInstant end, const TString& name) {
    if (start == TInstant::Zero() || end == TInstant::Zero() || end <= start) {
        return;
    }
    NWilson::TTraceId childId = parentId.Span(TWilsonKqp::KqpSession);
    NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
        parentId, childId, start, end,
        NWilson::NTraceProto::Status::STATUS_CODE_OK, name);
    span.End();
}

// Builds Queued/Compile/Execute as children of userSpan (post-hoc, no lifecycle stamps).
// Execute is emitted only from real executer timings (Executions[]), so compile-failed
// or purely compute queries get no phantom Execute. Precise timings need level>=FULL
// (see the level->stats-mode mapping in the session actor).
void BuildPhases(NWilson::TSpan& userSpan, TInstant startTime, TInstant continueTime,
        ui64 compileDurationUs, const TKqpQueryStats& queryStats) {
    const NWilson::TTraceId parentId = userSpan.GetTraceId();

    const bool queued = continueTime != TInstant::Zero() && continueTime > startTime;
    const TInstant afterQueue = queued ? continueTime : startTime;

    if (queued) {
        EmitPhase(parentId, startTime, continueTime, "Queued");
    }
    if (compileDurationUs) {
        EmitPhase(parentId, afterQueue, afterQueue + TDuration::MicroSeconds(compileDurationUs), "Compile");
    }

    TInstant execStart = TInstant::Max();
    TInstant execEnd = TInstant::Zero();
    for (const auto& e : queryStats.Executions) {
        if (e.GetStartTimeMs()) {
            execStart = Min(execStart, TInstant::MilliSeconds(e.GetStartTimeMs()));
        }
        if (e.GetFinishTimeMs()) {
            execEnd = Max(execEnd, TInstant::MilliSeconds(e.GetFinishTimeMs()));
        }
    }
    if (execStart != TInstant::Max() && execEnd != TInstant::Zero()) {
        EmitPhase(parentId, execStart, execEnd, "Execute");
    }
}

} // namespace

void FinishUserFacingSpan(TKqpQueryState& state, bool success, const TString& statusCode) {
    if (!state.UserSpan) {
        return;
    }
    BuildPhases(state.UserSpan, state.StartTime, state.ContinueTime,
        state.CompileStats.DurationUs, state.QueryStats);
    state.UserSpan.Attribute("db.response.status_code", statusCode);
    if (success) {
        state.UserSpan.EndOk();
    } else {
        state.UserSpan.EndError(statusCode);
    }
}

} // namespace NKikimr::NKqp
