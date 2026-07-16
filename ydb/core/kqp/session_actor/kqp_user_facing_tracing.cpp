#include "kqp_user_facing_tracing.h"
#include "kqp_query_state.h"
#include "kqp_query_stats.h"

#include <ydb/library/wilson_ids/wilson.h>
#include <ydb/library/yql/dq/actors/protos/dq_stats.pb.h>
#include <util/generic/utility.h>

namespace NKikimr::NKqp {

namespace {

using TPhaseAttrs = std::initializer_list<std::pair<TString, NWilson::TAttributeValue>>;

NWilson::TSpan MakePhase(const NWilson::TTraceId& parentId, TInstant start, TInstant end,
        const TString& name, TPhaseAttrs attrs = {}) {
    if (start == TInstant::Zero() || end == TInstant::Zero() || end <= start) {
        return {};
    }
    NWilson::TSpan span = NWilson::TSpan::ConstructTerminated(
        parentId, parentId.Span(TWilsonKqp::KqpSession), start, end,
        NWilson::NTraceProto::Status::STATUS_CODE_OK, name);
    for (const auto& [key, value] : attrs) {
        span.Attribute(key, value);
    }
    return span;
}

void EmitPhase(const NWilson::TTraceId& parentId, TInstant start, TInstant end,
        const TString& name, TPhaseAttrs attrs = {}) {
    NWilson::TSpan span = MakePhase(parentId, start, end, name, attrs);
    if (span) {
        span.End();
    }
}

// Builds the pre-execution phases as children of userSpan (post-hoc, no lifecycle stamps) and
// sets the query-level attributes. Queue wait and compile happen in the session before the
// executer and are not yet instrumented live, so they are derived from stats here. Execution
// phases (Execute -> Resolve/Snapshot/RunTasks) are emitted live by the executer.
void BuildPhases(NWilson::TSpan& userSpan, const TKqpQueryState& state) {
    const NWilson::TTraceId parentId = userSpan.GetTraceId();

    i64 rowsRead = 0;
    i64 rowsWritten = 0;
    i64 bytesRead = 0;
    ui64 cpuUs = 0;
    ui64 waitUs = 0;       // time compute actors spent waiting on I/O rather than computing
    ui64 spilledBytes = 0; // bytes spilled to disk under memory pressure (a slowdown source)
    double maxSkew = 0.0;  // worst stage's slowest-task / avg-task ratio (straggler detector)
    for (const auto& e : state.QueryStats.Executions) {
        cpuUs += e.GetCpuTimeUs();
        for (const auto& table : e.GetTables()) {
            rowsRead += table.GetReadRows();
            rowsWritten += table.GetWriteRows();
            bytesRead += table.GetReadBytes();
        }
        for (const auto& stage : e.GetStages()) {
            spilledBytes += stage.GetSpillingComputeBytes().GetSum() + stage.GetSpillingChannelBytes().GetSum();
            waitUs += stage.GetWaitInputTimeUs().GetSum() + stage.GetWaitOutputTimeUs().GetSum();
            const auto& taskDur = stage.GetDurationUs();
            if (stage.GetTotalTasksCount() > 1 && taskDur.GetCnt() > 0 && taskDur.GetSum() > 0) {
                const double avg = static_cast<double>(taskDur.GetSum()) / taskDur.GetCnt();
                if (avg > 0) {
                    maxSkew = Max(maxSkew, static_cast<double>(taskDur.GetMax()) / avg);
                }
            }
        }
    }

    userSpan.Attribute("ydb.consumed_ru", static_cast<i64>(CalcRequestUnit(state.QueryStats)));
    userSpan.Attribute("ydb.rows_read", rowsRead);
    userSpan.Attribute("ydb.rows_written", rowsWritten);
    userSpan.Attribute("ydb.bytes_read", bytesRead);
    userSpan.Attribute("ydb.cpu_us", static_cast<i64>(cpuUs));
    if (waitUs > 0) {
        userSpan.Attribute("ydb.wait_us", static_cast<i64>(waitUs));
    }
    if (spilledBytes > 0) {
        userSpan.Attribute("ydb.spilled_bytes", static_cast<i64>(spilledBytes));
    }
    if (maxSkew > 1.0) {
        userSpan.Attribute("ydb.max_task_skew", maxSkew);
    }
    // Lock contention: victim => this query aborted/retried due to a conflict; breaker => it
    // invalidated another tx's locks. Either explains latency that isn't CPU or I/O.
    if (state.QueryStats.LocksBrokenAsVictim > 0) {
        userSpan.Attribute("ydb.locks_broken_as_victim", static_cast<i64>(state.QueryStats.LocksBrokenAsVictim));
    }
    if (state.QueryStats.LocksBrokenAsBreaker > 0) {
        userSpan.Attribute("ydb.locks_broken_as_breaker", static_cast<i64>(state.QueryStats.LocksBrokenAsBreaker));
    }

    if (state.ContinueTime != TInstant::Zero() && state.ContinueTime > state.StartTime) {
        const TString poolId = state.UserRequestContext ? state.UserRequestContext->PoolId : TString();
        EmitPhase(parentId, state.StartTime, state.ContinueTime, "Queued", {{"ydb.pool_id", poolId}});
    }

    const TInstant afterQueue = (state.ContinueTime > state.StartTime) ? state.ContinueTime : state.StartTime;
    if (state.CompileStats.DurationUs) {
        const TInstant compileEnd = afterQueue + TDuration::MicroSeconds(state.CompileStats.DurationUs);
        EmitPhase(parentId, afterQueue, compileEnd, "Compile",
            {{"ydb.compile.cache_hit", state.CompileStats.FromCache}});
    }
}

} // namespace

TString UserFacingRootSpanName(const TString& queryText, const TString& fallback) {
    TString head = queryText.substr(0, 256);
    head.to_upper();
    size_t best = TString::npos;
    TString bestVerb;
    for (const char* verb : {"SELECT", "UPSERT", "INSERT", "UPDATE", "DELETE", "REPLACE"}) {
        const size_t pos = head.find(verb);
        if (pos < best) {
            best = pos;
            bestVerb = verb;
        }
    }
    return best == TString::npos ? fallback : bestVerb;
}

void FinishUserFacingSpan(TKqpQueryState& state, bool success, const TString& statusCode) {
    if (!state.UserSpan) {
        return;
    }
    BuildPhases(state.UserSpan, state);
    state.UserSpan.Attribute("db.response.status_code", statusCode);
    if (success) {
        state.UserSpan.EndOk();
    } else {
        state.UserSpan.EndError(statusCode);
    }
}

} // namespace NKikimr::NKqp
