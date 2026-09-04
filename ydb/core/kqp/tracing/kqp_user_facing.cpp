#include "kqp_user_facing.h"

#include <ydb/core/kqp/common/events/query.h>
#include <ydb/core/kqp/common/kqp_execution_trace.h>
#include <ydb/library/wilson_ids/wilson.h>

#include <util/generic/utility.h>

#include <vector>

namespace NKikimr::NKqp {

TProxyUserFacingTraceContext::TProxyUserFacingTraceContext(
        NPrivateEvents::TEvQueryRequest& request)
    : ParentTraceId(request.GetUserFacingWilsonTraceId())
    , Action(request.GetAction())
    , Origin(request.Record.GetUserFacingTrace().ProxyRequestHopsSize() == 0) {
    if (request.GetQuerySize() <= MaxUserFacingQueryTextSize) {
        QueryText = request.GetQuery();
    }
    if (const auto& seed = request.GetProxyTraceSeed()) {
        StartedAt = seed->StartTime;
        MonotonicStartedAt = seed->StartedAt;
        HasStart = true;
    }
    if (Origin) {
        RootTraceId = ParentTraceId.Span(ParentTraceId.GetVerbosity());
        RootTraceId.Serialize(request.Record.MutableUserFacingTrace()->MutableTraceId());
    } else {
        RootTraceId = NWilson::TTraceId(ParentTraceId);
    }
}

void TProxyUserFacingTraceContext::MarkSent(ui32 sourceNodeId, ui32 targetNodeId,
        NPrivateEvents::TEvQueryRequest& request) {
    Y_ABORT_UNLESS(HasStart);
    const TDuration hopDuration = NActors::TMonotonic::Now() - MonotonicStartedAt;
    SentAt = StartedAt + hopDuration;
    TargetNodeId = targetNodeId;
    auto* trace = request.Record.MutableUserFacingTrace();
    if (!trace->HasOriginSentAtUs()) {
        trace->SetOriginSentAtUs(SentAt.MicroSeconds());
    }
    auto* hop = trace->AddProxyRequestHops();
    hop->SetNodeId(sourceNodeId);
    hop->SetTargetNodeId(targetNodeId);
    hop->SetDurationUs(hopDuration.MicroSeconds());
}

std::optional<TProxyUserFacingTraceSnapshot> TProxyUserFacingTraceContext::Detach(
        Ydb::StatusIds::StatusCode status, ui32 nodeId,
        TString name, TString operation, TString coverage) {
    if (!Origin || !ParentTraceId || !RootTraceId) {
        return std::nullopt;
    }
    const TInstant finishedAt = HasStart
        ? StartedAt + (NActors::TMonotonic::Now() - MonotonicStartedAt)
        : TInstant::Now();
    const TInstant startedAt = HasStart ? StartedAt : finishedAt;
    const bool hasSessionTrace = bool(name);
    if (!hasSessionTrace) {
        name = NKikimrKqp::EQueryAction_Name(Action);
        constexpr TStringBuf prefix = "QUERY_ACTION_";
        if (name.StartsWith(prefix)) {
            name = name.substr(prefix.size());
        }
        operation = name;
    }
    return TProxyUserFacingTraceSnapshot{
        .ParentTraceId = std::move(ParentTraceId),
        .RootTraceId = std::move(RootTraceId),
        .QueryText = std::move(QueryText),
        .StartedAt = startedAt,
        .SentAt = SentAt,
        .FinishedAt = finishedAt,
        .Name = std::move(name),
        .Operation = std::move(operation),
        .Status = status,
        .NodeId = nodeId,
        .TargetNodeId = TargetNodeId,
        .HasSessionTrace = hasSessionTrace,
        .Coverage = std::move(coverage),
    };
}

bool TProxyUserFacingTraceContext::IsOrigin() const {
    return Origin;
}

NActors::IActor* CreateRejectedUserFacingTraceRendererActor(
        const NPrivateEvents::TEvQueryRequest& request,
        Ydb::StatusIds::StatusCode status) {
    NWilson::TTraceId traceId = request.GetUserFacingWilsonTraceId();
    if (!traceId) {
        return nullptr;
    }

    TRejectedUserFacingQuerySnapshot snapshot;
    snapshot.TraceId = std::move(traceId);
    if (request.GetQuerySize() <= MaxUserFacingQueryTextSize) {
        snapshot.QueryText = request.GetQuery();
    }
    const auto& trace = request.Record.GetUserFacingTrace();
    snapshot.ProxyRequestHops.assign(trace.GetProxyRequestHops().begin(),
        trace.GetProxyRequestHops().end());
    snapshot.RejectedAt = MapUserFacingSessionStart(TInstant::Now(),
        TInstant::MicroSeconds(trace.GetOriginSentAtUs()),
        trace.GetProxyRequestHops());
    snapshot.Status = status;
    return CreateRejectedUserFacingTraceRendererActor(std::move(snapshot));
}

TInstant MapUserFacingSessionStart(TInstant localStart, TInstant originSentAt,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TProxyRequestHop>& proxyHops) {
    if (originSentAt == TInstant::Zero() || proxyHops.size() <= 1) {
        return localStart;
    }
    TInstant mappedStart = originSentAt;
    for (int i = 1; i < proxyHops.size(); ++i) {
        mappedStart += TDuration::MicroSeconds(proxyHops.Get(i).GetDurationUs());
    }
    return mappedStart;
}

TUserFacingTraceContext::TUserFacingTraceContext(NWilson::TTraceId traceId,
        TInstant sessionStart,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TProxyRequestHop>& proxyHops,
        TInstant originSentAt)
    : TraceId(std::move(traceId))
    , SessionStart(sessionStart)
    , ProxyHops(proxyHops.begin(), proxyHops.end()) {
    if (originSentAt != TInstant::Zero() && !ProxyHops.empty()) {
        const TInstant mappedSessionStart = MapUserFacingSessionStart(
            sessionStart, originSentAt, proxyHops);
        TimestampOffsetUs = static_cast<i64>(mappedSessionStart.MicroSeconds())
            - static_cast<i64>(sessionStart.MicroSeconds());
        SessionStart = mappedSessionStart;
    }
    const ui8 level = TraceId.GetVerbosity();
    using TLevels = TComponentTracingLevels::TQueryProcessor;
    DiagnosticsPolicy.CollectTimeline = true;
    DiagnosticsPolicy.CollectStageAggregates = level >= TLevels::Detailed;
    DiagnosticsPolicy.CollectTaskSamples = level >= TLevels::Detailed;
    DiagnosticsPolicy.CollectShardSamples = level >= TLevels::Diagnostic;
    DiagnosticsPolicy.CollectBufferLookup = level >= TLevels::Detailed;
    DiagnosticsPolicy.CollectCommitTimeline = level >= TLevels::Detailed;
}

const TExecutionDiagnosticsPolicy& TUserFacingTraceContext::GetDiagnosticsPolicy() const {
    return DiagnosticsPolicy;
}

void TUserFacingTraceContext::StartAdmission(TInstant at) {
    AdmissionStartedAt = at;
}

void TUserFacingTraceContext::FinishAdmission(Ydb::StatusIds::StatusCode status, TInstant at) {
    AdmissionFinishedAt = at;
    AdmissionStatus = status;
}

void TUserFacingTraceContext::MarkExecutionDelegated() {
    ExecutionDelegated = true;
}

void TUserFacingTraceContext::BeginCompile(TInstant at) {
    if (ActiveCompileAttempt || OverflowCompileAttempt) {
        return;
    }
    if (CompileAttempts.size() >= MaxCompileAttempts) {
        OverflowCompileAttempt = TCompileAttemptDiagnostic{.Start = at};
        return;
    }
    CompileAttempts.push_back({.Start = at});
    ActiveCompileAttempt = CompileAttempts.size() - 1;
}

void TUserFacingTraceContext::RecordCompileCacheHit(
        Ydb::StatusIds::StatusCode status, TInstant at) {
    KeepCompileAttempt(CompileAttempts, {
        .Start = at,
        .End = at,
        .FromCache = true,
        .Status = status,
    }, CompileAttemptsDropped);
}

void TUserFacingTraceContext::FinishCompile(bool fromCache,
        Ydb::StatusIds::StatusCode status,
        std::shared_ptr<const TCompileDiagnostics> dependencies,
        std::optional<TCompileActorDiagnostic> actor, TInstant at) {
    if (!ActiveCompileAttempt && !OverflowCompileAttempt) {
        return;
    }
    auto& attempt = ActiveCompileAttempt
        ? CompileAttempts[*ActiveCompileAttempt]
        : *OverflowCompileAttempt;
    attempt.End = at;
    attempt.FromCache = fromCache;
    attempt.Status = status;
    attempt.Dependencies = std::move(dependencies);
    attempt.Actor = std::move(actor);
    attempt.Partial = !attempt.FromCache && !attempt.Actor;
    if (OverflowCompileAttempt) {
        KeepCompileAttempt(CompileAttempts,
            std::move(*OverflowCompileAttempt), CompileAttemptsDropped);
        OverflowCompileAttempt.reset();
    } else {
        ActiveCompileAttempt.reset();
    }
}

void TUserFacingTraceContext::FinishSplit(Ydb::StatusIds::StatusCode status, TInstant at) {
    FinishCompile(/*fromCache*/ false, status, nullptr, std::nullopt, at);
}

void TUserFacingTraceContext::AddExecutions(
        std::vector<TExecutionTraceSnapshot>& source,
        const TExecutionTraceTotals& totals, size_t sourceDropped) {
    AccumulateExecutionTraceTotals(ExecutionTraceTotals, totals);
    AppendExecutionTraceSnapshots(ExecutionTraces, ExecutionTracesDropped,
        source, sourceDropped, DiagnosticsPolicy.MaxExecutions);
}

void TUserFacingTraceContext::UpdateQueryDescription(
        const TUserFacingQueryDescription& description) {
    if (!description.DisplayName) {
        return;
    }
    if (description.DisplayName == "EXECUTE SCRIPT") {
        RootName = description.DisplayName;
        Operation = description.Operation;
        return;
    }
    if (!RootName) {
        RootName = description.DisplayName;
        Operation = description.Operation;
    } else if (RootName != description.DisplayName) {
        RootName = "EXECUTE SCRIPT";
        Operation = "EXECUTE SCRIPT";
    }
}

TTimeWindow FitUserFacingRemoteWindow(TTimeWindow window, const TTimeWindow& parent) {
    if (window.Start == TInstant::Zero() || window.End < window.Start) {
        return {};
    }
    if (window.End == window.Start) {
        window.End += TDuration::MicroSeconds(1);
    }
    if (!parent) {
        return window;
    }
    const TDuration duration = window.End - window.Start;
    const TDuration parentDuration = parent.End - parent.Start;
    if (duration >= parentDuration) {
        return parent;
    }
    if (window.Start < parent.Start) {
        window.Start = parent.Start;
        window.End = window.Start + duration;
    } else if (window.End > parent.End) {
        window.End = parent.End;
        window.Start = window.End - duration;
    }
    return window;
}

namespace {

TInstant ShiftTimestamp(TInstant value, i64 offsetUs) {
    if (value == TInstant::Zero() || offsetUs == 0) {
        return value;
    }
    return offsetUs > 0
        ? value + TDuration::MicroSeconds(offsetUs)
        : value - TDuration::MicroSeconds(-offsetUs);
}

void ShiftWindow(TTimeWindow& window, i64 offsetUs) {
    window.Start = ShiftTimestamp(window.Start, offsetUs);
    window.End = ShiftTimestamp(window.End, offsetUs);
}

void ShiftShardRead(NKqpProto::TKqpShardReadStats& shard, i64 offsetUs) {
    if (shard.GetStartTimeMs()) {
        shard.SetStartTimeMs(ShiftTimestamp(
            TInstant::MilliSeconds(shard.GetStartTimeMs()), offsetUs).MilliSeconds());
    }
    if (shard.GetFinishTimeMs()) {
        shard.SetFinishTimeMs(ShiftTimestamp(
            TInstant::MilliSeconds(shard.GetFinishTimeMs()), offsetUs).MilliSeconds());
    }
}

void ShiftShardReads(std::vector<NKqpProto::TKqpShardReadStats>& shards, i64 offsetUs) {
    for (auto& shard : shards) {
        ShiftShardRead(shard, offsetUs);
    }
}

void ShiftShardAcks(std::vector<TShardAckDiagnostic>& shards, i64 offsetUs) {
    for (auto& shard : shards) {
        shard.AcknowledgedAt = ShiftTimestamp(shard.AcknowledgedAt, offsetUs);
    }
}

// Normalizes timestamps to origin.
void ShiftExecutionTrace(TExecutionTraceSnapshot& trace, i64 offsetUs) {
    ShiftWindow(trace.Timeline.Execute, offsetUs);
    for (auto& phase : trace.Timeline.Phases) {
        ShiftWindow(phase, offsetUs);
    }
    for (auto& stage : trace.Stages) {
        ShiftWindow(stage.Window, offsetUs);
        for (auto& task : stage.InterestingTasks) {
            ShiftWindow(task.Window, offsetUs);
            ShiftShardReads(task.Shards, offsetUs);
        }
    }
    ShiftShardReads(trace.BufferLookup.Shards, offsetUs);
    ShiftWindow(trace.Commit.PrepareShards, offsetUs);
    ShiftWindow(trace.Commit.Coordinator, offsetUs);
    ShiftWindow(trace.Commit.ApplyShards, offsetUs);
    ShiftShardAcks(trace.Commit.PreparedShards, offsetUs);
    ShiftShardAcks(trace.Commit.CommittedShards, offsetUs);
}

void ShiftCompileAttempt(TCompileAttemptDiagnostic& attempt, i64 offsetUs) {
    attempt.Start = ShiftTimestamp(attempt.Start, offsetUs);
    attempt.End = ShiftTimestamp(attempt.End, offsetUs);
    if (attempt.Actor) {
        attempt.Actor->Start = ShiftTimestamp(attempt.Actor->Start, offsetUs);
        attempt.Actor->End = ShiftTimestamp(attempt.Actor->End, offsetUs);
    }
    if (attempt.Dependencies) {
        auto shifted = std::make_shared<TCompileDiagnostics>(*attempt.Dependencies);
        for (auto& dependency : shifted->Dependencies) {
            dependency.Start = ShiftTimestamp(dependency.Start, offsetUs);
            dependency.End = ShiftTimestamp(dependency.End, offsetUs);
        }
        attempt.Dependencies = std::move(shifted);
    }
}

} // namespace

TUserFacingQuerySnapshot TUserFacingTraceContext::DetachSnapshot(
        TUserFacingQueryCompletion completion, TInstant localRootEnd) {
    if (ActiveCompileAttempt) {
        CompileAttempts[*ActiveCompileAttempt].End = localRootEnd;
        ActiveCompileAttempt.reset();
    }
    if (OverflowCompileAttempt) {
        OverflowCompileAttempt->End = localRootEnd;
        KeepCompileAttempt(CompileAttempts, std::move(*OverflowCompileAttempt),
            CompileAttemptsDropped);
        OverflowCompileAttempt.reset();
    }
    for (auto& attempt : CompileAttempts) {
        ShiftCompileAttempt(attempt, TimestampOffsetUs);
    }
    for (auto& trace : ExecutionTraces) {
        ShiftExecutionTrace(trace, TimestampOffsetUs);
    }
    TUserFacingQuerySnapshot snapshot;
    snapshot.TraceId = std::move(TraceId);
    snapshot.RootName = RootName ? RootName : std::move(completion.FallbackName);
    snapshot.Operation = Operation ? Operation : snapshot.RootName;
    snapshot.QueryText = std::move(completion.QueryText);
    snapshot.RootEnd = ShiftTimestamp(localRootEnd, TimestampOffsetUs);
    snapshot.StartTime = SessionStart;
    snapshot.ProxyRequestHops = std::move(ProxyHops);
    snapshot.AdmissionStartedAt = ShiftTimestamp(
        AdmissionStartedAt, TimestampOffsetUs);
    snapshot.AdmissionFinishedAt = ShiftTimestamp(
        AdmissionFinishedAt, TimestampOffsetUs);
    snapshot.AdmissionStatus = AdmissionStatus;
    snapshot.PoolId = std::move(completion.PoolId);
    snapshot.Metrics = completion.Metrics;
    snapshot.ExecutionTraces = std::move(ExecutionTraces);
    snapshot.ExecutionTraceTotals = ExecutionTraceTotals;
    snapshot.ExecutionTracesDropped = ExecutionTracesDropped;
    snapshot.CompileAttempts = std::move(CompileAttempts);
    snapshot.CompileAttemptsDropped = CompileAttemptsDropped;
    snapshot.ExecutionDelegated = ExecutionDelegated;
    snapshot.Success = completion.Success;
    snapshot.StatusCode = std::move(completion.StatusCode);
    return snapshot;
}

} // namespace NKikimr::NKqp
