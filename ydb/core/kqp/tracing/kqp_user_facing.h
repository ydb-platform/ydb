#pragma once

#include <ydb/core/kqp/common/compilation/kqp_compile_diagnostics.h>
#include <ydb/core/kqp/common/kqp_execution_trace.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/core/protos/kqp_physical.pb.h>
#include <ydb/library/actors/core/monotonic.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include <util/generic/string.h>
#include <util/generic/maybe.h>

#include <optional>
#include <vector>

namespace NActors {
class IActor;
}

namespace NKikimr::NKqp {

namespace NPrivateEvents {
struct TEvQueryRequest;
}

constexpr size_t MaxUserFacingSpansPerQuery = 1000;
constexpr size_t MaxUserFacingQueryTextSize = 64 * 1024;

TInstant MapUserFacingSessionStart(TInstant localStart, TInstant originSentAt,
    const google::protobuf::RepeatedPtrField<NKikimrKqp::TProxyRequestHop>& proxyHops);

struct TUserFacingQueryDescription {
    TString DisplayName;
    TString Operation;
};

struct TUserFacingQueryMetrics {
    ui64 ConsumedRu = 0;
    ui64 RowsRead = 0;
    ui64 RowsWritten = 0;
    ui64 BytesRead = 0;
    ui64 LocksBrokenAsBreaker = 0;
    ui64 LocksBrokenAsVictim = 0;
};

struct TUserFacingQueryCompletion {
    TString FallbackName;
    TString QueryText;
    TString PoolId;
    TUserFacingQueryMetrics Metrics;
    bool Success = false;
    TString StatusCode;
};

struct TUserFacingQuerySnapshot;

class TUserFacingTraceContext {
public:
    TUserFacingTraceContext(NWilson::TTraceId traceId, TInstant sessionStart,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TProxyRequestHop>& proxyHops,
        TInstant originSentAt);

    TUserFacingTraceContext(const TUserFacingTraceContext&) = delete;
    TUserFacingTraceContext& operator=(const TUserFacingTraceContext&) = delete;
    TUserFacingTraceContext(TUserFacingTraceContext&&) = default;
    TUserFacingTraceContext& operator=(TUserFacingTraceContext&&) = default;

    const TExecutionDiagnosticsPolicy& GetDiagnosticsPolicy() const;
    void StartAdmission(TInstant at = TInstant::Now());
    void FinishAdmission(Ydb::StatusIds::StatusCode status, TInstant at = TInstant::Now());
    void MarkExecutionDelegated();

    void BeginCompile(TInstant at = TInstant::Now());
    void RecordCompileCacheHit(Ydb::StatusIds::StatusCode status,
        TInstant at = TInstant::Now());
    void FinishCompile(bool fromCache, Ydb::StatusIds::StatusCode status,
        std::shared_ptr<const TCompileDiagnostics> dependencies,
        std::optional<TCompileActorDiagnostic> actor, TInstant at = TInstant::Now());
    void FinishSplit(Ydb::StatusIds::StatusCode status, TInstant at = TInstant::Now());

    void AddExecutions(std::vector<TExecutionTraceSnapshot>& source,
        const TExecutionTraceTotals& totals, size_t sourceDropped);
    void UpdateQueryDescription(const TUserFacingQueryDescription& description);
    TUserFacingQuerySnapshot DetachSnapshot(TUserFacingQueryCompletion completion,
        TInstant at = TInstant::Now());

private:
    NWilson::TTraceId TraceId;
    TExecutionDiagnosticsPolicy DiagnosticsPolicy;
    TInstant SessionStart;
    i64 TimestampOffsetUs = 0;
    std::vector<NKikimrKqp::TProxyRequestHop> ProxyHops;
    TInstant AdmissionStartedAt;
    TInstant AdmissionFinishedAt;
    Ydb::StatusIds::StatusCode AdmissionStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    bool ExecutionDelegated = false;
    TString RootName;
    TString Operation;
    std::vector<TCompileAttemptDiagnostic> CompileAttempts;
    std::optional<size_t> ActiveCompileAttempt;
    std::optional<TCompileAttemptDiagnostic> OverflowCompileAttempt;
    size_t CompileAttemptsDropped = 0;
    std::vector<TExecutionTraceSnapshot> ExecutionTraces;
    TExecutionTraceTotals ExecutionTraceTotals;
    size_t ExecutionTracesDropped = 0;
};

// Detached from the session actor and consumed asynchronously by the renderer.
struct TUserFacingQuerySnapshot {
    NWilson::TTraceId TraceId;
    TString RootName;
    TString Operation;
    TString QueryText;
    TInstant RootEnd;
    TInstant StartTime;
    std::vector<NKikimrKqp::TProxyRequestHop> ProxyRequestHops;
    TInstant AdmissionStartedAt;
    TInstant AdmissionFinishedAt;
    Ydb::StatusIds::StatusCode AdmissionStatus = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    TString PoolId;
    TUserFacingQueryMetrics Metrics;
    std::vector<TExecutionTraceSnapshot> ExecutionTraces;
    TExecutionTraceTotals ExecutionTraceTotals;
    size_t ExecutionTracesDropped = 0;
    std::vector<TCompileAttemptDiagnostic> CompileAttempts;
    size_t CompileAttemptsDropped = 0;
    bool ExecutionDelegated = false;
    bool Success = false;
    TString StatusCode;
};

struct TRejectedUserFacingQuerySnapshot {
    NWilson::TTraceId TraceId;
    TString QueryText;
    std::vector<NKikimrKqp::TProxyRequestHop> ProxyRequestHops;
    TInstant RejectedAt;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
};

struct TProxyUserFacingTraceSnapshot {
    NWilson::TTraceId ParentTraceId;
    NWilson::TTraceId RootTraceId;
    TString QueryText;
    TInstant StartedAt;
    TInstant SentAt;
    TInstant FinishedAt;
    TString Name;
    TString Operation;
    Ydb::StatusIds::StatusCode Status = Ydb::StatusIds::STATUS_CODE_UNSPECIFIED;
    ui32 NodeId = 0;
    ui32 TargetNodeId = 0;
    bool HasSessionTrace = false;
    TString Coverage;
};

// Owns the entry-proxy root and converts monotonic hop measurements to Wilson timestamps.
class TProxyUserFacingTraceContext {
public:
    explicit TProxyUserFacingTraceContext(NPrivateEvents::TEvQueryRequest& request);

    TProxyUserFacingTraceContext(const TProxyUserFacingTraceContext&) = delete;
    TProxyUserFacingTraceContext& operator=(const TProxyUserFacingTraceContext&) = delete;
    TProxyUserFacingTraceContext(TProxyUserFacingTraceContext&&) = default;
    TProxyUserFacingTraceContext& operator=(TProxyUserFacingTraceContext&&) = default;

    void MarkSent(ui32 sourceNodeId, ui32 targetNodeId,
        NPrivateEvents::TEvQueryRequest& request);

    std::optional<TProxyUserFacingTraceSnapshot> Detach(
        Ydb::StatusIds::StatusCode status, ui32 nodeId,
        TString name = {}, TString operation = {}, TString coverage = {});

    bool IsOrigin() const;

private:
    NWilson::TTraceId ParentTraceId;
    NWilson::TTraceId RootTraceId;
    TString QueryText;
    TInstant StartedAt;
    NActors::TMonotonic MonotonicStartedAt;
    NKikimrKqp::EQueryAction Action;
    TInstant SentAt;
    ui32 TargetNodeId = 0;
    bool HasStart = false;
    bool Origin = false;
};

NActors::IActor* CreateUserFacingTraceRendererActor(TUserFacingQuerySnapshot snapshot);
NActors::IActor* CreateRejectedUserFacingTraceRendererActor(
    TRejectedUserFacingQuerySnapshot snapshot);
NActors::IActor* CreateRejectedUserFacingTraceRendererActor(
    const NPrivateEvents::TEvQueryRequest& request, Ydb::StatusIds::StatusCode status);
NActors::IActor* CreateProxyUserFacingTraceRendererActor(
    TProxyUserFacingTraceSnapshot snapshot);

TTimeWindow FitUserFacingRemoteWindow(TTimeWindow window, const TTimeWindow& parent);

class TUserFacingSpanBudget {
public:
    explicit TUserFacingSpanBudget(ui8 verbosity, size_t limit = MaxUserFacingSpansPerQuery,
            size_t reserved = 5)
        : Remaining_(limit > reserved ? limit - reserved : 0)
        , Verbosity_(verbosity)
    {}

    bool Admit(ui8 requiredVerbosity) {
        if (Verbosity_ < requiredVerbosity) {
            return false;
        }
        if (Remaining_ == 0) {
            ++Dropped_;
            return false;
        }
        --Remaining_;
        return true;
    }

    size_t Remaining() const {
        return Remaining_;
    }

    ui64 Dropped() const {
        return Dropped_;
    }

    void Drop(size_t count = 1) {
        Dropped_ += count;
    }

private:
    size_t Remaining_;
    ui8 Verbosity_;
    ui64 Dropped_ = 0;
};

TUserFacingQueryDescription DescribeUserFacingQuery(NKikimrKqp::EQueryType queryType,
    size_t statementCount, const NKqpProto::TKqpPhyQuery& physicalQuery,
    const TMaybe<TString>& commandTag);
TString ProtectUserFacingQueryText(const TString& text);
TString FallbackUserFacingQueryName(NKikimrKqp::EQueryType queryType,
    NKikimrKqp::EQueryAction queryAction);

} // namespace NKikimr::NKqp
