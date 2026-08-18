#pragma once

#include <ydb/core/kqp/common/compilation/compile_diagnostics.h>
#include <ydb/core/kqp/common/kqp_execution_trace.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

#include "kqp_query_stats.h"

#include <util/generic/string.h>

#include <optional>
#include <vector>

namespace NActors {
class IActor;
}

namespace NKikimr::NKqp {

class TKqpQueryState;
namespace NPrivateEvents {
struct TEvQueryRequest;
}

constexpr size_t MaxUserFacingSpansPerQuery = 1000;

TInstant MapUserFacingSessionStart(TInstant localStart, TInstant originSentAt,
    const google::protobuf::RepeatedPtrField<NKikimrKqp::TProxyRequestHop>& proxyHops);

struct TUserFacingQueryDescription {
    TString DisplayName;
    TString Operation;
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
    TUserFacingQuerySnapshot DetachSnapshot(TKqpQueryState& state, bool success,
        const TString& statusCode, NKikimrKqp::TEvQueryResponse* response);

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
    TKqpQueryStats QueryStats;
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

NActors::IActor* CreateUserFacingTraceRendererActor(TUserFacingQuerySnapshot snapshot);
NActors::IActor* CreateRejectedUserFacingTraceRendererActor(
    TRejectedUserFacingQuerySnapshot snapshot);

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

TUserFacingQueryDescription DescribeUserFacingQuery(const TKqpQueryState& state);
TString SanitizeUserFacingQueryText(const TString& text);
TString FallbackUserFacingQueryName(const TKqpQueryState& state);

// Consumes the sampled context and detaches an immutable snapshot for asynchronous rendering.
NActors::IActor* CreateUserFacingTraceRenderer(TKqpQueryState& state, bool success,
    const TString& statusCode, NKikimrKqp::TEvQueryResponse* response = nullptr);

// Detaches a sampled request rejected before a per-query state can be created.
NActors::IActor* CreateRejectedUserFacingTraceRenderer(const NPrivateEvents::TEvQueryRequest& request,
    Ydb::StatusIds::StatusCode status);

// Derives the root name from the physical query rather than raw SQL text.
void UpdateUserFacingRootSpanName(TKqpQueryState& state);

} // namespace NKikimr::NKqp
