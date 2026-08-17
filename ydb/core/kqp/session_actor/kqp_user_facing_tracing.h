#pragma once

#include <ydb/core/kqp/common/compilation/compile_diagnostics.h>
#include <ydb/core/kqp/common/kqp_execution_trace.h>
#include <ydb/core/protos/kqp.pb.h>
#include <ydb/library/actors/wilson/wilson_trace.h>

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

class TUserFacingTraceContext {
public:
    TUserFacingTraceContext(NWilson::TTraceId traceId, TInstant sessionStart,
        const google::protobuf::RepeatedPtrField<NKikimrKqp::TProxyRequestHop>& proxyHops);

    TUserFacingTraceContext(const TUserFacingTraceContext&) = delete;
    TUserFacingTraceContext& operator=(const TUserFacingTraceContext&) = delete;
    TUserFacingTraceContext(TUserFacingTraceContext&&) = default;
    TUserFacingTraceContext& operator=(TUserFacingTraceContext&&) = default;

    NWilson::TTraceId TraceId;
    TExecutionDiagnosticsPolicy DiagnosticsPolicy;
    TInstant SessionStart;
    TInstant ProxyRequestStart;
    std::vector<NKikimrKqp::TProxyRequestHop> ProxyHops;
    TInstant AdmissionStartedAt;
    bool ExecutionDelegated = false;
    TString RootName;
    TString Operation;
    std::vector<TCompileAttemptDiagnostic> CompileAttempts;
    std::optional<size_t> ActiveCompileAttempt;
    std::optional<TCompileAttemptDiagnostic> OverflowCompileAttempt;
    size_t CompileAttemptsDropped = 0;
    std::vector<TExecutionTraceSnapshot> ExecutionTraces;
    size_t ExecutionTracesDropped = 0;
};

TTimeWindow FitUserFacingRemoteWindow(TTimeWindow window, const TTimeWindow& parent);

class TUserFacingSpanBudget {
public:
    explicit TUserFacingSpanBudget(ui8 verbosity, size_t limit = MaxUserFacingSpansPerQuery,
            size_t reserved = 2)
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

struct TUserFacingQueryDescription {
    TString DisplayName;
    TString Operation;
};

TUserFacingQueryDescription DescribeUserFacingQuery(const TKqpQueryState& state);
TString SanitizeUserFacingQueryText(const TString& text);
TString FallbackUserFacingQueryName(const TKqpQueryState& state);

// Consumes the sampled context and detaches an immutable snapshot for asynchronous rendering.
NActors::IActor* CreateUserFacingTraceRenderer(TKqpQueryState& state, bool success,
    const TString& statusCode);

// Detaches a sampled request rejected before a per-query state can be created.
NActors::IActor* CreateRejectedUserFacingTraceRenderer(const NPrivateEvents::TEvQueryRequest& request,
    Ydb::StatusIds::StatusCode status);

// Derives the root name from the physical query rather than raw SQL text.
void UpdateUserFacingRootSpanName(TKqpQueryState& state);

} // namespace NKikimr::NKqp
