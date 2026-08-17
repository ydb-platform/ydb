#pragma once

#include <ydb/core/kqp/common/kqp_execution_trace.h>

#include <util/generic/string.h>

namespace NActors {
class IActor;
}

namespace NKikimr::NKqp {

class TKqpQueryState;
namespace NPrivateEvents {
struct TEvQueryRequest;
}

constexpr size_t MaxUserFacingSpansPerQuery = 1000;

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

TString DescribeUserFacingQuery(const TKqpQueryState& state);
TString SanitizeUserFacingQueryText(const TString& text);
TString FallbackUserFacingQueryName(const TKqpQueryState& state);

void InitializeUserFacingQueryText(TKqpQueryState& state);

// Consumes the sampled context and detaches an immutable snapshot for asynchronous rendering.
NActors::IActor* CreateUserFacingTraceRenderer(TKqpQueryState& state, bool success,
    const TString& statusCode);

// Finishes a sampled request rejected before a per-query state can be created.
void FinishRejectedUserFacingSpan(const NPrivateEvents::TEvQueryRequest& request,
    Ydb::StatusIds::StatusCode status);

// Derives the root name from the physical query rather than raw SQL text.
void UpdateUserFacingRootSpanName(TKqpQueryState& state);

} // namespace NKikimr::NKqp
