#pragma once

#include <ydb/core/kqp/common/kqp_user_facing_trace_data.h>

#include <util/generic/string.h>

namespace NKikimr::NKqp {

class TKqpQueryState;

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

    ui64 TaskDurationCutoffMs = 0;

private:
    size_t Remaining_;
    ui8 Verbosity_;
    ui64 Dropped_ = 0;
};

TString DescribeUserFacingQuery(const TKqpQueryState& state);
TString SanitizeUserFacingQueryText(const TString& text);
TString FallbackUserFacingQueryName(const TKqpQueryState& state);

void InitializeUserFacingQueryText(TKqpQueryState& state);

// Consumes the sampled trace context and renders the finished query.
void FinishUserFacingSpan(TKqpQueryState& state, bool success, const TString& statusCode);

// Derives the root name from the physical query rather than raw SQL text.
void UpdateUserFacingRootSpanName(TKqpQueryState& state);

} // namespace NKikimr::NKqp
