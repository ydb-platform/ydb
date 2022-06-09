#include "kqp_compute_actor.h"

#include <ydb/core/base/appdata.h>
#include <ydb/library/yql/core/issue/yql_issue.h>

namespace NKikimr::NKqp::NComputeActor {

static constexpr TDuration MIN_RETRY_DELAY = TDuration::MilliSeconds(250);
static constexpr TDuration MAX_RETRY_DELAY = TDuration::Seconds(2);

std::string_view EShardStateToString(EShardState state) {
    switch (state) {
        case EShardState::Initial: return "Initial"sv;
        case EShardState::Starting: return "Starting"sv;
        case EShardState::Running: return "Running"sv;
        case EShardState::Resolving: return "Resolving"sv;
        case EShardState::PostRunning: return "PostRunning"sv;
    }
}

void TShardState::ResetRetry() {
    RetryAttempt = 0;
    AllowInstantRetry = true;
    LastRetryDelay = {};
    if (RetryTimer) {
        TlsActivationContext->Send(new IEventHandle(RetryTimer, RetryTimer, new TEvents::TEvPoison));
        RetryTimer = {};
    }
    ResolveAttempt = 0;
}

TDuration TShardState::CalcRetryDelay() {
    if (std::exchange(AllowInstantRetry, false)) {
        return TDuration::Zero();
    }

    if (LastRetryDelay) {
        LastRetryDelay = Min(LastRetryDelay * 2, MAX_RETRY_DELAY);
    } else {
        LastRetryDelay = MIN_RETRY_DELAY;
    }
    return LastRetryDelay;
}

TString TShardState::ToString(TConstArrayRef<NScheme::TTypeId> keyTypes) const {
    TStringBuilder sb;
    sb << "TShardState{ TabletId: " << TabletId << ", State: " << EShardStateToString(State)
       << ", Gen: " << Generation << ", Ranges: [";
    for (size_t i = 0; i < Ranges.size(); ++i) {
        sb << "#" << i << ": " << DebugPrintRange(keyTypes, Ranges[i].ToTableRange(), *AppData()->TypeRegistry);
        if (i + 1 != Ranges.size()) {
            sb << ", ";
        }
    }
    sb << "], "
       << ", RetryAttempt: " << RetryAttempt << ", TotalRetries: " << TotalRetries
       << ", ResolveAttempt: " << ResolveAttempt << ", ActorId: " << ActorId << " }";
    return sb;
}

const TSmallVec<TSerializedTableRange> TShardState::GetScanRanges(TConstArrayRef<NScheme::TTypeId> keyTypes, const TOwnedCellVec& lastKey) const {
    // No any data read previously, return all ranges
    if (!lastKey.DataSize()) {
        return Ranges;
    }

    // Form new vector. Skip ranges already read.
    TVector<TSerializedTableRange> ranges;
    ranges.reserve(Ranges.size());

    YQL_ENSURE(keyTypes.size() == lastKey.size(), "Key columns size != last key");

    for (auto rangeIt = Ranges.begin(); rangeIt != Ranges.end(); ++rangeIt) {
        int cmp = ComparePointAndRange(lastKey, rangeIt->ToTableRange(), keyTypes, keyTypes);

        YQL_ENSURE(cmp >= 0, "Missed intersection of LastKey and range.");

        if (cmp > 0) {
            continue;
        }

        // It is range, where read was interrupted. Restart operation from last read key.
        ranges.emplace_back(std::move(TSerializedTableRange(
            TSerializedCellVec::Serialize(lastKey), rangeIt->To.GetBuffer(), false, rangeIt->ToInclusive
            )));

        // And push all others
        ranges.insert(ranges.end(), ++rangeIt, Ranges.end());
        break;
    }

    return ranges;
}


bool FindSchemeErrorInIssues(const Ydb::StatusIds::StatusCode& status, const NYql::TIssues& issues) {
    bool schemeError = false;
    if (status == Ydb::StatusIds::SCHEME_ERROR) {
        schemeError = true;
    } else if (status == Ydb::StatusIds::ABORTED) {
        for (auto& issue : issues) {
            WalkThroughIssues(issue, false, [&schemeError](const NYql::TIssue& x, ui16) {
                if (x.IssueCode == NYql::TIssuesIds::KIKIMR_SCHEME_MISMATCH) {
                    schemeError = true;
                }
            });
            if (schemeError) {
                break;
            }
        }
    }
    return schemeError;
}

} // namespace NKikimr::NKqp::NComputeActor
