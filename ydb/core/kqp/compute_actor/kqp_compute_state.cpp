#include "kqp_compute_state.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/kqp/runtime/kqp_compute.h>
#include <ydb/core/kqp/runtime/kqp_read_table.h>
#include <ydb/core/tx/datashard/range_ops.h>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NKqp::NComputeActor {

static constexpr TDuration MIN_RETRY_DELAY = TDuration::MilliSeconds(250);
static constexpr TDuration MAX_RETRY_DELAY = TDuration::Seconds(2);

void TCommonRetriesState::ResetRetry() {
    RetryAttempt = 0;
    AllowInstantRetry = true;
    LastRetryDelay = {};
    if (RetryTimer) {
        TlsActivationContext->Send(new IEventHandle(RetryTimer, RetryTimer, new TEvents::TEvPoison));
        RetryTimer = {};
    }
    ResolveAttempt = 0;
}

TString TShardState::PrintLastKey(TConstArrayRef<NScheme::TTypeInfo> keyTypes) const {
    if (LastKey.empty()) {
        return "<none>";
    }
    return DebugPrintPoint(keyTypes, LastKey, *AppData()->TypeRegistry);
}

TDuration TCommonRetriesState::CalcRetryDelay() {
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

TString TShardState::ToString(TConstArrayRef<NScheme::TTypeInfo> keyTypes) const {
    TStringBuilder sb;
    sb << "TShardState{ TabletId: " << TabletId << ", State: " << State
        << ", Gen: " << Generation << ", Last Key " << TShardState::PrintLastKey(keyTypes)
        << ", Ranges: [";
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

const TSmallVec<TSerializedTableRange> TShardState::GetScanRanges(TConstArrayRef<NScheme::TTypeInfo> keyTypes) const {
    // No any data read previously, return all ranges
    if (!LastKey.DataSize()) {
        return Ranges;
    }

    // Form new vector. Skip ranges already read.
    TVector<TSerializedTableRange> ranges;
    ranges.reserve(Ranges.size());

    YQL_ENSURE(keyTypes.size() == LastKey.size(), "Key columns size != last key");

    for (auto rangeIt = Ranges.begin(); rangeIt != Ranges.end(); ++rangeIt) {
        int cmp = ComparePointAndRange(LastKey, rangeIt->ToTableRange(), keyTypes, keyTypes);

        YQL_ENSURE(cmp >= 0, "Missed intersection of LastKey and range.");

        if (cmp > 0) {
            continue;
        }

        // It is range, where read was interrupted. Restart operation from last read key.
        ranges.emplace_back(std::move(TSerializedTableRange(
            TSerializedCellVec::Serialize(LastKey), rangeIt->To.GetBuffer(), false, rangeIt->ToInclusive
        )));

        // And push all others
        ranges.insert(ranges.end(), ++rangeIt, Ranges.end());
        break;
    }

    return ranges;
}

TString TShardState::GetAddress() const {
    TStringBuilder sb;
    sb << TabletId;
    return sb;
}

TShardState::TShardState(const ui64 tabletId)
    : TabletId(tabletId)
{
    AFL_ENSURE(TabletId);
}

}
