#include "reattach.h"

#include <ydb/core/base/appdata_fwd.h>
#include <library/cpp/random_provider/random_provider.h>

namespace NKikimr::NKqp {

namespace {
    static constexpr TDuration MinReattachDelay = TDuration::MilliSeconds(10);
    static constexpr TDuration MaxReattachDelay = TDuration::MilliSeconds(100);
    static constexpr TDuration MaxReattachDuration = TDuration::Seconds(4);
}

bool ShouldReattach(TInstant now, TReattachInfo& reattachInfo) {
    if (!reattachInfo.Reattaching) {
        reattachInfo.Deadline = now + MaxReattachDuration;
        reattachInfo.Delay = TDuration::Zero();
        reattachInfo.Reattaching = true;
        return true;
    }

    TDuration left = reattachInfo.Deadline - now;
    if (!left) {
        reattachInfo.Reattaching = false;
        return false;
    }

    reattachInfo.Delay *= 2.0;
    if (reattachInfo.Delay < MinReattachDelay) {
        reattachInfo.Delay = MinReattachDelay;
    } else if (reattachInfo.Delay > MaxReattachDelay) {
        reattachInfo.Delay = MaxReattachDelay;
    }

    // Add ±10% jitter
    reattachInfo.Delay *= 0.9 + 0.2 * TAppData::RandomProvider->GenRandReal4();
    if (reattachInfo.Delay > left) {
        reattachInfo.Delay = left;
    }

    return true;
}

}
