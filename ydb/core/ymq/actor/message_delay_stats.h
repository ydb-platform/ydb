#pragma once
#include "defs.h"

#include <util/datetime/base.h>

#include <vector>

namespace NKikimr::NSQS {

class TMessageDelayStatistics {
public:
    TMessageDelayStatistics() = default;

    size_t UpdateAndGetMessagesDelayed(TInstant now);

    void AddDelayedMessage(TInstant delayDeadline, TInstant now);

private:
    void AdvanceTime(TInstant now);

private:
    std::vector<size_t> Buckets;
    size_t FirstBucket = 0;
    TInstant Start = {};
};

} // namespace NKikimr::NSQS
