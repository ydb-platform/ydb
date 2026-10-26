#pragma once

#include "storage.h"

namespace NKikimr::NOlap::NBlobOperations {

// Larger value is taken first. The same value stays FIFO.
struct TCacheRequestPriority {
    static constexpr ui32 RequestPriorityHigh = 2;
    static constexpr ui32 RequestPriorityNormal = 1;
    static constexpr ui32 RequestPriorityLow = 0;

    static ui32 GetRequestPriority(const EConsumer consumer) {
        if (consumer == EConsumer::SCAN || consumer == EConsumer::SYS_VIEW_SCAN) {
            return RequestPriorityHigh;
        }
        if (consumer == EConsumer::FETCH_ON_LOAD) {
            return RequestPriorityLow;
        }
        return RequestPriorityNormal;
    }
};

}   // namespace NKikimr::NOlap::NBlobOperations
