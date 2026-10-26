#pragma once

#include "storage.h"

namespace NKikimr::NOlap::NBlobOperations {

// Larger value is taken first. The same value stays FIFO.
// Scans are first, compaction and the other readers stay in the middle, prefetch is last.
inline ui32 GetCacheRequestPriority(const EConsumer consumer) {
    if (consumer == EConsumer::SCAN || consumer == EConsumer::SYS_VIEW_SCAN) {
        return 2;
    }
    if (consumer == EConsumer::FETCH_ON_LOAD) {
        return 0;
    }
    return 1;
}

}   // namespace NKikimr::NOlap::NBlobOperations
