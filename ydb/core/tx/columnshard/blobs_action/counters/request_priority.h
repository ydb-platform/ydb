#pragma once

#include "storage.h"

namespace NKikimr::NOlap::NBlobOperations {

inline ui32 GetCacheRequestPriority(const EConsumer consumer) {
    enum class EPriority: ui32 {
        Prefetch = 0,
        Normal = 1,
        Scan = 2,
    };
    if (consumer == EConsumer::SCAN || consumer == EConsumer::SYS_VIEW_SCAN) {
        return static_cast<ui32>(EPriority::Scan);
    }
    if (consumer == EConsumer::FETCH_ON_LOAD) {
        return static_cast<ui32>(EPriority::Prefetch);
    }
    return static_cast<ui32>(EPriority::Normal);
}

}   // namespace NKikimr::NOlap::NBlobOperations
