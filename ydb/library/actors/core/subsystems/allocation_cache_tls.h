#pragma once

#include <ydb/library/actors/core/subsystems/allocation_cache.h>
#include <ydb/library/actors/core/thread_context.h>

namespace NActors {

template<class TTag>
TAllocationCache<TTag>* TAllocationCache<TTag>::GetCurrent() noexcept {
    auto* context = TlsThreadContext;
    if (!context) {
        return nullptr;
    }
    if constexpr (SystemAllocationCacheFamilyId<TTag>() < SystemAllocationCacheFamilyCount) {
        return static_cast<TAllocationCache<TTag>*>(
            context->AllocationCachePointers.System[SystemAllocationCacheFamilyId<TTag>()]);
    }
    const size_t family = TAllocationCacheFamily<TTag>::FamilyId();
    return family < context->AllocationCachePointers.size()
        ? static_cast<TAllocationCache<TTag>*>(context->AllocationCachePointers[family])
        : nullptr;
}

} // namespace NActors
