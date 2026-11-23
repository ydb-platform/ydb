#pragma once

#include <util/generic/typelist.h>

#include <array>
#include <vector>

namespace NActors {

struct TAsyncFrameCacheTag;

// Only actor-system families belong here. Other tags register dynamically.
using TSystemAllocationCacheFamilies = TTypeList<TAsyncFrameCacheTag>;
inline constexpr size_t SystemAllocationCacheFamilyCount = TSystemAllocationCacheFamilies::Length;

template<class TTag, class TList = TSystemAllocationCacheFamilies>
constexpr size_t SystemAllocationCacheFamilyId() {
    if constexpr (TList::Length == 0) {
        return SystemAllocationCacheFamilyCount;
    } else if constexpr (std::is_same_v<TTag, typename TList::THead>) {
        return SystemAllocationCacheFamilyCount - TList::Length;
    } else {
        return SystemAllocationCacheFamilyId<TTag, typename TList::TTail>();
    }
}

// System slots live directly in the thread context; extension slots keep the
// existing dynamically sized table. Both use one family-id space for statistics.
struct TAllocationCachePointers {
    std::array<void*, SystemAllocationCacheFamilyCount> System{};
    std::vector<void*> Dynamic;

    size_t size() const noexcept {
        return SystemAllocationCacheFamilyCount + Dynamic.size();
    }

    void resize(size_t size) {
        Dynamic.resize(size > SystemAllocationCacheFamilyCount ? size - SystemAllocationCacheFamilyCount : 0);
    }

    void*& operator[](size_t family) noexcept {
        return family < SystemAllocationCacheFamilyCount
            ? System[family] : Dynamic[family - SystemAllocationCacheFamilyCount];
    }

    void* operator[](size_t family) const noexcept {
        return family < SystemAllocationCacheFamilyCount
            ? System[family] : Dynamic[family - SystemAllocationCacheFamilyCount];
    }
};

} // namespace NActors
