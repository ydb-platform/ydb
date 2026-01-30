#include "inflite_limiter.h"
#include <algorithm>
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ {

TInFlightMemoryController::TInFlightMemoryController(ui64 MaxAllowedSize)
    : LayoutUnit(MaxAllowedSize / MAX_LAYOUT_COUNT)
    , TotalSize(0)
    , MaxAllowedSize(MaxAllowedSize)
{
    if (MaxAllowedSize > 0 && LayoutUnit == 0) {
        LayoutUnit = 1;
    }
}

bool TInFlightMemoryController::Add(ui64 Offset, ui64 Size) {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return true;
    }

    AFL_ENSURE(Layout.empty() || Offset > Layout.back());

    auto unitsBefore = (TotalSize + LayoutUnit - 1) / LayoutUnit;
    TotalSize += Size;
    auto unitsAfter = (TotalSize + LayoutUnit - 1) / LayoutUnit;
    for (auto currentUnits = unitsBefore; currentUnits < unitsAfter; currentUnits++) {
        Layout.push_back(Offset);
    }

    return TotalSize < MaxAllowedSize;
}

bool TInFlightMemoryController::Remove(ui64 Offset) {
    if (MaxAllowedSize == 0) {
        return true;
    }

    auto it = std::upper_bound(Layout.begin(), Layout.end(), Offset);
    auto toRemove = it - Layout.begin();

    TotalSize -= toRemove * LayoutUnit;
    Layout.erase(Layout.begin(), it);
    return TotalSize < MaxAllowedSize;
}

bool TInFlightMemoryController::IsMemoryLimitReached() const {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return false;
    }

    return TotalSize >= MaxAllowedSize;
}

} // namespace NKikimr::NPQ