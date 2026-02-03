#include "inflite_limiter.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ {

TInFlightMemoryController::TInFlightMemoryController(ui64 MaxAllowedSize)
    : LayoutUnitSize(MaxAllowedSize / MAX_LAYOUT_COUNT)
    , TotalSize(0)
    , MaxAllowedSize(MaxAllowedSize)
{
    if (MaxAllowedSize > 0 && LayoutUnitSize == 0) {
        LayoutUnitSize = 1;
    }
}

bool TInFlightMemoryController::Add(ui64 Offset, ui64 Size) {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return true;
    }

    AFL_ENSURE(Layout.empty() || Offset > Layout.back());

    if (TotalSize % LayoutUnitSize != 0) {
        AFL_ENSURE(!Layout.empty());
        Layout.back() = Offset;
    }

    auto unitsBefore = (TotalSize + LayoutUnitSize - 1) / LayoutUnitSize;
    TotalSize += Size;
    auto unitsAfter = (TotalSize + LayoutUnitSize - 1) / LayoutUnitSize;
    for (auto currentUnits = unitsBefore; currentUnits < unitsAfter; currentUnits++) {
        Layout.push_back(Offset);
    }

    return TotalSize < MaxAllowedSize;
}

bool TInFlightMemoryController::Remove(ui64 Offset) {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return true;
    }

    for (auto it = Layout.begin(); it != Layout.end(); it = Layout.erase(it)) {
        if (*it > Offset) {
            break;
        }

        if (Layout.size() == 1) {
            TotalSize = 0;
        } else {
            TotalSize -= LayoutUnitSize;
        }
    }

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