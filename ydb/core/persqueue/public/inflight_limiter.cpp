#include "inflight_limiter.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr::NPQ {

TInFlightController::TInFlightController()
    : LayoutUnitSize(1)
    , TotalSize(0)
    , MaxAllowedSize(MAX_LAYOUT_COUNT)
    , SlidingWindow(SLIDING_WINDOW_SIZE, SLIDING_WINDOW_UNITS_COUNT)
{
}

TInFlightController::TInFlightController(ui64 MaxAllowedSize)
    : LayoutUnitSize(MaxAllowedSize / MAX_LAYOUT_COUNT)
    , TotalSize(0)
    , MaxAllowedSize(MaxAllowedSize)
    , SlidingWindow(SLIDING_WINDOW_SIZE, SLIDING_WINDOW_UNITS_COUNT)
{
    if (MaxAllowedSize > 0 && LayoutUnitSize == 0) {
        LayoutUnitSize = 1;
    }
}

bool TInFlightController::Add(ui64 Offset, ui64 Size) {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return true;
    }

    auto wasMemoryLimitReached = IsMemoryLimitReached();
    if (Size == 0) {
        return !wasMemoryLimitReached;
    }

    AFL_ENSURE(Layout.empty() || Offset > Layout.back());
    if (TotalSize % LayoutUnitSize != 0) {
        AFL_ENSURE(!Layout.empty());
        Layout.back() = Offset;
    }

    auto unitsBefore = (TotalSize + LayoutUnitSize - 1) / LayoutUnitSize;
    TotalSize += Size;
    auto unitsAfter = std::min(MAX_LAYOUT_COUNT, (TotalSize + LayoutUnitSize - 1) / LayoutUnitSize);
    for (auto currentUnits = unitsBefore; currentUnits < unitsAfter; currentUnits++) {
        Layout.push_back(Offset);
    }
    
    AFL_ENSURE(!Layout.empty());
    Layout.back() = Offset;

    bool isMemoryLimitReached = IsMemoryLimitReached();
    if (!wasMemoryLimitReached && isMemoryLimitReached && InFlightFullSince == TInstant::Zero()) {
        InFlightFullSince = TInstant::Now();
    }

    return !isMemoryLimitReached;
}

bool TInFlightController::Remove(ui64 Offset) {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return true;
    }

    auto wasMemoryLimitReached = IsMemoryLimitReached();
    for (auto it = Layout.begin(); it != Layout.end(); it = Layout.erase(it)) {
        if (*it >= Offset) {
            break;
        }

        if (Layout.size() == 1) {
            TotalSize = 0;
        } else {
            AFL_ENSURE(TotalSize >= LayoutUnitSize);
            TotalSize -= LayoutUnitSize;
        }
    }

    auto isMemoryLimitReached = IsMemoryLimitReached();
    if (wasMemoryLimitReached && !isMemoryLimitReached && InFlightFullSince != TInstant::Zero()) {
        auto now = TInstant::Now();
        SlidingWindow.Update(now - InFlightFullSince, now);
        InFlightFullSince = TInstant::Zero();
    }

    return !isMemoryLimitReached;
}

bool TInFlightController::IsMemoryLimitReached() const {
    if (MaxAllowedSize == 0) {
        // means that there are no limits were set
        return false;
    }

    return TotalSize >= MaxAllowedSize;
}

TDuration TInFlightController::GetLimitReachedDuration() {
    TDuration carry = TDuration::Zero();
    if (InFlightFullSince != TInstant::Zero()) {
        carry = TInstant::Now() - InFlightFullSince;
    }

    return Min(SLIDING_WINDOW_SIZE, SlidingWindow.GetValue() + carry);
}

} // namespace NKikimr::NPQ