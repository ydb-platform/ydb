#include "inflight_limiter.h"

#include <ydb/core/base/appdata_fwd.h>
#include <ydb/library/actors/core/log.h>

#include <library/cpp/time_provider/time_provider.h>

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

    AFL_ENSURE(Layout.empty() || Offset > Layout.back().Offset)
        ("reason", "offset must be monotonic")("offset", Offset)
        ("layout_back_offset", Layout.empty() ? 0 : Layout.back().Offset)("layout_size", Layout.size());

    ui64 rest = Size;
    if (!Layout.empty() && Layout.back().Size < LayoutUnitSize) {
        const ui64 toCurrentUnit = std::min(rest, LayoutUnitSize - Layout.back().Size);
        Layout.back().Offset = Offset;
        Layout.back().Size += toCurrentUnit;
        rest -= toCurrentUnit;
    }

    while (rest > 0) {
        if (Layout.size() < MAX_LAYOUT_COUNT) {
            const ui64 unitSize = std::min(rest, LayoutUnitSize);
            Layout.emplace_back(Offset, unitSize);
            rest -= unitSize;
        } else {
            Layout.back().Offset = Offset;
            Layout.back().Size += rest;
            rest = 0;
        }
    }

    TotalSize += Size;

    bool isMemoryLimitReached = IsMemoryLimitReached();
    if (!wasMemoryLimitReached && isMemoryLimitReached && InFlightFullSince == TInstant::Zero()) {
        InFlightFullSince = TAppData::TimeProvider->Now();
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
        if (it->Offset >= Offset) {
            break;
        }

        if (Layout.size() == 1) {
            TotalSize = 0;
        } else {
            AFL_ENSURE(TotalSize >= it->Size)
                ("reason", "total size must cover removed entry")("total_size", TotalSize)("entry_size", it->Size)("offset", Offset);
            TotalSize -= it->Size;
        }
    }

    auto isMemoryLimitReached = IsMemoryLimitReached();
    if (wasMemoryLimitReached && !isMemoryLimitReached && InFlightFullSince != TInstant::Zero()) {
        auto now = TAppData::TimeProvider->Now();
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
        carry = TAppData::TimeProvider->Now() - InFlightFullSince;
    }

    return Min(SLIDING_WINDOW_SIZE, SlidingWindow.GetValue() + carry);
}

} // namespace NKikimr::NPQ
