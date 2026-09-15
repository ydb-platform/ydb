#pragma once

#include <library/cpp/sliding_window/sliding_window.h>

#include <util/datetime/base.h>
#include <util/system/types.h>

#include <deque>

namespace NKikimr::NPQ {

// Tracks in-flight read data volume against MaxAllowedSize.
// Layout is a deque of at most MAX_LAYOUT_COUNT buckets (see LayoutUnitSize).
// Each bucket is TUnit{Offset, Size}:
//   Offset — max message offset charged to this bucket;
//   Size   — bytes attributed to this bucket (normally up to LayoutUnitSize; the last bucket may grow when Layout is full).
// TotalSize is the sum of all in-flight bytes; Remove(Offset) drops buckets with Offset < Offset.
//
// Example (LayoutUnitSize = 60): Add(1, 60), Add(2, 60), Add(3, 60), Add(4, 80)
//   -> Layout = [{1, 60}, {2, 60}, {3, 60}, {4, 60}, {4, 20}], TotalSize = 260
struct TInFlightController {
    constexpr static ui64 MAX_LAYOUT_COUNT = 1024;
    constexpr static TDuration SLIDING_WINDOW_SIZE = TDuration::Seconds(60);
    constexpr static ui64 SLIDING_WINDOW_UNITS_COUNT = 60;

    TInFlightController();
    TInFlightController(ui64 MaxAllowedSize);

    struct TUnit {
        ui64 Offset;
        ui64 Size;
    };

    ui64 LayoutUnitSize = 0;
    std::deque<TUnit> Layout;
    ui64 TotalSize = 0;
    ui64 MaxAllowedSize = 0;

    TInstant InFlightFullSince = TInstant::Zero();
    NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<TDuration>> SlidingWindow;

    // Adds an offset with size
    bool Add(ui64 Offset, ui64 Size);
    // Drops layout buckets with offset < Offset (committed/read past this offset).
    bool Remove(ui64 Offset);
    bool IsMemoryLimitReached() const;

    TDuration GetLimitReachedDuration();
};

} // namespace NKikimr::NPQ
