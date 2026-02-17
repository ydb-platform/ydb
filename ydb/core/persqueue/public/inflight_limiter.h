#include <util/datetime/base.h>
#include <util/system/types.h>
#include <library/cpp/sliding_window/sliding_window.h>
#include <deque>

namespace NKikimr::NPQ {

// This class is used to control in-flight data.
// Contoller handles layout of data units, max units count is MAX_LAYOUT_COUNT constant.
// Layout is a deque of offsets
// Each item (associated with data unit) in the deque is max offset which intersects with this unit
// For example, if we have 3 units:
// Unit 1: [0, 100]
// Unit 2: [100, 200]
// Unit 3: [200, 300]
// Offsets are:
// 1 - Size 60
// 2 - Size 60
// 3 - Size 60
// 4 - Size 80
// Then the deque will be [2, 4, 4]
struct TInFlightController {
    constexpr static ui64 MAX_LAYOUT_COUNT = 1024;
    constexpr static TDuration SLIDING_WINDOW_SIZE = TDuration::Seconds(60);
    constexpr static ui64 SLIDING_WINDOW_UNITS_COUNT = 60;

    TInFlightController();
    TInFlightController(ui64 MaxAllowedSize);
    
    ui64 LayoutUnitSize = 0;
    std::deque<ui64> Layout;
    ui64 TotalSize = 0;
    ui64 MaxAllowedSize = 0;

    TInstant InFlightFullSince = TInstant::Zero();
    NSlidingWindow::TSlidingWindow<NSlidingWindow::TSumOperation<TDuration>> SlidingWindow;

    // Adds an offset with size
    bool Add(ui64 Offset, ui64 Size);
    // Removes offsets <= given offset
    bool Remove(ui64 Offset);
    bool IsMemoryLimitReached() const;

    TDuration GetLimitReachedDuration();
};

} // namespace NKikimr::NPQ
