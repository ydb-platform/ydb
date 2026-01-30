#include <util/system/types.h>
#include <deque>

namespace NKikimr::NPQ {

struct TInFlightMemoryController {
    constexpr static ui64 MAX_LAYOUT_COUNT = 1024;

    TInFlightMemoryController() = default;
    TInFlightMemoryController(ui64 MaxAllowedSize);
    
    ui64 LayoutUnit = 0;
    std::deque<ui64> Layout;
    ui64 TotalSize = 0;
    ui64 MaxAllowedSize = 0;

    bool Add(ui64 Offset, ui64 Size);
    bool Remove(ui64 Offset);
    bool IsMemoryLimitReached() const;
};

} // namespace NKikimr::NPQ
