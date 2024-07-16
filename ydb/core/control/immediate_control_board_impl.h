#pragma once

#include "immediate_control_board_control.h"
#include "immediate_control_board_wrapper.h"
#include <optional>
#include <string_view>
#include <util/generic/ptr.h>
#include <util/system/mutex.h>

namespace NKikimr {

class TControlBoard : public TThrRefBase {
public:
    bool RegisterLocalControl(TControlWrapper control, TString name);

    bool RegisterSharedControl(TControlWrapper& control, TString name);

    void RestoreDefaults();

    void RestoreDefault(TString name);

    bool SetValue(TString name, TAtomic value, TAtomic &outPrevValue);

    // Only for tests
    void GetValue(TString name, TAtomic &outValue, bool &outIsControlExists) const;

    TString RenderAsHtml() const;

private:
    ui8 GetIndex(const std::string_view s) const noexcept;

    constexpr static ui8 BoardSize = 128;
    std::array<TIntrusivePtr<TControl>, BoardSize> Board;
    std::array<TMutex, BoardSize> BoardLocks;
};

}
