#include "immediate_control_board_impl.h"

#include "immediate_control_board_html_renderer.h"

#include <library/cpp/iterator/enumerate.h>

#include <util/generic/string.h>

namespace NKikimr {

// TControlBoard

void TControlBoard::RestoreDefaults() {
    for (auto& [_, control]: GetAllAvailableControls()) {
        if (control) {
            control->RestoreDefault();
        }
    }
}

void TControlBoard::RenderAsHtml(TControlBoardTableHtmlRenderer& renderer) const {
    auto availableControls = GetAllAvailableControls();
    TMap<TString, TIntrusivePtr<TControl>> soredControls(availableControls.begin(), availableControls.end());
    for (const auto& [name, control]: soredControls) {
        if (control) {
            renderer.AddTableItem(name, control);
        }
    }
}

TIntrusivePtr<TControl> TControlBoard::GetControlByName(const TString& name) const {
    const auto allControls = GetAllAvailableControls();
    if (auto control = MapFindPtr(allControls, name)) {
        return *control;
    }
    return nullptr;
}

void TControlBoard::RegisterSharedControl(TControlWrapper& newControl, THotSwap<TControl>& icbControl) {
    auto oldControlValue = icbControl.AtomicLoad();
    if (oldControlValue) {
        newControl.Control = oldControlValue;
    }
    icbControl.AtomicStore(newControl.Control);
}

void TControlBoard::RegisterLocalControl(TControlWrapper control, THotSwap<TControl>& icbControl) {
    icbControl.AtomicStore(control.Control);
}

void TControlBoard::SetValue(TAtomicBase value, THotSwap<TControl>& icbControl) {
    if (auto control = icbControl.AtomicLoad()) {
        control->SetFromHtmlRequest(value);
    }
}

} // NKikimr
