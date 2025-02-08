#include "static_control_board_impl.h"

#include "immediate_control_board_html_renderer.h"

#include <library/cpp/iterator/enumerate.h>

#include <util/generic/string.h>

namespace NKikimr {

// TStaticControlBoard

size_t TStaticControlBoard::GetIndex(EStaticControlType type) const {
    auto idx = static_cast<size_t>(type);
    Y_ENSURE(idx < GetEnumItemsCount<EStaticControlType>());
    return idx;
}

bool TStaticControlBoard::RegisterLocalControl(TControlWrapper control, EStaticControlType type) {
    auto& ourControl = StaticControls[GetIndex(type)];
    auto prevValue = ourControl.AtomicLoad();
    if (!prevValue) {
        ourControl.AtomicStore(control.Control);
        return true;
    }

    ourControl.AtomicStore(control.Control);
    return false;
}

bool TStaticControlBoard::RegisterSharedControl(TControlWrapper& control, EStaticControlType type) {
    auto& ourControl = StaticControls[GetIndex(type)];
    auto valueBefore = ourControl.AtomicLoad();
    if (valueBefore) {
        control.Control = valueBefore;
        return false;
    }
    ourControl.AtomicStore(control.Control);

    return true;
}

void TStaticControlBoard::RestoreDefault(EStaticControlType type) {
    if (auto control = StaticControls[GetIndex(type)].AtomicLoad()) {
        control->RestoreDefault();
    }
}

void TStaticControlBoard::RestoreDefaults() {
    for (auto& controlPtr: StaticControls) {
        if (auto control = controlPtr.AtomicLoad()) {
            control->RestoreDefault();
        }
    }
}

bool TStaticControlBoard::SetValue(EStaticControlType type, TAtomic value, TAtomic &outPrevValue) {
    if (auto control = StaticControls[GetIndex(type)].AtomicLoad()) {
        outPrevValue = control->SetFromHtmlRequest(value);
        return control->IsDefault();
    }
    return true;
}

// Only for tests
void TStaticControlBoard::GetValue(EStaticControlType type, TAtomic &outValue, bool &outIsControlExists) const {
    outIsControlExists = false;
    if (auto control = StaticControls[GetIndex(type)].AtomicLoad()) {
        outIsControlExists = true;
        outValue = control->Get();
    }
}

void TStaticControlBoard::RenderAsHtml(TControlBoardTableHtmlRenderer& renderer) const {
    for (const auto& [idx, controlPtr]: Enumerate(StaticControls)) {
        if (auto control = controlPtr.AtomicLoad()) {
            auto name = ToString(static_cast<EStaticControlType>(idx));
            renderer.AddTableItem(name, control);
        }
    }
}

TMaybe<EStaticControlType> TStaticControlBoard::GetStaticControlId(const TStringBuf name) const {
    EStaticControlType res;
    if (TryFromString<EStaticControlType>(name, res)) {
        return res;
    }
    return Nothing();
}

} // NKikimr
