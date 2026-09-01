#include "dynamic_control_board_impl.h"

#include "immediate_control_board_html_renderer.h"

#include <util/generic/string.h>

namespace NKikimr {
// TControlBoard

bool TDynamicControlBoard::RegisterLocalControl(TControlWrapper control, TString name) {
    TIntrusivePtr<TControl> ptr;
    bool result = Board.Swap(name, control.Control, ptr);
    return !result;
}

bool TDynamicControlBoard::RegisterSharedControl(TControlWrapper& control, TString name) {
    TIntrusivePtr<TControl> ptr;
    if (Board.Get(name, ptr)) {
        control.Control = ptr;
        return false;
    }
    ptr = Board.InsertIfAbsent(name, control.Control);
    if (control.Control == ptr) {
        return true;
    } else {
        control.Control = ptr;
        return false;
    }
}

void TDynamicControlBoard::RestoreDefaults() {
    for (auto& bucket : Board.Buckets) {
        TReadGuard guard(bucket.GetLock());
        for (auto& control : bucket.GetMap()) {
            control.second->RestoreDefault();
        }
    }
}

void TDynamicControlBoard::RestoreDefault(TString name) {
    TIntrusivePtr<TControl> control;
    if (Board.Get(name, control)) {
        control->RestoreDefault();
    }
}

bool TDynamicControlBoard::SetValue(TString name, TAtomic value, TAtomic &outPrevValue) {
    TControlMutation mutation;
    if (SetValue(std::move(name), value, mutation)) {
        outPrevValue = mutation.Before.Value;
        return !mutation.After.Overridden;
    }
    return true;
}

bool TDynamicControlBoard::SetValue(TString name, TAtomic value, TControlMutation& outMutation) {
    TIntrusivePtr<TControl> control;
    if (Board.Get(name, control)) {
        outMutation = control->SetFromHtmlRequestWithState(value);
        return true;
    }
    return false;
}

bool TDynamicControlBoard::ClearOverride(TString name, TControlMutation& outMutation) {
    TIntrusivePtr<TControl> control;
    if (Board.Get(name, control)) {
        outMutation = control->ClearOverride();
        return true;
    }
    return false;
}

ui64 TDynamicControlBoard::GetOverriddenCount() const {
    ui64 count = 0;
    for (const auto& bucket : Board.Buckets) {
        TReadGuard guard(bucket.GetLock());
        for (const auto& [_, control] : bucket.GetMap()) {
            count += control->HasOverride();
        }
    }
    return count;
}

// Only for tests
void TDynamicControlBoard::GetValue(TString name, TAtomic &outValue, bool &outIsControlExists) const {
    TIntrusivePtr<TControl> control;
    outIsControlExists = Board.Get(name, control);
    if (outIsControlExists) {
        outValue = control->Get();
    }
}

void TDynamicControlBoard::RenderAsHtml(TControlBoardTableHtmlRenderer& renderer) const {
    for (const auto& bucket : Board.Buckets) {
        TReadGuard guard(bucket.GetLock());
        for (const auto &item : bucket.GetMap()) {
            renderer.AddTableItem(item.first, item.second);
        }
    }
}

}
