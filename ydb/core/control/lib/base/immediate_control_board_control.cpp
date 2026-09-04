#include "immediate_control_board_control.h"
#include <util/stream/str.h>
#include <util/system/guard.h>

namespace NKikimr {

TControl::TControl(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound)
    : Sequence(0)
      , Value(defaultValue)
      , Default(defaultValue)
      , Overridden(0)
      , LowerBound(lowerBound)
      , UpperBound(upperBound)
{}

void TControl::Set(TAtomicBase newValue) {
    TGuard<TAdaptiveLock> guard(WriteLock);
    AtomicIncrement(Sequence);
    AtomicSet(Value, newValue);
    AtomicSet(Default, newValue);
    AtomicSet(Overridden, 0);
    AtomicIncrement(Sequence);
}

void TControl::Reset(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound) {
    TGuard<TAdaptiveLock> guard(WriteLock);
    AtomicIncrement(Sequence);
    AtomicSet(Value, defaultValue);
    AtomicSet(Default, defaultValue);
    AtomicSet(Overridden, 0);
    LowerBound = lowerBound;
    UpperBound = upperBound;
    AtomicIncrement(Sequence);
}

void TControl::UpdateDefault(TAtomicBase newDefault) {
    TGuard<TAdaptiveLock> guard(WriteLock);
    AtomicIncrement(Sequence);
    const bool overridden = AtomicGet(Overridden);
    AtomicSet(Default, newDefault);
    if (!overridden) {
        AtomicSet(Value, newDefault);
    }
    AtomicIncrement(Sequence);
}

TControlMutation TControl::SetFromHtmlRequestWithState(TAtomicBase newValue) {
    TGuard<TAdaptiveLock> guard(WriteLock);
    AtomicIncrement(Sequence);
    const TControlState before = GetStateUnsafe();
    if (newValue != before.Default) {
        newValue = Max(newValue, LowerBound);
        newValue = Min(newValue, UpperBound);
    }
    if (newValue == before.Default) {
        AtomicSet(Value, before.Default);
        AtomicSet(Overridden, 0);
    } else {
        AtomicSet(Value, newValue);
        AtomicSet(Overridden, 1);
    }
    const TControlState after = GetStateUnsafe();
    AtomicIncrement(Sequence);
    return {before, after};
}

TAtomicBase TControl::SetFromHtmlRequest(TAtomicBase newValue) {
    return SetFromHtmlRequestWithState(newValue).Before.Value;
}

TAtomicBase TControl::Get() const {
    return AtomicGet(Value);
}

TAtomicBase TControl::GetDefault() const {
    return AtomicGet(Default);
}

TControlState TControl::GetState() const {
    for (;;) {
        const TAtomicBase sequence = AtomicGet(Sequence);
        if (sequence & 1) {
            continue;
        }
        const TControlState state = GetStateUnsafe();
        if (sequence == AtomicGet(Sequence)) {
            return state;
        }
    }
}

std::optional<TAtomicBase> TControl::GetOverride() const {
    const TControlState state = GetState();
    if (state.Overridden) {
        return state.Value;
    }
    return std::nullopt;
}

TControlMutation TControl::ClearOverride() {
    TGuard<TAdaptiveLock> guard(WriteLock);
    AtomicIncrement(Sequence);
    const TControlState before = GetStateUnsafe();
    AtomicSet(Value, before.Default);
    AtomicSet(Overridden, 0);
    const TControlState after = GetStateUnsafe();
    AtomicIncrement(Sequence);
    return {before, after};
}

void TControl::RestoreDefault() {
    ClearOverride();
}

bool TControl::HasOverride() const {
    return AtomicGet(Overridden);
}

bool TControl::IsDefault() const {
    return !HasOverride();
}

TString TControl::RangeAsString() const {
    TStringStream str;
    str << "[" << LowerBound << ", " << UpperBound << "]";
    return str.Str();
}

TControlState TControl::GetStateUnsafe() const {
    return {
        AtomicGet(Value),
        AtomicGet(Default),
        static_cast<bool>(AtomicGet(Overridden)),
    };
}

}
