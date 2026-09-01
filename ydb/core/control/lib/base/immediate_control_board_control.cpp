#include "immediate_control_board_control.h"
#include <util/stream/str.h>
#include <util/system/guard.h>

namespace NKikimr {

TControl::TControl(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound)
    : Value(defaultValue)
      , Default(defaultValue)
      , LowerBound(lowerBound)
      , UpperBound(upperBound)
      , Overridden(0)
      , Sequence(0)
{}

void TControl::Set(TAtomicBase newValue) {
    TGuard<TAdaptiveLock> guard(StateLock);
    AtomicIncrement(Sequence);
    AtomicSet(Value, newValue);
    AtomicSet(Default, newValue);
    AtomicSet(Overridden, 0);
    AtomicIncrement(Sequence);
}

void TControl::Reset(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound) {
    TGuard<TAdaptiveLock> guard(StateLock);
    AtomicIncrement(Sequence);
    AtomicSet(Value, defaultValue);
    AtomicSet(Default, defaultValue);
    AtomicSet(Overridden, 0);
    LowerBound = lowerBound;
    UpperBound = upperBound;
    AtomicIncrement(Sequence);
}

void TControl::UpdateDefault(TAtomicBase newDefault) {
    TGuard<TAdaptiveLock> guard(StateLock);
    AtomicIncrement(Sequence);
    const bool overridden = AtomicGet(Overridden);
    AtomicSet(Default, newDefault);
    if (!overridden) {
        AtomicSet(Value, newDefault);
    }
    AtomicIncrement(Sequence);
}

TControlMutation TControl::SetOverride(TAtomicBase newValue) {
    TGuard<TAdaptiveLock> guard(StateLock);
    AtomicIncrement(Sequence);
    const TControlState before = GetStateUnsafe();
    newValue = Max(newValue, LowerBound);
    newValue = Min(newValue, UpperBound);
    AtomicSet(Value, newValue);
    AtomicSet(Overridden, 1);
    const TControlState after = GetStateUnsafe();
    AtomicIncrement(Sequence);
    return {before, after};
}

TControlMutation TControl::SetFromHtmlRequestWithState(TAtomicBase newValue) {
    TGuard<TAdaptiveLock> guard(StateLock);
    AtomicIncrement(Sequence);
    const TControlState before = GetStateUnsafe();
    if (newValue != before.Default) {
        newValue = Max(newValue, LowerBound);
        newValue = Min(newValue, UpperBound);
    }
    AtomicSet(Value, newValue);
    AtomicSet(Overridden, 1);
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

// Return a coherent control snapshot, using Sequence to validate one
// optimistic sample before falling back to StateLock.
TControlState TControl::GetState() const {
    // Accept the sample only if no writer was active or completed during it.
    const TAtomicBase sequence = AtomicGet(Sequence);
    if (!(sequence & 1)) {
        const TControlState state = GetStateUnsafe();
        if (sequence == AtomicGet(Sequence)) {
            return state;
        }
    }

    // Serialize with a writer after an overlapping mutation.
    TGuard<TAdaptiveLock> guard(StateLock);
    return GetStateUnsafe();
}

// Return the active override from a coherent complete state snapshot.
std::optional<TAtomicBase> TControl::GetOverride() const {
    const TControlState state = GetState();
    return state.Overridden
        ? std::optional<TAtomicBase>(state.Value)
        : std::nullopt;
}

TControlMutation TControl::RestoreDefault() {
    TGuard<TAdaptiveLock> guard(StateLock);
    AtomicIncrement(Sequence);
    const TControlState before = GetStateUnsafe();
    AtomicSet(Value, before.Default);
    AtomicSet(Overridden, 0);
    const TControlState after = GetStateUnsafe();
    AtomicIncrement(Sequence);
    return {before, after};
}

bool TControl::IsDefault() const {
    return !AtomicGet(Overridden);
}

TString TControl::RangeAsString() const {
    TStringStream str;
    str << "[" << LowerBound << ", " << UpperBound << "]";
    return str.Str();
}

// Read one unvalidated state sample; callers must hold StateLock or verify
// that Sequence stayed even and unchanged.
TControlState TControl::GetStateUnsafe() const {
    return {
        AtomicGet(Value),
        AtomicGet(Default),
        static_cast<bool>(AtomicGet(Overridden)),
    };
}

}
