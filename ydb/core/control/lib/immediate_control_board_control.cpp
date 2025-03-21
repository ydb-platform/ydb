#include "immediate_control_board_control.h"
#include <util/stream/str.h>

namespace NKikimr {

TControl::TControl(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound)
    : Value(defaultValue)
      , Default(defaultValue)
      , LowerBound(lowerBound)
      , UpperBound(upperBound)
{}

void TControl::Set(TAtomicBase newValue) {
    AtomicSet(Value, newValue);
    AtomicSet(Default, newValue);
}

void TControl::Reset(TAtomicBase defaultValue, TAtomicBase lowerBound, TAtomicBase upperBound) {
    Value = defaultValue;
    Default = defaultValue;
    LowerBound = lowerBound;
    UpperBound = upperBound;
}

TAtomicBase TControl::SetFromHtmlRequest(TAtomicBase newValue) {
    TAtomicBase prevValue = AtomicGet(Value);
    if (newValue == AtomicGet(Default)) {
        AtomicSet(Value, newValue);
    } else {
        newValue = Max(newValue, LowerBound);
        newValue = Min(newValue, UpperBound);
        AtomicSet(Value, newValue);
    }
    return prevValue;
}

TAtomicBase TControl::Get() const {
    return AtomicGet(Value);
}

TAtomicBase TControl::GetDefault() const {
    return AtomicGet(Default);
}

void TControl::RestoreDefault() {
    AtomicSet(Value, Default);
}

bool TControl::IsDefault() const {
    return AtomicGet(Value) == AtomicGet(Default);
}

TString TControl::RangeAsString() const {
    TStringStream str;
    str << "[" << LowerBound << ", " << UpperBound << "]";
    return str.Str();
}

}
