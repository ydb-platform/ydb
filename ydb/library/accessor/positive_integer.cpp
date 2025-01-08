#include "positive_integer.h"
#include <ydb/library/actors/core/log.h>

namespace NKikimr {

TPositiveControlInteger::TPositiveControlInteger(const i64 value)
    : Value(value) {
    AFL_VERIFY(0 <= value);
}

ui64 TPositiveControlInteger::Add(const ui64 value) {
    Value += value;
    return Value;
}

ui64 TPositiveControlInteger::Sub(const ui64 value) {
    if (value <= Value) {
        Value -= value;
    } else {
        AFL_VERIFY(false)("base", Value)("delta", value);
    }
    return Value;
}

ui64 TPositiveControlInteger::GetDec() const {
    if (Value) {
        return Value - 1;
    } else {
        AFL_VERIFY(false);
    }
    return 0;
}

ui64 TPositiveControlInteger::Val() const {
    return Value;
}

}