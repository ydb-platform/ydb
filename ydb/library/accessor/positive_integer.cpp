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
    AFL_VERIFY(value <= Value)("base", Value)("delta", value);
    Value -= value;
    return Value;
}

ui64 TPositiveControlInteger::Val() const {
    return Value;
}

TPositiveControlInteger TPositiveControlInteger::operator-(const ui64 val) const {
    AFL_VERIFY(val <= Value);
    return Value - val;
}

TPositiveControlInteger TPositiveControlInteger::operator+(const ui64 val) const {
    return Value + val;
}

}