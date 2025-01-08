#pragma once
#include <util/system/types.h>

namespace NKikimr {

class TPositiveControlInteger {
private:
    ui64 Value = 0;
public:
    TPositiveControlInteger() = default;
    TPositiveControlInteger(const ui64 value)
        : Value(value) {

    }
    TPositiveControlInteger(const i64 value);
    ui64 Add(const ui64 value);
    ui64 Sub(const ui64 value);
    ui64 Inc() {
        return Add(1);
    }
    ui64 Dec() {
        return Sub(1);
    }
    ui64 Val() const;
    bool operator!() const {
        return !Value;
    }
    operator ui64() const {
        return Value;
    }
    ui64 operator++() {
        return Inc();
    }
    ui64 operator--() {
        return Dec();
    }
    TPositiveControlInteger operator-(const ui64 val) const;
    TPositiveControlInteger operator+(const ui64 val) const;
};

}