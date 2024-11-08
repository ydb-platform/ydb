#pragma once

#include "dynumber.h"

#include <util/generic/ymath.h>

#include <limits>

namespace NKikimr::NDyNumber {

template <typename T>
TMaybe<T> TryFromDyNumber(TStringBuf buffer) {
    using traits = std::numeric_limits<T>;

    if (!traits::is_specialized || !IsValidDyNumber(buffer)) {
        return Nothing();
    }

    auto result = T();

    auto s = buffer.data();
    if (*s == '\x01') {
        return result;
    }

    const bool negative = !*s++;
    if (negative && !traits::is_signed) {
        return Nothing();
    }

    auto power = ui8(*s++);
    if (negative) {
        power = '\xFF' - power;
    }

    std::function<TMaybe<T>(T, i16)> pow = [&pow](T base, i16 exp) -> TMaybe<T> {
        if (exp > 0) {
            const auto p = Power(base, exp - 1);
            if (!p || p > traits::max()) {
                return Nothing();
            }

            return base * p;
        } else if (exp < 0) {
            if (const auto p = pow(base, abs(exp))) {
                return 1 / *p;
            }

            return Nothing();
        } else {
            return T(1);
        }
    };

    auto apply = [&negative, &result, &pow](ui8 digit, i16 exp) -> bool {
        if (traits::is_integer && (exp < 0 || exp > traits::digits10)) {
            return false;
        }

        if (digit == '\x00') {
            return true;
        }

        if (const auto p = pow(10, exp)) {
            const T mul = digit * *p;

            if (!mul || mul < *p || mul > traits::max()) {
                return false;
            }

            if (negative) {
                if (traits::lowest() - result > -mul) {
                    return false;
                }

                result -= mul;
            } else {
                if (traits::max() - result < mul) {
                    return false;
                }

                result += mul;
            }

            return true;
        }

        return false;
    };

    const auto digits = negative ? "FEDCBA9876543210" : "0123456789ABCDEF";

    auto e = power - 129;
    auto l = buffer.size() - 1;
    while (--l) {
        const auto c = *s++;
        const auto hi = (c >> '\x04') & '\x0F';
        const auto lo = c & '\x0F';

        if (!apply(digits[hi] - '0', --e)) {
            return Nothing();
        }

        if (lo != (negative ? '\x0F' : '\x00') || l > 1U) {
            if (!apply(digits[lo] - '0', --e)) {
                return Nothing();
            }
        }
    }

    return result;
}

}
