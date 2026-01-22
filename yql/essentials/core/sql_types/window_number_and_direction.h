#pragma once

#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/core/sql_types/window_direction.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <variant>
#include <compare>
#include <cmath>

namespace NYql::NWindow {

template <typename T>
class TNumberAndDirection {
public:
    static inline constexpr bool IsArithmetic = std::is_arithmetic_v<T>;

    struct TUnbounded {
        bool operator==(const TUnbounded&) const = default;
    };

    using TValueType = std::variant<T, TUnbounded>;
    using TNumberType = T;

    TNumberAndDirection(TValueType value, EDirection direction)
        : Value_(value)
        , Direction_(direction)
    {
        YQL_ENSURE(IsInf() || GetUnderlyingValue() >= 0, "Only positive values are allowed.");
        if constexpr (std::is_floating_point_v<TNumberType>) {
            if (!IsInf()) {
                Y_ABORT_UNLESS(!std::isnan(GetUnderlyingValue()), "Nan is not allowed to be a directioned value.");
            }
        }
        if constexpr (IsArithmetic) {
            // Normalize zero value to prevent two possible interpretations.
            if (!IsInf() && GetUnderlyingValue() == 0) {
                Direction_ = EDirection::Following;
            }
        }
    }

    TNumberAndDirection(TNumberType value, EDirection direction)
        : TNumberAndDirection(TValueType(value), direction)
    {
    }

    static TNumberAndDirection<T> Inf(EDirection direction) {
        return TNumberAndDirection<T>(TUnbounded{}, direction);
    }

    static TNumberAndDirection<T> Zero()
        requires IsArithmetic
    {
        return TNumberAndDirection<T>(0, EDirection::Following);
    }

    static TNumberAndDirection<T> Zero()
        requires std::same_as<T, TString>
    {
        return TNumberAndDirection<T>("0", EDirection::Following);
    }

    const T& GetUnderlyingValue() const {
        return std::get<T>(Value_);
    }

    bool IsInf() const {
        return std::holds_alternative<TUnbounded>(Value_);
    }

    EDirection GetDirection() const {
        return Direction_;
    }

    std::strong_ordering operator<=>(const TNumberAndDirection& other) const
        requires(IsArithmetic)
    {
        if (Direction_ == EDirection::Preceding && other.Direction_ == EDirection::Following) {
            return std::strong_ordering::less;
        }
        if (Direction_ == EDirection::Following && other.Direction_ == EDirection::Preceding) {
            return std::strong_ordering::greater;
        }
        if (Direction_ == EDirection::Preceding) {
            if (other.IsInf()) {
                if (IsInf()) {
                    return std::strong_ordering::equivalent;
                } else {
                    return std::strong_ordering::greater;
                }
            } else {
                if (IsInf()) {
                    return std::strong_ordering::less;
                } else {
                    return ToStrong(other.GetUnderlyingValue() <=> GetUnderlyingValue());
                }
            }

        } else {
            if (other.IsInf()) {
                if (IsInf()) {
                    return std::strong_ordering::equivalent;
                } else {
                    return std::strong_ordering::less;
                }
            } else {
                if (IsInf()) {
                    return std::strong_ordering::greater;
                } else {
                    return ToStrong(GetUnderlyingValue() <=> other.GetUnderlyingValue());
                }
            }
        }
    }

    bool operator==(const TNumberAndDirection& other) const = default;
    bool operator!=(const TNumberAndDirection& other) const = default;

private:
    static std::strong_ordering ToStrong(std::partial_ordering po) {
        if (po < 0) {
            return std::strong_ordering::less;
        }
        if (po > 0) {
            return std::strong_ordering::greater;
        }
        if (po == 0) {
            return std::strong_ordering::equal;
        }
        ythrow yexception() << "unexpected unordered";
    }

    static std::strong_ordering ToStrong(std::strong_ordering po) {
        return po;
    }

    TValueType Value_;
    EDirection Direction_;
};

} // namespace NYql::NWindow
