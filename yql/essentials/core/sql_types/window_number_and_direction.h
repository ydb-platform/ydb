#pragma once

#include <util/generic/overloaded.h>
#include <yql/essentials/public/decimal/yql_decimal.h>
#include <yql/essentials/core/sql_types/window_direction.h>
#include <yql/essentials/utils/yql_panic.h>

#include <util/system/types.h>
#include <util/system/yassert.h>
#include <util/generic/hash.h>

#include <utility>
#include <variant>

namespace NYql::NWindow {

template <typename T>
class TNumberAndDirection {
public:
    static inline constexpr bool IsInt128 = std::is_same_v<T, NYql::NDecimal::TInt128> || std::is_same_v<T, NYql::NDecimal::TUint128>;
    static inline constexpr bool IsArithmetic = std::is_arithmetic_v<T> || IsInt128;

    struct TUnbounded {
        bool operator==(const TUnbounded&) const = default;
    };

    struct TZero {
        bool operator==(const TZero&) const = default;
    };

    using TNumberType = T;

    // Here we need entity named `Zero` for non-numeric types.
    // In numeric types we can just use T{0}.
    using TValueType = std::conditional_t<!IsArithmetic, std::variant<TNumberType, TUnbounded, TZero>, std::variant<TNumberType, TUnbounded>>;

    TNumberAndDirection(TNumberType value, EDirection direction)
        : TNumberAndDirection(TValueType(std::move(value)), direction, TPrivateTag{})
    {
    }

    static TNumberAndDirection<T> Inf(EDirection direction) {
        return TNumberAndDirection<T>(TUnbounded{}, direction, TPrivateTag{});
    }

    static TNumberAndDirection<T> Zero()
        requires(!IsArithmetic)
    {
        return TNumberAndDirection<T>(TZero{}, EDirection::Following, TPrivateTag{});
    }

    const T& GetUnderlyingValue() const {
        return std::get<T>(Value_);
    }

    const TValueType& GetValue() const {
        return Value_;
    }

    bool IsFinite() const {
        return std::holds_alternative<T>(Value_);
    }

    bool IsInf() const {
        return std::holds_alternative<TUnbounded>(Value_);
    }

    bool IsZero() const
        requires(!IsArithmetic)
    {
        return std::holds_alternative<TZero>(Value_);
    }

    EDirection GetDirection() const {
        return Direction_;
    }

    template <typename U>
    std::strong_ordering operator<=>(const TNumberAndDirection<U>& other) const
        requires(IsArithmetic)
    {
        if (Direction_ == EDirection::Preceding && other.Direction_ == EDirection::Following) {
            return std::strong_ordering::less;
        }
        if (Direction_ == EDirection::Following && other.Direction_ == EDirection::Preceding) {
            return std::strong_ordering::greater;
        }

        using Common = std::common_type_t<T, U>;

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
                    return ToStrong(static_cast<Common>(other.GetUnderlyingValue()) <=> static_cast<Common>(GetUnderlyingValue()));
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
                    return ToStrong(static_cast<Common>(GetUnderlyingValue()) <=> static_cast<Common>(other.GetUnderlyingValue()));
                }
            }
        }
    }

    bool operator==(const TNumberAndDirection& other) const = default;
    bool operator!=(const TNumberAndDirection& other) const = default;

private:
    struct TPrivateTag {};
    TNumberAndDirection(TValueType value, EDirection direction, TPrivateTag)
        : Value_(std::move(value))
        , Direction_(direction)
    {
        if constexpr (std::is_floating_point_v<TNumberType>) {
            if (!IsInf()) {
                Y_DEBUG_ABORT_UNLESS(!std::isnan(GetUnderlyingValue()), "Nan is not allowed to be a directioned value.");
                Y_DEBUG_ABORT_UNLESS(!std::isinf(GetUnderlyingValue()), "Inf is not allowed to be a directioned value.");
            }
        }
        if constexpr (IsInt128) {
            Y_DEBUG_ABORT_UNLESS(NYql::NDecimal::IsNormal(GetUnderlyingValue()), "Only normal values are allowed.");
        }
        if constexpr (IsArithmetic) {
            YQL_ENSURE(IsInf() || GetUnderlyingValue() >= 0, "Only positive values are allowed.");
            // Normalize zero value to prevent two possible interpretations.
            if (!IsInf() && GetUnderlyingValue() == 0) {
                Direction_ = EDirection::Following;
            }
        }
    }

    template <typename U>
    friend class TNumberAndDirection;

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

template <typename T, typename TValueHasher = THash<typename TNumberAndDirection<T>::TNumberType>>
class TNumberAndDirectionHash {
public:
    explicit TNumberAndDirectionHash(TValueHasher hasher = TValueHasher{})
        : Hasher_(std::move(hasher))
    {
    }

    size_t operator()(const TNumberAndDirection<T>& value) const {
        size_t hash = THash<int>{}(static_cast<int>(value.GetDirection()));
        hash = CombineHashes(hash, std::visit(
                                       TOverloaded{
                                           [&](TNumberAndDirection<T>::TUnbounded) {
                                               return THash<size_t>{}(1);
                                           },
                                           [&](TNumberAndDirection<T>::TZero) {
                                               return THash<size_t>{}(2);
                                           },
                                           [&](const TNumberAndDirection<T>::TNumberType& number) {
                                               return Hasher_(number);
                                           }}, value.GetValue()));
        return hash;
    }

private:
    TValueHasher Hasher_;
};

template <typename T, typename TComparator = TEqualTo<typename TNumberAndDirection<T>::TNumberType>>
class TNumberAndDirectionComparator {
public:
    explicit TNumberAndDirectionComparator(TComparator comparator = TComparator{})
        : Comparator_(comparator)
    {
    }

    bool operator()(const TNumberAndDirection<T>& left, const TNumberAndDirection<T>& right) const {
        if (left.GetDirection() != right.GetDirection()) {
            return false;
        }
        return std::visit(
            TOverloaded{
                [&](const typename TNumberAndDirection<T>::TNumberType& l, const typename TNumberAndDirection<T>::TNumberType& r) {
                    return Comparator_(l, r);
                },
                [](typename TNumberAndDirection<T>::TUnbounded, typename TNumberAndDirection<T>::TUnbounded) {
                    return true;
                },
                [](typename TNumberAndDirection<T>::TZero, typename TNumberAndDirection<T>::TZero) {
                    return true;
                },
                [](const auto&, const auto&) {
                    return false;
                }},
            left.GetValue(), right.GetValue());
    }

private:
    TComparator Comparator_;
};

} // namespace NYql::NWindow

template <typename T>
struct THash<NYql::NWindow::TNumberAndDirection<T>>: NYql::NWindow::TNumberAndDirectionHash<T> {};
