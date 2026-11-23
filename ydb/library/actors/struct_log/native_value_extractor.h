#pragma once

#include "structured_message.h"

#include <cmath>
#include <limits>
#include <optional>
#include <string_view>
#include <type_traits>
#include <utility>

namespace NActors::NStructuredLog {

template <typename T>
class TNativeValueExtractor {
    static_assert(
        !std::is_same_v<T, TStringBuf>,
        "TNativeValueExtractor does not support TStringBuf; use TNativeValueExtractor<TString> or TStringValueExtractor");
    static_assert(
        !std::is_same_v<T, std::string_view>,
        "TNativeValueExtractor does not support std::string_view; use TNativeValueExtractor<TString> or TStringValueExtractor");

public:
    TNativeValueExtractor() = default;
    TNativeValueExtractor(const TNativeValueExtractor&) = delete;
    TNativeValueExtractor(const TNativeValueExtractor&&) = delete;
    TNativeValueExtractor& operator=(const TNativeValueExtractor&) = delete;
    TNativeValueExtractor& operator=(const TNativeValueExtractor&&) = delete;

    enum class TResultKind {
        Ok = 0,
        NoCast = 1,
        NoValue = 2
    };
    using TOptional = std::optional<T>;
    using TResult = std::pair<TResultKind, TOptional>;

    TResult ExtractValue(const TStructuredMessage& message, std::size_t index) {
        Result.first = TResultKind::NoValue;
        Result.second.reset();

        auto processValue = [&](const std::vector<TKeyName>& , TNativeTypeCode typeCode, const void* data, std::size_t length)->bool {
            auto it = TypeValueMap.find(typeCode);
            if (it == end(TypeValueMap)) {
                return false;
            }
            return it->second(data, length);
        };
        message.ForIndexSerialized(index, processValue);
        return std::move(Result);
    }

    TResult ExtractValue(const TStructuredMessage& message, const std::vector<TKeyName>& name) {
        auto index = message.GetValueIndex(name);
        if (!index.has_value()) {
            Result.first = TResultKind::NoValue;
            Result.second.reset();
            return Result;
        }
        return ExtractValue(message, index.value());
    }

    // Rejects NaN/Inf and out-of-range numeric casts (notably float→int UB, narrowing float→float).
    template <typename TDst, typename TSrc>
    static bool IsSafeNumericCast(const TSrc& value) {
        if constexpr (std::is_same_v<TDst, TSrc>) {
            return true;
        } else if constexpr (std::is_floating_point_v<TSrc> && std::is_same_v<TDst, bool>) {
            if (!std::isfinite(value)) {
                return false;
            }

            TSrc intValue;
            std::modf(value, &intValue);
            if (value != intValue) {
                return false;
            }

            return value == TSrc(0) || value == TSrc(1);
        } else if constexpr (std::is_floating_point_v<TSrc> && std::numeric_limits<TDst>::is_integer) {
            if (!std::isfinite(value)) {
                return false;
            }

            TSrc intValue;
            std::modf(value, &intValue);
            if (value != intValue) {
                return false;
            }

            // signed: [-2^digits, 2^digits); unsigned: (-1, 2^digits)
            const TSrc upper = std::ldexp(static_cast<TSrc>(1), std::numeric_limits<TDst>::digits);
            if constexpr (std::is_signed_v<TDst>) {
                return value >= -upper && value < upper;
            } else {
                return value > static_cast<TSrc>(-1) && value < upper;
            }
        } else if constexpr (std::numeric_limits<TSrc>::is_integer && std::numeric_limits<TDst>::is_integer && !std::is_same_v<TSrc, bool>) {
            if constexpr (std::is_signed_v<TSrc> == std::is_signed_v<TDst>) {
                return (value >= std::numeric_limits<TDst>::min()) && (value <= std::numeric_limits<TDst>::max());
            } else {
                if (value < TSrc(0)) {
                    // Unable cast negative to unsigned
                    return false;
                }

                auto unsignedValue = static_cast<std::make_unsigned_t<TSrc>>(value);
                return unsignedValue <= std::numeric_limits<TDst>::max();
            }
        } else if constexpr (std::is_floating_point_v<TSrc> && std::is_floating_point_v<TDst>) {
            if (!std::isfinite(value)) {
                return false;
            }

            using TCommon = std::common_type_t<TSrc, TDst>;
            const TCommon maxVal = std::numeric_limits<TDst>::max();
            return static_cast<TCommon>(value) >= -maxVal && static_cast<TCommon>(value) <= maxVal;
        } else {
            return true;
        }
    }

    template <typename TValueType>
    bool operator()(const TValueType& value) {
        if constexpr(std::is_convertible_v<TValueType, T>) {
            if (!IsSafeNumericCast<T, TValueType>(value)) {
                Result.first = TResultKind::NoCast;
            } else {
                Result.first = TResultKind::Ok;
                Result.second = value;
            }
        } else {
            Result.first = TResultKind::NoCast;
        }
        return true;
    }

protected:
    TResult Result;
    TInvokerMap TypeValueMap = TTypesMapping::CreateInvokerMap(*this);
};

}
