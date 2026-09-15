#pragma once

#include "structured_message.h"

#include <cmath>
#include <limits>
#include <optional>
#include <type_traits>

namespace NActors::NStructuredLog {

template <typename T>
class TNativeValueExtractor {
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

    const TResult& ExtractValue(const TStructuredMessage& message, std::size_t index) {
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
        return Result;
    }

    const TResult& ExtractValue(const TStructuredMessage& message, const std::vector<TKeyName>& name) {
        auto index = message.GetValueIndex(name);
        if (!index.has_value()) {
            Result.first = TResultKind::NoValue;
            Result.second.reset();
            return Result;
        }
        return ExtractValue(message, index.value());
    }

    // Rejects NaN/Inf and values whose truncation would not fit in TDst (UB on float→int).
    template <typename TDst, typename TSrc>
    bool IsSafeNumericCast(const TSrc& value) {
        if constexpr (std::is_floating_point_v<TSrc> && std::is_integral_v<TDst> && !std::is_same_v<TDst, bool>) {
            if (!std::isfinite(value)) {
                return false;
            }
            // signed: [-2^digits, 2^digits); unsigned: (-1, 2^digits)
            const TSrc upper = std::ldexp(static_cast<TSrc>(1), std::numeric_limits<TDst>::digits);
            if constexpr (std::is_signed_v<TDst>) {
                return value >= -upper && value < upper;
            } else {
                return value > static_cast<TSrc>(-1) && value < upper;
            }
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

template <>
class TNativeValueExtractor<TStringBuf> {};

}
