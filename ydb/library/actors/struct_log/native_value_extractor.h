#pragma once

#include "structured_message.h"

#include <optional>

namespace NActors::NStructuredLog {

template <typename T>
class TNativeValueExtractor {
public:
    TNativeValueExtractor() = default;

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

    template <typename TValueType>
    bool operator()(const TValueType& value) {
        if constexpr(std::is_convertible_v<TValueType, T>) {
            Result.first = TResultKind::Ok;
            Result.second = value;
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
