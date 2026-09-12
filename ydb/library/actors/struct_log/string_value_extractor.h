#pragma once

#include "structured_message.h"

#include <optional>

namespace NActors::NStructuredLog {

class TStringValueExtractor {
public:
    TStringValueExtractor() = default;

    const std::optional<TString>& ExtractValue(const TStructuredMessage& message, std::size_t index) {
        ExtractedValue.reset();

        auto processValue = [&](const std::vector<TKeyName>& , TNativeTypeCode typeCode, const void* data, std::size_t length)->bool {
            auto it = TypeValueMap.find(typeCode);
            if (it == end(TypeValueMap)) {
                return false;
            }
            return it->second(data, length);
        };
        message.ForIndexSerialized(index, processValue);
        return ExtractedValue;
    }

    const std::optional<TString>& ExtractValue(const TStructuredMessage& message, const std::vector<TKeyName>& name) {
        auto index = message.GetValueIndex(name);
        if (!index.has_value()) {
            ExtractedValue.reset();
            return ExtractedValue;
        }
        return ExtractValue(message, index.value());
    }

    template <typename TValueType>
    bool operator()(const TValueType& value) {
        ExtractedValue = TNativeTypeSupport<TValueType>::ToString(value);
        return true;
    }

protected:
    std::optional<TString> ExtractedValue;
    TInvokerMap TypeValueMap = TTypesMapping::CreateInvokerMap(*this);
};

}
