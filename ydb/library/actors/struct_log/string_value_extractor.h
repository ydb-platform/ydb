#pragma once

#include "structured_message.h"

#include <optional>

namespace NActors::NStructuredLog {

class TStringValueExtractor {
public:
    TStringValueExtractor() = default;
    TStringValueExtractor(const TStringValueExtractor&) = delete;
    TStringValueExtractor(const TStringValueExtractor&&) = delete;
    TStringValueExtractor& operator=(const TStringValueExtractor&) = delete;
    TStringValueExtractor& operator=(const TStringValueExtractor&&) = delete;

    std::optional<TString> ExtractValue(const TStructuredMessage& message, std::size_t index);
    std::optional<TString> ExtractValue(const TStructuredMessage& message, const std::vector<TKeyName>& name);

    template <typename TValueType>
    bool operator()(const TValueType& value) {
        ExtractedValue = TNativeTypeSupport<TValueType>::ToString(value);
        return true;
    }

protected:
    std::optional<TString> ExtractedValue;
    TInvokerMap TypeValueMap = TTypesMapping::CreateInvokerMap(*this);
};

} // namespace NActors::NStructuredLog
