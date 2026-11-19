#include "string_value_extractor.h"

namespace NActors::NStructuredLog {

std::optional<TString> TStringValueExtractor::ExtractValue(const TStructuredMessage& message, std::size_t index) {
    ExtractedValue.reset();

    auto processValue = [&](const std::vector<TKeyName>&, TNativeTypeCode typeCode, const void* data, std::size_t length) -> bool {
        auto it = TypeValueMap.find(typeCode);
        if (it == end(TypeValueMap)) {
            return false;
        }
        return it->second(data, length);
    };
    message.ForIndexSerialized(index, processValue);
    return std::move(ExtractedValue);
}

std::optional<TString> TStringValueExtractor::ExtractValue(const TStructuredMessage& message, const std::vector<TKeyName>& name) {
    auto index = message.GetValueIndex(name);
    if (!index.has_value()) {
        ExtractedValue.reset();
        return ExtractedValue;
    }
    return ExtractValue(message, index.value());
}

} // namespace NActors::NStructuredLog
