#include "text_writer.h"

namespace NActors::NStructuredLog {

bool TTextWriter::Write(TStringBuilder& outputText, const TStructuredMessage& message) {
    OutputText = &outputText;
    FirstValue = true;

    auto processValue =
        [&](const std::vector<TKeyName>& name, TNativeTypeCode typeCode, const void* data, std::size_t length) {
            auto it = TypeValueWriterMap.find(typeCode);
            if (it != end(TypeValueWriterMap)) {
                ValueWriter.KeyName = &name;
                return it->second(data, length);
            } else {
                return false;
            }
        };
    if (!message.ForEachSerialized(processValue)) {
        return false;
    };

    OutputText = nullptr;
    return true;
}

TTextWriter::TValueWriter::TValueWriter(TTextWriter& writer) : Writer(writer) {}

}  // namespace NActors::NStructuredLog
