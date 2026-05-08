#include "meta_writer.h"

namespace NActors::NStructuredLog {

bool TMetaWriter::Write(TLogRecord::TMetaFlags& metaFlags, const TStructuredMessage& message) {
    MetaFlags = &metaFlags;

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

    MetaFlags = nullptr;
    return true;
}

TMetaWriter::TValueWriter::TValueWriter(TMetaWriter& writer) : Writer(writer) {}

}  // namespace NActors::NStructuredLog
