#include "meta_writer.h"

namespace NActors::NStructuredLog {

bool TMetaWriter::Write(TLogRecord::TMetaFlags& metaFlags, const TStructuredMessage& message) {
    MetaFlags = &metaFlags;

    auto result = MessageWriter.WriteMessage(message);

    MetaFlags = nullptr;
    return result;
}

TMetaWriter::TValueWriter::TValueWriter(TMetaWriter& writer)
    : TBaseValueWriter<TMetaWriter>(writer)
{}

void TMetaWriter::TValueWriter::operator()(const TString& value) const {
    TStringBuilder metakeyName;
    metakeyName << "meta";
    for (const auto& keyItem : *KeyName) {
        metakeyName << ".";
        metakeyName << keyItem.ToString();
    }
    Writer.MetaFlags->push_back({metakeyName, TTypesMapping::ToString(value)});
}

}  // namespace NActors::NStructuredLog
