#include "meta_writer.h"

namespace NActors::NStructuredLog {

bool TMetaWriter::Write(TLogRecord::TMetaFlags& metaFlags, const TStructuredMessage& message) {
    MetaFlags = &metaFlags;

    auto result = MessageWriter.WriteMessage(message);

    MetaFlags = nullptr;
    return result;
}

void TMetaWriter::WriteCanonical(TLogRecord::TMetaFlags& metaFlags, const TStructuredMessage& message) {
    message.ForEachTypedWithKind([&](const auto& name, const auto& value, EFieldKind kind) {
        metaFlags.push_back({MetaKey(kind == EFieldKind::Label ? "labels" : "meta.message", name), TTypesMapping::ToString(value)});
    });
}

TString TMetaWriter::MetaKey(TStringBuf prefix, const std::vector<TKeyName>& name) {
    TStringBuilder key;
    key << prefix;
    for (const auto& item : name) {
        key << '.' << TStringBuf(item.GetData(), item.GetLength());
    }
    return std::move(key);
}

TMetaWriter::TValueWriter::TValueWriter(TMetaWriter& writer)
    : TBaseValueWriter<TMetaWriter>(writer)
{}

}  // namespace NActors::NStructuredLog
