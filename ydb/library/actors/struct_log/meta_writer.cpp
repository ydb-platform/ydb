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

}  // namespace NActors::NStructuredLog
