#include "writer.h"
#include <library/cpp/yson_pull/detail/writer.h>

using namespace NYsonPull;

TWriter NYsonPull::MakeBinaryWriter(
    THolder<NOutput::IStream> stream,
    EStreamType mode) {
    return NYsonPull::NDetail::make_writer<NYsonPull::NDetail::TBinaryWriterImpl>(
        std::move(stream),
        mode);
}

TWriter NYsonPull::MakeTextWriter(
    THolder<NOutput::IStream> stream,
    EStreamType mode) {
    return NYsonPull::NDetail::make_writer<NYsonPull::NDetail::TTextWriterImpl>(
        std::move(stream),
        mode);
}

TWriter NYsonPull::MakePrettyTextWriter(
    THolder<NOutput::IStream> stream,
    EStreamType mode,
    size_t indent_size) {
    return NYsonPull::NDetail::make_writer<NYsonPull::NDetail::TPrettyWriterImpl>(
        std::move(stream),
        mode,
        indent_size);
}
