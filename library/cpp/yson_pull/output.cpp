#include "output.h"

#include <library/cpp/yson_pull/detail/output/stdio_file.h>
#include <library/cpp/yson_pull/detail/output/stream.h>

#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/str.h>

using namespace NYsonPull::NOutput;
using namespace NYsonPull::NDetail::NOutput;

namespace NOutput = NYsonPull::NOutput;

THolder<IStream> NOutput::FromStdioFile(FILE* file, size_t buffer_size) {
    return MakeHolder<TStdioFile>(file, buffer_size);
}

THolder<IStream> NOutput::FromPosixFd(int fd, size_t buffer_size) {
    return MakeHolder<TFHandle>(fd, buffer_size);
}

THolder<IStream> NOutput::FromString(TString* output, size_t buffer_size) {
    return MakeHolder<TOwned<TStringOutput>>(buffer_size, *output);
}

THolder<IStream> NOutput::FromOutputStream(IOutputStream* output, size_t buffer_size) {
    return MakeHolder<TStream>(output, buffer_size);
}
