#include "input.h"

#include <library/cpp/yson_pull/detail/input/stdio_file.h>
#include <library/cpp/yson_pull/detail/input/stream.h>

#include <util/generic/ptr.h>
#include <util/stream/file.h>
#include <util/stream/mem.h>

using namespace NYsonPull::NInput;
using namespace NYsonPull::NDetail::NInput;

namespace NInput = NYsonPull::NInput;

THolder<IStream> NInput::FromStdioFile(FILE* file, size_t buffer_size) {
    return MakeHolder<TStdioFile>(file, buffer_size);
}

THolder<IStream> NInput::FromPosixFd(int fd, size_t buffer_size) {
    return MakeHolder<TFHandle>(fd, buffer_size);
}

THolder<IStream> NInput::FromMemory(TStringBuf data) {
    return MakeHolder<TOwned<TMemoryInput>>(data);
}

THolder<IStream> NInput::FromInputStream(IInputStream* input, size_t buffer_size) {
    return MakeHolder<TOwned<TBufferedInput>>(input, buffer_size);
}

THolder<IStream> NInput::FromZeroCopyInput(IZeroCopyInput* input) {
    return MakeHolder<TZeroCopy>(input);
}
