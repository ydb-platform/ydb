#pragma once

#include <util/folder/path.h>
#include <util/generic/maybe.h>

#include <cstddef>

namespace NLsp::NYql {

struct TArgs {
    bool IsStdIO = false;
    size_t Threads = 0;
    TMaybe<TFsPath> MessageCapturePath;

    static TArgs Parse(int argc, char** argv);
};

} // namespace NLsp::NYql
