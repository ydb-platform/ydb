#pragma once

#include <cstddef>

namespace NLsp::NYql {

struct TArgs {
    bool IsStdIO = false;
    size_t Threads = 0;

    static TArgs Parse(int argc, char** argv);
};

} // namespace NLsp::NYql
