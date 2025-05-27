#pragma once
#include "backtrace.h"

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {
    namespace NBacktrace {
        struct TStackFrame {
            const char* File;
            size_t Address;
        };
        void Symbolize(const TStackFrame* frames, size_t count, IOutputStream* out);
    }
}