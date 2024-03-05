#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {
    namespace NBacktrace {
        struct TStackFrame {
            TString File;
            size_t Address;
        };
        [[nodiscard]] TVector<TString> Symbolize(const TVector<TStackFrame>& frames);
    }
}