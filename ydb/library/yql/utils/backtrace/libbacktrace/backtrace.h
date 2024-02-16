#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {
    namespace NBacktrace {
        size_t CollectBacktrace(void** addresses, size_t limit, void* data);
        struct TCollectedFrame {
            TCollectedFrame(uintptr_t addr);
            TString File;
            size_t Address;
        };
        TVector<TCollectedFrame> CollectFrames(void* data);
        TVector<TCollectedFrame> CollectFrames(void** stack, size_t cnt);
    }
}