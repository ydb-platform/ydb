#pragma once

#include <util/generic/string.h>
#include <util/generic/vector.h>

namespace NYql {
    namespace NBacktrace {
        size_t CollectBacktrace(void** addresses, size_t limit, void* data);
        struct TCollectedFrame {
            TCollectedFrame(uintptr_t addr);
            TCollectedFrame() = default;
            const char* File;
            size_t Address;
        };
        size_t CollectFrames(TCollectedFrame* frames, void* data);
        size_t CollectFrames(TCollectedFrame* frames, void** stack, size_t cnt);
    }
}