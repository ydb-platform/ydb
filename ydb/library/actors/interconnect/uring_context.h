#pragma once

#include <util/system/types.h>

namespace NActors {

    class TUringContext {
    public:
        // SQPOLL kernel-thread idle window (ms) before it sleeps; only used when SQPOLL is on.
        static constexpr ui32 SqThreadIdleMs = 2000;

#ifdef __linux__
        // True when io_uring is usable at all (a plain ring can be created). This is the minimum for the
        // v2 data plane.
        static bool IsAvailable();
#else
        static bool IsAvailable() { return false; }
#endif
    };

} // namespace NActors
