#pragma once

#include <util/system/compiler.h>

namespace NBench {

/**
 * Functions that states "I can read and write everywhere in memory".
 *
 * Use it to prevent optimizer from reordering or discarding memory writes prior
 * to it's call, and force memory reads after it's call.
 */
Y_FORCE_INLINE void Clobber() {
#if defined(__GNUC__)
    asm volatile(""
                 :
                 :
                 : "memory");
#elif defined(_MSC_VER)
    _ReadWriteBarrier();
#else
    // Otherwise, do nothing
#endif
}

} // namespace NBench
