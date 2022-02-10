#pragma once

#include <util/system/defaults.h>

namespace NArgonish {
    /**
     * Instruction sets for which Argon2 is optimized
     */
    enum class EInstructionSet : ui32 {
        REF = 0,   /// Reference implementation
#if !defined(_arm64_)
        SSE2 = 1,  /// SSE2 optimized version
        SSSE3 = 2, /// SSSE3 optimized version
        SSE41 = 3, /// SSE4.1 optimized version
        AVX2 = 4   /// AVX2 optimized version
#endif
    };
}
