#pragma once

#include "crc32c_config-linux.h"

// Define to 1 if targeting X86 and the compiler has the _mm_prefetch intrinsic.
#undef HAVE_MM_PREFETCH

// Define to 1 if targeting X86 and the compiler has the _mm_crc32_u{8,32,64}
// intrinsics.
#undef HAVE_SSE42

// Define to 1 if targeting ARM and the compiler has the __crc32c{b,h,w,d} and
// the vmull_p64 intrinsics.
#undef HAVE_ARM64_CRC32C
#define HAVE_ARM64_CRC32C 1
