#pragma once 

#include "config-linux.h"

#undef HAVE_SSSE3
#define HAVE_SSSE3 0

#undef HAVE_SSE41
#define HAVE_SSE41 0

#undef HAVE_SSE42
#define HAVE_SSE42 0

#undef HAVE_AVX
#define HAVE_AVX 0

#undef HAVE_AVX2
#define HAVE_AVX2 0

#undef HAVE_AVX512
#define HAVE_AVX512 0

#undef HAVE_NEON32
#define HAVE_NEON32 1

#undef HAVE_NEON64
#define HAVE_NEON64 1
