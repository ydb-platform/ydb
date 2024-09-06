#pragma once

#if defined(WIN32) && !defined(__CYGWIN__)

/* Must be included before sys/stat.h on WIN */
#if defined(_CRT_INTERNAL_NONSTDC_NAMES)
# error "pg_compat.h should be included first"
#endif

#include "win32_pg_compat.h"

#endif

#if defined(__cplusplus)
extern "C" {
#endif

#include "postgres.h"

#if defined(__cplusplus)
}
#endif

#undef Min
#undef Max
#undef Abs
#undef bind
#undef open
#undef FATAL
#undef locale_t
#undef strtou64
