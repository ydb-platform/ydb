#pragma once

// Linux-only wrapper around liburing. Must be included AFTER YDB headers because
// linux/uapi headers pulled by liburing may define macros that clash with project
// headers. Do not include this header from cross-platform translation units: ya make
// validates includes statically and cannot see through #if defined(__linux__).

// Keep musl's NGROUPS_MAX visible to the rest of the translation unit while
// still allowing linux/uapi headers pulled by liburing to define their own.
#if defined(NGROUPS_MAX)
#define YDB_LIBURING_RESTORE_NGROUPS_MAX 1
#pragma push_macro("NGROUPS_MAX")
#undef NGROUPS_MAX
#endif

#include <liburing.h>

// linux/uapi headers pulled by liburing define macros that clash with YDB code.
#ifdef BLOCK_SIZE
#undef BLOCK_SIZE
#endif

#if defined(YDB_LIBURING_RESTORE_NGROUPS_MAX)
#pragma pop_macro("NGROUPS_MAX")
#undef YDB_LIBURING_RESTORE_NGROUPS_MAX
#endif
