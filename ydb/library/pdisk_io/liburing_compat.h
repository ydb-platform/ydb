#pragma once

// Keep musl's NGROUPS_MAX visible to the rest of the translation unit while
// still allowing linux/uapi headers pulled by liburing to define their own.
#if defined(NGROUPS_MAX)
#define YDB_PDISK_IO_RESTORE_NGROUPS_MAX 1
#pragma push_macro("NGROUPS_MAX")
#undef NGROUPS_MAX
#endif

#include <liburing.h>

#if defined(YDB_PDISK_IO_RESTORE_NGROUPS_MAX)
#pragma pop_macro("NGROUPS_MAX")
#undef YDB_PDISK_IO_RESTORE_NGROUPS_MAX
#endif
