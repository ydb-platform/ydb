#pragma once

#include "clickhouse_config.h"

#if USE_LIBARCHIVE

#ifdef __clang__
#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreserved-macro-identifier"

#error #include <archive.h>
#error #include <archive_entry.h>
#endif
#endif
