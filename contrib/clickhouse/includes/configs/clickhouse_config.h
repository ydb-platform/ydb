#pragma once

#if defined(__APPLE__)
#   include "clickhouse_config-osx.h"
#elif defined(__linux__) && (defined(__aarch64__) || defined(_M_ARM64))
#   include "clickhouse_config-linux-aarch64.h"
#else
#   include "clickhouse_config-linux.h"
#endif

#if defined(_musl_)
#   include "clickhouse_config-musl.h"
#endif
