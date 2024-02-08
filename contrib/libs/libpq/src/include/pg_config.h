#pragma once

#if defined(__APPLE__) && (defined(__aarch64__) || defined(_M_ARM64))
#   include "pg_config-osx-arm64.h"
#elif defined(__APPLE__)
#   include "pg_config-osx.h"
#elif defined(_MSC_VER)
#   include "pg_config-win.h"
#elif defined(__linux__) && (defined(__aarch64__) || defined(_M_ARM64))
#   include "pg_config-linux-aarch64.h"
#else
#   include "pg_config-linux.h"
#endif
