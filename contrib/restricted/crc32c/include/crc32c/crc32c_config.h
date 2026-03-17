#pragma once

#if defined(__APPLE__) && (defined(__aarch64__) || defined(_M_ARM64))
#   include "crc32c_config-osx-arm64.h"
#else
#   include "crc32c_config-linux.h"
#endif
