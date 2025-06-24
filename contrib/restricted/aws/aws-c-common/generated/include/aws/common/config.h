#pragma once

#if defined(__aarch64__) || defined(_M_ARM64)
#   include "config-armv8a.h"
#else
#   include "config-linux.h"
#endif
