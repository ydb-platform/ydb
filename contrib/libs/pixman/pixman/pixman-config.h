#pragma once

#if defined(__aarch64__) || defined(_M_ARM64)
#   include "pixman-config-armv8a.h"
#elif defined(_MSC_VER)
#   include "pixman-config-win.h"
#else
#   include "pixman-config-linux.h"
#endif
