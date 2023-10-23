#pragma once

#if defined(__IOS__)
#   include "ares_build-ios.h"
#elif defined(_MSC_VER) && (defined(__x86_64__) || defined(_M_X64))
#   include "ares_build-win-x86_64.h"
#else
#   include "ares_build-linux.h"
#endif
