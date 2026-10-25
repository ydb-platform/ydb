#pragma once

#if defined(__IOS__)
#   include "config-ios.h"
#elif defined(__APPLE__)
#   include "config-osx.h"
#elif defined(_MSC_VER)
#   include "config-win.h"
#else
#   include "config-linux.h"
#endif
