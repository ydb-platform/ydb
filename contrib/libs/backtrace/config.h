#pragma once

#if defined(__APPLE__)
#   include "config-osx.h"
#else
#   include "config-linux.h"
#endif

#if defined(__arm__) || defined(__ARM__)
#   include "config-armv7a.h"
#endif
