#pragma once

#if defined(__APPLE__)
#   include "config-osx.h"
#elif defined(_MSC_VER)
#   include "config-win.h"
#else
#   include "config-linux.h"
#endif
