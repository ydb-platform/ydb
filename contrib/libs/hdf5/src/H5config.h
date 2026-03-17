#pragma once

#if defined(__APPLE__)
#   include "H5config-osx.h"
#elif defined(_MSC_VER)
#   include "H5config-win.h"
#else
#   include "H5config-linux.h"
#endif
