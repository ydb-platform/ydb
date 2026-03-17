#pragma once

#if defined(__APPLE__)
#   include "H5pubconf-osx.h"
#elif defined(_MSC_VER)
#   include "H5pubconf-win.h"
#else
#   include "H5pubconf-linux.h"
#endif
