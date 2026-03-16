#pragma once

#if defined(__APPLE__)
#   include "magick-baseconfig-osx.h"
#elif defined(_MSC_VER)
#   include "magick-baseconfig-win.h"
#else
#   include "magick-baseconfig-linux.h"
#endif
