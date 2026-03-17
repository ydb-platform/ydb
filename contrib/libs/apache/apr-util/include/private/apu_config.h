#pragma once

#if defined(__APPLE__)
#   include "apu_config-osx.h"
#elif defined(_MSC_VER)
#   include "apu_config-win.h"
#else
#   include "apu_config-linux.h"
#endif
