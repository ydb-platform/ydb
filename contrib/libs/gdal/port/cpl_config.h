#pragma once

#if defined(__APPLE__)
#   include "cpl_config-osx.h"
#elif defined(_MSC_VER)
#   include "cpl_config-win.h"
#else
#   include "cpl_config-linux.h"
#endif

#if defined(_musl_)
#   include "cpl_config-musl.h"
#endif
