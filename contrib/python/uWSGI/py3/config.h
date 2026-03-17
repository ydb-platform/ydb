#pragma once

#if defined(__APPLE__)
#   include "config-osx.h"
#else
#   include "config-linux.h"
#endif

#if defined(_musl_)
#   include "config-musl.h"
#endif
