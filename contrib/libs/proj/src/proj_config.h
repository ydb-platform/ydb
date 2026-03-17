#pragma once

#if defined(__APPLE__)
#   include "proj_config-osx.h"
#else
#   include "proj_config-linux.h"
#endif

#if defined(_musl_)
#   include "proj_config-musl.h"
#endif
