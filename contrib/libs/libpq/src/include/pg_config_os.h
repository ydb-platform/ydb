#pragma once

#if defined(__APPLE__)
#   include "pg_config_os-osx.h"
#elif defined(_MSC_VER)
#   include "pg_config_os-win.h"
#else
#   include "pg_config_os-linux.h"
#endif
