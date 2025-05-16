#pragma once

#if defined(__APPLE__)
#   include "libssh2_config-osx.h"
#elif defined(_MSC_VER)
#   include "libssh2_config-win.h"
#else
#   include "libssh2_config-linux.h"
#endif
