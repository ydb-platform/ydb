#pragma once

#if defined(__APPLE__)
#   include "libssh2_config-osx.h"
#elif defined(_MSC_VER)
#   include "libssh2_config-win.h"
#elif defined(__FreeBSD__) && (defined(__x86_64__) || defined(_M_X64))
#   include "libssh2_config-freebsd-x86_64.h"
#else
#   include "libssh2_config-linux.h"
#endif
