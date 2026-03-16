#pragma once

#if defined(__APPLE__)
#   include "config-osx.h"
#elif defined(_MSC_VER)
#   include "config-win.h"
#elif defined(__FreeBSD__) && (defined(__x86_64__) || defined(_M_X64))
#   include "config-freebsd-x86_64.h"
#else
#   include "config-linux.h"
#endif
