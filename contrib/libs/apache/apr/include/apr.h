#pragma once

#if defined(__ANDROID__)
#   include "apr-android.h"
#elif defined(__APPLE__)
#   include "apr-osx.h"
#elif defined(_MSC_VER)
#   include "apr-win.h"
#elif defined(__FreeBSD__) && (defined(__x86_64__) || defined(_M_X64))
#   include "apr-freebsd-x86_64.h"
#else
#   include "apr-linux.h"
#endif
