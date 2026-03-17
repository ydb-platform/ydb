#pragma once

#if defined(__ANDROID__)
#   include "apr_private-android.h"
#elif defined(__APPLE__)
#   include "apr_private-osx.h"
#elif defined(__FreeBSD__) && (defined(__x86_64__) || defined(_M_X64))
#   include "apr_private-freebsd-x86_64.h"
#else
#   include "apr_private-linux.h"
#endif

#if defined(_musl_)
#   include "apr_private-musl.h"
#endif
