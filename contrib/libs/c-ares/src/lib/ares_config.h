#pragma once

#if defined(__ANDROID__)
#   include "ares_config-android.h"
#elif defined(__APPLE__)
#   include "ares_config-osx.h"
#elif defined(__FreeBSD__) && (defined(__x86_64__) || defined(_M_X64))
#   include "ares_config-freebsd-x86_64.h"
#else
#   include "ares_config-linux.h"
#endif
