#pragma once

#if defined(__APPLE__)
#   include "event-config-osx.h"
#elif defined(_MSC_VER)
#   include "event-config-win.h"
#else
#   include "event-config-linux.h"
#endif

#if defined(_musl_)
#   include "event-config-musl.h"
#endif
