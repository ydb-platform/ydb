#pragma once

#if defined(__APPLE__)
#   include "portable-linux.h"
#   include "portable-osx.h"
#else
#   include "portable-linux.h"
#endif

#if defined(_musl_)
#   include "portable-musl.h"
#endif
