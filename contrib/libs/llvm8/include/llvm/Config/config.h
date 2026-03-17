#pragma once

#if defined(__linux__)
#include "config-linux.h"
#endif

#if defined(__APPLE__)
#include "config-osx.h"
#endif

#if defined(_MSC_VER)
#include "config-win.h"
#endif

#if defined(_musl_)
#include "config-musl.h"
#endif
