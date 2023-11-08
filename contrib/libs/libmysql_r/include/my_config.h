#pragma once

#if defined(__linux__)
#include "my_config-linux.h"
#endif

#if defined(__APPLE__)
#include "my_config-osx.h"
#endif

#if defined(_MSC_VER)
#include "my_config-win.h"
#endif

#if defined(_musl_)
#include "my_config-musl.h"
#endif
