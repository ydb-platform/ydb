#pragma once

#if defined(__APPLE__)
#   include "llvm-config-osx.h"
#elif defined(_MSC_VER)
#   include "llvm-config-win.h"
#else
#   include "llvm-config-linux.h"
#endif
