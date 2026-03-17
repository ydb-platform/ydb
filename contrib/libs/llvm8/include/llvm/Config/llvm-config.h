#pragma once

#if defined(__linux__)
#include "llvm-config-linux.h"
#endif

#if defined(__APPLE__)
#include "llvm-config-osx.h"
#endif

#if defined(_MSC_VER)
#include "llvm-config-win.h"
#endif
