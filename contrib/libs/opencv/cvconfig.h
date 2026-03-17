#pragma once

#if defined(__arm__) || defined(__ARM__) || defined(__ARM_NEON) || defined(__aarch64__) || defined(_M_ARM64)
#   include "cvconfig-arm.h"
#elif defined(__powerpc64__)
#   include "cvconfig-ppc64le.h"
#elif defined(__ANDROID__) && defined(__i686__)
#   include "cvconfig-android-x86.h"
#elif defined(__ANDROID__) && defined(__x86_64__)
#   include "cvconfig-android-x86_64.h"
#elif defined(__IOS__)
#   include "cvconfig-ios.h"
#elif defined(__APPLE__)
#   include "cvconfig-osx.h"
#elif defined(_MSC_VER)
#   include "cvconfig-win.h"
#else
#   include "cvconfig-linux.h"
#endif
