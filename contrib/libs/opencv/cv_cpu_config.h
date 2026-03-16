#pragma once

#if defined(__arm__) || defined(__ARM__)
#   include "cv_cpu_config-armv7a.h"
#elif defined(__ARM_NEON)
#   include "cv_cpu_config-armv7a_neon.h"
#elif defined(__aarch64__) || defined(_M_ARM64)
#   include "cv_cpu_config-armv8a.h"
#elif defined(__powerpc64__)
#   include "cv_cpu_config-ppc64le.h"
#elif defined(__x86_64__) || defined(_M_X64)
#   include "cv_cpu_config-x86_64.h"
#elif defined(__i686__) || defined(_M_IX86)
#   include "cv_cpu_config-x86.h"
#else
#   include "cv_cpu_config-linux.h"
#endif
