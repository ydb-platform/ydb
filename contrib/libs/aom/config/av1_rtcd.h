#pragma once

#if defined(__arm__) || defined(__ARM__) || defined(__ARM_NEON) || defined(__aarch64__) || defined(_M_ARM64)
#   include "av1_rtcd-arm.h"
#else
#   include "av1_rtcd-linux.h"
#endif
