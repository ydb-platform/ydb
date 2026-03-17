#pragma once

#if defined(__arm__) || defined(__ARM__) || defined(__ARM_NEON) || defined(__aarch64__) || defined(_M_ARM64)
#   include "aom_scale_rtcd-arm.h"
#else
#   include "aom_scale_rtcd-linux.h"
#endif
