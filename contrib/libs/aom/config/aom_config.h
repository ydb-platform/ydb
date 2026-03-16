#pragma once

#if defined(__arm__) || defined(__ARM__) || defined(__ARM_NEON) || defined(__aarch64__) || defined(_M_ARM64)
#   include "aom_config-arm.h"
#else
#   include "aom_config-linux.h"
#endif
