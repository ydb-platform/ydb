#pragma once
#ifndef CLOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include clock.h"
// For the sake of sane code completion.
#include "clock.h"
#endif

#if !defined(__arm__) && !defined(__aarch64__)
#   ifdef _WIN32
#       include <intrin.h>
#   else
#       include <x86intrin.h>
#   endif
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline TCpuInstant GetCpuInstant()
{
#if !defined(__arm__) && !defined(__aarch64__)
    return __rdtsc();
#else
    return MicroSeconds();
#endif
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
