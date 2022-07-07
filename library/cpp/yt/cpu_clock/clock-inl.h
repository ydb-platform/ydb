#pragma once
#ifndef CLOCK_INL_H_
#error "Direct inclusion of this file is not allowed, include clock.h"
// For the sake of sane code completion.
#include "clock.h"
#endif

#ifdef _WIN32
#   include <intrin.h>
#else
#   include <x86intrin.h>
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

inline TCpuInstant GetCpuInstant()
{
    return __rdtsc();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
