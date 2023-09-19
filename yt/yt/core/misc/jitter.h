#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! TRandomGenerator is required to produce numbers only in range [-1, 1].
//! Will crash otherwise
template <CScalable<double> TValue, class TRandomGenerator>
    requires std::is_invocable_r<double, TRandomGenerator>::value
constexpr inline TValue ApplyJitter(TValue average, double jitter, const TRandomGenerator& randomGenerator)
{
    YT_VERIFY(jitter >= 0 && jitter <= 1);

    double rnd = randomGenerator();

    YT_VERIFY(std::abs(rnd) <= 1);

    double multiplier = static_cast<double>(1) + jitter * rnd;

    return average * multiplier;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
