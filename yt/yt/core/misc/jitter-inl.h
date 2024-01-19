#ifndef JITTER_INL_H_
#error "Direct inclusion of this file is not allowed, include jitter.h"
// For the sake of sane code completion.
#include "jitter.h"
#endif

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <CScalable<double> TValue, CInvocable<double()> TRandomGenerator>
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
