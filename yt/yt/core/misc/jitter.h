#pragma once

#include "public.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! TRandomGenerator is required to produce numbers only in range [-1, 1].
//! Will crash otherwise
template <CScalable<double> TValue, CInvocable<double()> TRandomGenerator>
constexpr inline TValue ApplyJitter(TValue average, double jitter, const TRandomGenerator& randomGenerator);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define JITTER_INL_H_
#include "jitter-inl.h"
#undef JITTER_INL_H_
