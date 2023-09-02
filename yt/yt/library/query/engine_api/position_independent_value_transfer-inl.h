#ifndef POSITION_INDEPENDENT_VALUE_TRANSFER_INL_H
#error "Direct inclusion of this file is not allowed, position_independent_value_transfer.h"
// For the sake of sane code completion.
#include "position_independent_value_transfer.h"
#endif

namespace NYT::NQueryClient {

////////////////////////////////////////////////////////////////////////////////

// NB(dtorilov): in WebAssembly case this function should use compartment's memory base.
template <class TNonPI>
TBorrowingPIValueGuard<TNonPI> BorrowFromNonPI(TNonPI value)
{
    return TBorrowingPIValueGuard<TNonPI>(value);
}

// NB(dtorilov): in WebAssembly case this function should use compartment's memory base.
template <class TPI>
TBorrowingNonPIValueGuard<TPI> BorrowFromPI(TPI value)
{
    return TBorrowingNonPIValueGuard<TPI>(value);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NQueryClient
