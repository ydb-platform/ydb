#ifndef TRANSACTION_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction.h"
// For the sake of sane code completion.
#include "transaction.h"
#endif

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

template <class TDerivedTransaction>
TDerivedTransaction* ITransaction::As()
{
    auto* derived = dynamic_cast<TDerivedTransaction*>(this);
    YT_VERIFY(derived);
    return derived;
}

template <class TDerivedTransaction>
TDerivedTransaction* ITransaction::TryAs()
{
    return dynamic_cast<TDerivedTransaction*>(this);
}

template <class TDerivedTransaction>
const TDerivedTransaction* ITransaction::As() const
{
    const auto* derived = dynamic_cast<const TDerivedTransaction*>(this);
    YT_VERIFY(derived);
    return derived;
}

template <class TDerivedTransaction>
const TDerivedTransaction* ITransaction::TryAs() const
{
    return dynamic_cast<const TDerivedTransaction*>(this);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi

