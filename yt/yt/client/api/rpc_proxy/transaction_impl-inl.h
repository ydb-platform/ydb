#ifndef TRANSACTION_IMPL_INL_H_
#error "Direct inclusion of this file is not allowed, include transaction_impl.h"
// For the sake of sane code completion.
#include "transaction_impl.h"
#endif
#undef TRANSACTION_IMPL_INL_H_

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

template <class T>
T TTransaction::PatchTransactionId(const T& options)
{
    auto copiedOptions = options;
    copiedOptions.TransactionId = Id_;
    return copiedOptions;
}

template <class T>
T TTransaction::PatchTransactionTimestamp(const T& options)
{
    auto copiedOptions = options;
    copiedOptions.Timestamp = StartTimestamp_;
    return copiedOptions;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
