#pragma once

#include "client.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

struct IStickyTransactionPool
    : public virtual TRefCounted
{
    //! Registers transaction in the pool.
    virtual ITransactionPtr RegisterTransaction(ITransactionPtr transaction) = 0;

    //! Unregisters transaction from the pool.
    virtual void UnregisterTransaction(NTransactionClient::TTransactionId transactionId) = 0;

    //! Finds a transaction by id and renews its lease. Returns |nullptr| if transaction is not found.
    virtual ITransactionPtr FindTransactionAndRenewLease(NTransactionClient::TTransactionId transactionId) = 0;

    //! Finds a transaction by id and renews its lease. Throws if transaction is not found.
    ITransactionPtr GetTransactionAndRenewLeaseOrThrow(NTransactionClient::TTransactionId transactionId);
};

DEFINE_REFCOUNTED_TYPE(IStickyTransactionPool)

////////////////////////////////////////////////////////////////////////////////

IStickyTransactionPoolPtr CreateStickyTransactionPool(const NLogging::TLogger& logger);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
