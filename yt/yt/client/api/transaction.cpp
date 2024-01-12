#include "transaction.h"

namespace NYT::NApi {

////////////////////////////////////////////////////////////////////////////////

TFuture<ITransactionPtr> StartAlienTransaction(
    const ITransactionPtr& localTransaction,
    const IClientPtr& alienClient,
    const TAlienTransactionStartOptions& options)
{
    YT_VERIFY(localTransaction->GetType() == NTransactionClient::ETransactionType::Tablet);

    if (localTransaction->GetConnection()->IsSameCluster(alienClient->GetConnection())) {
        return MakeFuture(localTransaction);
    }

    return alienClient->StartTransaction(
        NTransactionClient::ETransactionType::Tablet,
        TTransactionStartOptions{
            .Id = localTransaction->GetId(),
            .Atomicity = options.Atomicity,
            .Durability = options.Durability,
            .StartTimestamp = options.StartTimestamp
        }).Apply(BIND([=] (const ITransactionPtr& alienTransaction) {
            localTransaction->RegisterAlienTransaction(alienTransaction);
            return alienTransaction;
        }));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi
