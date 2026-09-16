#include "transaction.h"

#include "transaction_impl.h"

namespace NYT::NApi::NRpcProxy {

////////////////////////////////////////////////////////////////////////////////

NApi::ITransactionPtr CreateTransaction(
    TConnectionPtr connection,
    TClientPtr client,
    NRpc::IChannelPtr channel,
    NTransactionClient::TTransactionId id,
    NTransactionClient::TTimestamp startTimestamp,
    NTransactionClient::ETransactionType type,
    NTransactionClient::EAtomicity atomicity,
    NTransactionClient::EDurability durability,
    TDuration timeout,
    bool pingAncestors,
    std::optional<std::string> pingerAddress,
    std::optional<TDuration> pingPeriod,
    std::optional<TStickyTransactionParameters> stickyParameters,
    i64 sequenceNumberSourceId,
    TStringBuf creationReason)
{
    auto transaction = New<TTransaction>(
        std::move(connection),
        std::move(client),
        std::move(channel),
        id,
        startTimestamp,
        type,
        atomicity,
        durability,
        timeout,
        pingAncestors,
        pingerAddress,
        pingPeriod,
        std::move(stickyParameters),
        sequenceNumberSourceId,
        creationReason);

    transaction->Initialize();

    return transaction;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
