#pragma once

#include "private.h"

#include <yt/yt/core/rpc/public.h>

#include <yt/yt/client/api/public.h>

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
    std::optional<TDuration> pingPeriod,
    std::optional<TStickyTransactionParameters> stickyParameters,
    i64 sequenceNumberSourceId,
    TStringBuf capitalizedCreationReason);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NApi::NRpcProxy
