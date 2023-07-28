#pragma once

#include "public.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct THedgingChannelOptions
{
    IHedgingManagerPtr HedgingManager;
    bool CancelPrimaryOnHedging = false;
};

//! The resulting channel initially forwards a request to #primaryChannel and
//! if no response comes within delay, re-sends the request to #backupChannel
//! (optionally canceling the initial request).
//! Whatever underlying channel responds first is the winner.
//! HedgingManager determines hedging delay and may restrain channel from sending backup request.
IChannelPtr CreateHedgingChannel(
    IChannelPtr primaryChannel,
    IChannelPtr backupChannel,
    const THedgingChannelOptions& options);

//! Returns |true| if #response was received from backup.
bool IsBackup(const TClientResponsePtr& response);

//! Returns |true| if #responseOrError was received from backup.
template <class T>
bool IsBackup(const TErrorOr<TIntrusivePtr<T>>& responseOrError);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc

#define HEDGING_CHANNEL_INL_H_
#include "hedging_channel-inl.h"
#undef HEDGING_CHANNEL_INL_H_
