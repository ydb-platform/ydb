#pragma once

#include "channel.h"

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

struct IThrottlingChannel
    : public virtual IChannel
{
    virtual void Reconfigure(const TThrottlingChannelDynamicConfigPtr& config) = 0;
};

DEFINE_REFCOUNTED_TYPE(IThrottlingChannel)

//! Constructs a channel that limits request rate to the underlying channel.
IThrottlingChannelPtr CreateThrottlingChannel(
    TThrottlingChannelConfigPtr config,
    IChannelPtr underlyingChannel);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
