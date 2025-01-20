#pragma once

#include "private.h"

#include <yt/yt/core/bus/bus.h>

#include <yt/yt/core/net/public.h>

namespace NYT::NBus {

////////////////////////////////////////////////////////////////////////////////

//! An in-process variant of IMessageHandler that is used implement local bypass.
struct ILocalMessageHandler
    : public IMessageHandler
{
    DECLARE_INTERFACE_SIGNAL(void(const TError&), Terminated);
};

DEFINE_REFCOUNTED_TYPE(ILocalMessageHandler)

////////////////////////////////////////////////////////////////////////////////

IBusPtr CreateLocalBypassReplyBus(
    const NNet::TNetworkAddress& localAddress,
    ILocalMessageHandlerPtr serverHandler,
    IMessageHandlerPtr clientHandler);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus

