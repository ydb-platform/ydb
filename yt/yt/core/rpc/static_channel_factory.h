#pragma once

#include "public.h"
#include "channel.h"
#include "helpers.h"

#include <library/cpp/yt/threading/atomic_object.h>

namespace NYT::NRpc {

////////////////////////////////////////////////////////////////////////////////

class TStaticChannelFactory
    : public IChannelFactory
{
public:
    TStaticChannelFactoryPtr Add(const std::string& address, IChannelPtr channel);
    IChannelPtr CreateChannel(const std::string& address) override;

private:
    NThreading::TAtomicObject<THashMap<std::string, IChannelPtr>> ChannelMap;
};

DEFINE_REFCOUNTED_TYPE(TStaticChannelFactory)

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
