#include "static_channel_factory.h"

namespace NYT::NRpc {

using namespace NYT::NBus;

////////////////////////////////////////////////////////////////////////////////

TStaticChannelFactoryPtr TStaticChannelFactory::Add(const TString& address, IChannelPtr channel)
{
    YT_VERIFY(ChannelMap.emplace(address, channel).second);
    return this;
}

IChannelPtr TStaticChannelFactory::CreateChannel(const TString& address)
{
    auto it = ChannelMap.find(address);
    if (it == ChannelMap.end()) {
        THROW_ERROR_EXCEPTION("Unknown address %Qv", address);
    }
    return it->second;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
