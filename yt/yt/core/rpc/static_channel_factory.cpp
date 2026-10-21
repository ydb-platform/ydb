#include "static_channel_factory.h"

namespace NYT::NRpc {

using namespace NYT::NBus;

////////////////////////////////////////////////////////////////////////////////

TStaticChannelFactoryPtr TStaticChannelFactory::Add(const std::string& address, IChannelPtr channel)
{
    ChannelMap.Transform([&] (auto& channelMap) {
        YT_VERIFY(channelMap.emplace(address, channel).second);
    });
    return this;
}

IChannelPtr TStaticChannelFactory::CreateChannel(const std::string& address)
{
    return ChannelMap.Read([&] (const auto& channelMap) {
        auto it = channelMap.find(address);
        if (it == channelMap.end()) {
            THROW_ERROR_EXCEPTION("Unknown address %Qv", address);
        }
        return it->second;
    });
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
