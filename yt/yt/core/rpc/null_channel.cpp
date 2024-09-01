#include "null_channel.h"

#include "channel.h"

#include <yt/yt/core/ytree/helpers.h>

#include <yt/yt/core/misc/memory_usage_tracker.h>
#include <yt/yt/core/misc/singleton.h>

namespace NYT::NRpc {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TNullChannel
    : public NRpc::IChannel
{
public:
    explicit TNullChannel(const std::string& address)
        : Address_(address)
    { }

    const TString& GetEndpointDescription() const override
    {
        return Address_;
    }

    const IAttributeDictionary& GetEndpointAttributes() const override
    {
        return EmptyAttributes();
    }

    NRpc::IClientRequestControlPtr Send(
        NRpc::IClientRequestPtr /*request*/,
        NRpc::IClientResponseHandlerPtr /*handler*/,
        const NRpc::TSendOptions& /*options*/) override
    {
        return nullptr;
    }

    void Terminate(const TError& /*error*/) override
    { }

    DEFINE_SIGNAL_OVERRIDE(void(const TError&), Terminated);

    int GetInflightRequestCount() override
    {
        return 0;
    }

    const IMemoryUsageTrackerPtr& GetChannelMemoryTracker() override
    {
        return MemoryUsageTracker_;
    }

private:
    const TString Address_;
    const IMemoryUsageTrackerPtr MemoryUsageTracker_ = GetNullMemoryUsageTracker();
};

IChannelPtr CreateNullChannel(const std::string& address)
{
    return New<TNullChannel>(address);
}

////////////////////////////////////////////////////////////////////////////////

class TNullChannelFactory
    : public IChannelFactory
{
public:
    IChannelPtr CreateChannel(const std::string& address) override
    {
        return CreateNullChannel(address);
    }
};

IChannelFactoryPtr GetNullChannelFactory()
{
    return LeakyRefCountedSingleton<TNullChannelFactory>();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
