#include "dynamic_channel_pool_provider.h"

#include "dynamic_channel_pool.h"
#include "roaming_channel.h"

namespace NYT::NRpc {

using namespace NYTree;

////////////////////////////////////////////////////////////////////////////////

class TDynamicChannelPoolProviderBase
    : public IRoamingChannelProvider
{
public:
    TDynamicChannelPoolProviderBase(
        TDynamicChannelPoolPtr pool,
        std::string endpointDescription,
        IAttributeDictionaryPtr endpointAttributes)
        : Pool_(std::move(pool))
        , EndpointDescription_(std::move(endpointDescription))
        , EndpointAttributes_(std::move(endpointAttributes))
    { }

    using IRoamingChannelProvider::GetChannel;

    const std::string& GetEndpointDescription() const override
    {
        return EndpointDescription_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
    }

    void Terminate(const TError& /*error*/) override
    {
        // NB: We don't want to terminate the entire pool on terminate of one channel.
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

protected:
    const TDynamicChannelPoolPtr Pool_;

    const std::string EndpointDescription_;
    const IAttributeDictionaryPtr EndpointAttributes_;
};

////////////////////////////////////////////////////////////////////////////////

class TDynamicChannelPoolProvider
    : public TDynamicChannelPoolProviderBase
{
public:
    TDynamicChannelPoolProvider(
        TDynamicChannelPoolPtr pool,
        std::string endpointDescription,
        IAttributeDictionaryPtr endpointAttributes)
        : TDynamicChannelPoolProviderBase(
            std::move(pool),
            std::move(endpointDescription),
            std::move(endpointAttributes))
    { }

    using TDynamicChannelPoolProviderBase::GetChannel;

    TFuture<IChannelPtr> GetChannel() override
    {
        return Pool_->GetRandomChannel();
    }
};

////////////////////////////////////////////////////////////////////////////////

class TStickyDynamicChannelPoolProvider
    : public TDynamicChannelPoolProviderBase
{
public:
    TStickyDynamicChannelPoolProvider(
        TDynamicChannelPoolPtr pool,
        std::string endpointDescription,
        IAttributeDictionaryPtr endpointAttributes)
        : TDynamicChannelPoolProviderBase(
            std::move(pool),
            std::move(endpointDescription),
            std::move(endpointAttributes))
    { }

    using TDynamicChannelPoolProviderBase::GetChannel;

    TFuture<IChannelPtr> GetChannel() override
    {
        auto guard = Guard(SpinLock_);
        if (!Channel_) {
            Channel_ = Pool_->GetRandomChannel();
        }
        return Channel_;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, SpinLock_);
    TFuture<IChannelPtr> Channel_;
};

////////////////////////////////////////////////////////////////////////////////

IRoamingChannelProviderPtr CreateDynamicChannelPoolProvider(
    TDynamicChannelPoolPtr pool,
    std::string endpointDescription,
    IAttributeDictionaryPtr endpointAttributes)
{
    return New<TDynamicChannelPoolProvider>(
        std::move(pool),
        std::move(endpointDescription),
        std::move(endpointAttributes));
}

IRoamingChannelProviderPtr CreateStickyDynamicChannelPoolProvider(
    TDynamicChannelPoolPtr pool,
    std::string endpointDescription,
    IAttributeDictionaryPtr endpointAttributes)
{
    return New<TStickyDynamicChannelPoolProvider>(
        std::move(pool),
        std::move(endpointDescription),
        std::move(endpointAttributes));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NRpc
