#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/rpc/unittests/lib/common.h>

namespace NYT::NRpc {
namespace {

////////////////////////////////////////////////////////////////////////////////

class TOneChannelProvider
    : public IRoamingChannelProvider
{
public:
    explicit TOneChannelProvider(IChannelPtr channel)
        : Channel_(std::move(channel))
    { }

    const TString& GetEndpointDescription() const override
    {
        YT_UNIMPLEMENTED();
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        return MakeFuture(Channel_);
    }

    void Terminate(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const IChannelPtr Channel_;
};

class TManualProvider
    : public IRoamingChannelProvider
{
public:
    void SetChannel(IChannelPtr channel)
    {
        YT_ASSERT(!Channel_.IsSet());
        Channel_.Set(std::move(channel));
    }

    const TString& GetEndpointDescription() const override
    {
        YT_UNIMPLEMENTED();
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        return Channel_.ToFuture();
    }

    void Terminate(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const TPromise<IChannelPtr> Channel_ = NewPromise<IChannelPtr>();
};

class TNeverProvider
    : public IRoamingChannelProvider
{
public:
    const TString& GetEndpointDescription() const override
    {
        YT_UNIMPLEMENTED();
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        YT_UNIMPLEMENTED();
    }

    TFuture<IChannelPtr> GetChannel(const IClientRequestPtr& /*request*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel(std::string /*serviceName*/) override
    {
        return GetChannel();
    }

    TFuture<IChannelPtr> GetChannel() override
    {
        return Channel_.ToFuture();
    }

    void Terminate(const TError& /*error*/) override
    {
        YT_UNIMPLEMENTED();
    }

private:
    const TPromise<IChannelPtr> Channel_ = NewPromise<IChannelPtr>();
};

template <class TImpl>
using TRpcTest = TRpcTestBase<TImpl>;
TYPED_TEST_SUITE(TRpcTest, TAllTransports);

TYPED_TEST(TRpcTest, RoamingChannelNever)
{
    auto channel = CreateRoamingChannel(New<TOneChannelProvider>(CreateRoamingChannel(New<TNeverProvider>())));

    TTestProxy proxy(std::move(channel));
    auto req = proxy.SomeCall();
    req->set_a(42);

    auto asyncRspOrError = req->Invoke()
        .WithTimeout(TDuration::Seconds(1));

    auto rspOrError = NConcurrency::WaitFor(asyncRspOrError);
    EXPECT_TRUE(!rspOrError.IsOK() && rspOrError.GetCode() == NYT::EErrorCode::Timeout);
}

TYPED_TEST(TRpcTest, RoamingChannelManual)
{
    auto manualProvider = New<TManualProvider>();

    auto channel = CreateRoamingChannel(New<TOneChannelProvider>(CreateRoamingChannel(manualProvider)));

    auto manualProviderWeak = MakeWeak(manualProvider);
    manualProvider.Reset();

    TTestProxy proxy(std::move(channel));
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto asyncRspOrError = req->Invoke()
        .WithTimeout(TDuration::Seconds(1));

    {
        auto strong = manualProviderWeak.Lock();
        YT_ASSERT(strong);
        strong->SetChannel(this->CreateChannel());
    }

    auto rspOrError = NConcurrency::WaitFor(asyncRspOrError);
    EXPECT_TRUE(rspOrError.IsOK())
        << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ(142, rsp->b());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
