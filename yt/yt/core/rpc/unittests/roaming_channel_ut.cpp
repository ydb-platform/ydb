#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/roaming_channel.h>

#include <yt/yt/core/rpc/unittests/lib/common.h>

#include <yt/yt/core/tracing/trace_context.h>

#include <yt/yt/library/tracing/tracer.h>

#include <util/generic/hash.h>

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

    const std::string& GetEndpointDescription() const override
    {
        return EndpointAddress_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return Channel_->GetEndpointAttributes();
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
    const std::string EndpointAddress_;
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

    const std::string& GetEndpointDescription() const override
    {
        return EndpointAddress_;
    }

    const NYTree::IAttributeDictionary& GetEndpointAttributes() const override
    {
        return *EndpointAttributes_;
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
    const std::string EndpointAddress_;
    const NYTree::IAttributeDictionaryPtr EndpointAttributes_ = NYTree::CreateEphemeralAttributes();
};

class TNeverProvider
    : public IRoamingChannelProvider
{
public:
    const std::string& GetEndpointDescription() const override
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

////////////////////////////////////////////////////////////////////////////////

class TTracer
    : public NTracing::ITracer
{
public:
    void Enqueue(NTracing::TTraceContextPtr trace) override
    {
        auto guard = Guard(Spinlock_);
        TracesBySpanName_.emplace(trace->GetSpanName(), std::move(trace));
    }

    void Stop() override
    { }

    std::optional<std::string> FindTraceTag(const std::string& spanName, const std::string& tagName) const
    {
        auto guard = Guard(Spinlock_);
        auto traceIt = TracesBySpanName_.find(spanName);
        if (traceIt != TracesBySpanName_.end()) {
            for (const auto& [key, value] : traceIt->second->GetTags()) {
                if (key == tagName) {
                    return value;
                }
            }
        }
        return std::nullopt;
    }

private:
    YT_DECLARE_SPIN_LOCK(NThreading::TSpinLock, Spinlock_);
    THashMap<std::string, NTracing::TTraceContextPtr> TracesBySpanName_;
};

struct TTracerGuard
{
    TTracerGuard(const NTracing::ITracerPtr& tracer)
        : PreviousTracer_(NTracing::GetGlobalTracer())
    {
        NTracing::SetGlobalTracer(tracer);
    }

    ~TTracerGuard()
    {
        NTracing::SetGlobalTracer(PreviousTracer_);
    }

    TTracerGuard(TTracerGuard&&) = delete;
    TTracerGuard& operator=(TTracerGuard&&) = delete;

private:
    const NTracing::ITracerPtr PreviousTracer_;
};

////////////////////////////////////////////////////////////////////////////////

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
    ASSERT_TRUE(rspOrError.IsOK())
        << ToString(rspOrError);
    const auto& rsp = rspOrError.Value();
    EXPECT_EQ(142, rsp->b());
}

TYPED_TEST(TRpcTest, RoamingChannelEndpointAddressPropogation)
{
    auto manualProvider = New<TManualProvider>();
    auto channel = CreateRoamingChannel(New<TOneChannelProvider>(CreateRoamingChannel(manualProvider)));
    auto manualProviderWeak = MakeWeak(manualProvider);
    manualProvider.Reset();
    std::string endpointAddress;
    {
        auto strong = manualProviderWeak.Lock();
        YT_ASSERT(strong);
        auto channel = this->CreateChannel();
        endpointAddress = channel->GetEndpointDescription();
        strong->SetChannel(channel);
    }
    if (endpointAddress.starts_with("unix://")) {
        // Do not support unix domain sockets yet.
        return;
    }

    auto retryingConfig = New<TRetryingChannelConfig>();
    retryingConfig->Load(ConvertTo<NYTree::INodePtr>(NYson::TYsonString(TStringBuf("{}"))));
    channel = CreateRetryingChannel(
        std::move(retryingConfig),
        std::move(channel),
        BIND([] (const TError&) {
            return false;
        }));

    TTestProxy proxy(std::move(channel));
    for (bool recorded : {true, false}) {
        auto tracer = New<TTracer>();
        TTracerGuard tracerGuard(tracer);

        auto rootTraceContext = NTracing::CreateTraceContextFromCurrent("root");
        rootTraceContext->SetSampled(recorded);
        auto contextGuard = NTracing::TTraceContextGuard(rootTraceContext);

        auto req = proxy.DoNothing();
        req->SetResponseHeavy(true);
        req->SetMultiplexingBand(NRpc::EMultiplexingBand::Interactive);
        req->TracingTags().emplace_back("yt.query", "test query");
        auto asyncRspOrError = req->Invoke()
            .WithTimeout(TDuration::Seconds(1));

        auto responseOrError = NConcurrency::WaitFor(asyncRspOrError);
        ASSERT_TRUE(responseOrError.IsOK())
            << ToString(responseOrError);

        auto endpoint = tracer->FindTraceTag("RpcClient:TestService.DoNothing", EndpointAddressAnnotation);
        if (recorded) {
            EXPECT_EQ(endpointAddress, endpoint);
        } else {
            EXPECT_EQ(std::nullopt, endpoint);
        }

        auto slowReq = proxy.SlowCall();
        slowReq->SetResponseHeavy(true);
        slowReq->SetMultiplexingBand(NRpc::EMultiplexingBand::Interactive);
        slowReq->TracingTags().emplace_back("yt.query", "test query");
        slowReq->SetTimeout(TDuration::MilliSeconds(100));
        auto error = NConcurrency::WaitFor(slowReq->Invoke());
        ASSERT_FALSE(error.IsOK());

        endpoint = tracer->FindTraceTag("RpcClient:TestService.SlowCall", EndpointAddressAnnotation);
        if (recorded) {
            EXPECT_EQ(endpointAddress, endpoint);
        } else {
            EXPECT_EQ(std::nullopt, endpoint);
        }
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
