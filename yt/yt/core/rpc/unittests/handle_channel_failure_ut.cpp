#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/unittests/lib/common.h>

namespace NYT::NRpc {
namespace {

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class THandleChannelFailureTestBase
    : public ::testing::Test
{
public:
    TTestServerHostPtr CreateTestServerHost(
        NTesting::TPortHolder port,
        std::vector<IServicePtr> services,
        TTestNodeMemoryTrackerPtr memoryUsageTracker)
    {
        return TImpl::CreateTestServerHost(
            std::move(port),
            std::move(services),
            std::move(memoryUsageTracker));
    }

    IChannelPtr CreateChannel(const TString& address)
    {
        return TImpl::CreateChannel(address, address, {});
    }
};

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
using THandleChannelFailureTest = THandleChannelFailureTestBase<TImpl>;

TYPED_TEST_SUITE(THandleChannelFailureTest, TWithoutUds);

TYPED_TEST(THandleChannelFailureTest, HandleChannelFailureTest)
{
    auto workerPool = NConcurrency::CreateThreadPool(4, "Worker");

    auto outerMemoryUsageTracker = New<TTestNodeMemoryTracker>(32_MB);
    auto innerMemoryUsageTracker = New<TTestNodeMemoryTracker>(32_MB);

    auto outerServices = std::vector<IServicePtr>{
        CreateTestService(
            workerPool->GetInvoker(),
            false,
            BIND([&] (const TString& address) {
                return this->CreateChannel(address);
            }),
            outerMemoryUsageTracker),
        CreateNoBaggageService(workerPool->GetInvoker())
    };

    auto innerServices = std::vector<IServicePtr>{
        CreateTestService(
            workerPool->GetInvoker(),
            false,
            BIND([&] (const TString& address) {
                return this->CreateChannel(address);
            }),
            innerMemoryUsageTracker),
        CreateNoBaggageService(workerPool->GetInvoker())
    };

    TTestServerHostPtr outerHost = this->CreateTestServerHost(
        NTesting::GetFreePort(),
        outerServices,
        outerMemoryUsageTracker);

    TTestServerHostPtr innerHost = this->CreateTestServerHost(
        NTesting::GetFreePort(),
        innerServices,
        innerMemoryUsageTracker);

    auto finally = Finally([&] {
        outerHost->TearDown();
        innerHost->TearDown();
    });

    {
        auto channel = this->CreateChannel(outerHost->GetAddress());
        TTestProxy proxy(channel);
        auto req = proxy.GetChannelFailureError();
        auto error = req->Invoke().Get();
        ASSERT_FALSE(error.IsOK());
        ASSERT_TRUE(error.FindMatching(NRpc::EErrorCode::Unavailable));
        ASSERT_TRUE(IsChannelFailureErrorHandled(error));
    }

    {
        int failCount = 0;

        auto channel = CreateFailureDetectingChannel(
            this->CreateChannel(outerHost->GetAddress()),
            /*acknowledgementTimeout*/ std::nullopt,
            BIND([&] (const IChannelPtr& /*channel*/, const TError& error) {
                ++failCount;
                ASSERT_FALSE(IsChannelFailureErrorHandled(error));
            }));

        TTestProxy proxy(channel);
        auto req = proxy.GetChannelFailureError();
        auto error = req->Invoke().Get();
        ASSERT_FALSE(error.IsOK());
        ASSERT_TRUE(error.FindMatching(NRpc::EErrorCode::Unavailable));
        ASSERT_TRUE(IsChannelFailureErrorHandled(error));
        ASSERT_EQ(1, failCount);
    }

    {
        int failCount = 0;

        auto channel = CreateFailureDetectingChannel(
            this->CreateChannel(outerHost->GetAddress()),
            /*acknowledgementTimeout*/ std::nullopt,
            BIND([&] (const IChannelPtr& /*channel*/, const TError& error) {
                ++failCount;
                ASSERT_TRUE(IsChannelFailureErrorHandled(error));
            }));

        TTestProxy proxy(channel);
        auto req = proxy.GetChannelFailureError();
        req->set_redirection_address(innerHost->GetAddress());
        auto error = req->Invoke().Get();
        ASSERT_FALSE(error.IsOK());
        ASSERT_TRUE(error.FindMatching(NRpc::EErrorCode::Unavailable));
        ASSERT_TRUE(IsChannelFailureErrorHandled(error));
        ASSERT_EQ(1, failCount);
    }
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
