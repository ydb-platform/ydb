#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/rpc/unittests/lib/common.h>

namespace NYT::NRpc {
namespace {

////////////////////////////////////////////////////////////////////////////////

const NLogging::TLogger Logger("HandleChannelFailureTest");

////////////////////////////////////////////////////////////////////////////////

template <class TImpl>
class THandleChannelFailureTestBase
    : public ::testing::Test
{
public:
    IServerPtr CreateServer(const TTestServerHost& serverHost, IMemoryUsageTrackerPtr memoryUsageTracker)
    {
        return TImpl::CreateServer(serverHost.GetPort(), memoryUsageTracker);
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
    TTestServerHost outerServer;
    TTestServerHost innerServer;

    outerServer.InitilizeAddress();
    innerServer.InitilizeAddress();

    auto finally = Finally([&] {
        outerServer.TearDown();
        innerServer.TearDown();
    });

    auto workerPool = NConcurrency::CreateThreadPool(4, "Worker");

    outerServer.InitializeServer(
        this->CreateServer(outerServer, New<TTestNodeMemoryTracker>(32_MB)),
        workerPool->GetInvoker(),
        /*secure*/ false,
        BIND([&] (const TString& address) {
            return this->CreateChannel(address);
        }));

    innerServer.InitializeServer(
        this->CreateServer(innerServer, New<TTestNodeMemoryTracker>(32_MB)),
        workerPool->GetInvoker(),
        /*secure*/ false,
        /*createChannel*/ {});

    {
        auto channel = this->CreateChannel(outerServer.GetAddress());
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
            this->CreateChannel(outerServer.GetAddress()),
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
            this->CreateChannel(outerServer.GetAddress()),
            /*acknowledgementTimeout*/ std::nullopt,
            BIND([&] (const IChannelPtr& /*channel*/, const TError& error) {
                ++failCount;
                ASSERT_TRUE(IsChannelFailureErrorHandled(error));
            }));

        TTestProxy proxy(channel);
        auto req = proxy.GetChannelFailureError();
        req->set_redirection_address(innerServer.GetAddress());
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
