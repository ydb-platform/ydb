#include <yt/yt/core/rpc/unittests/lib/common.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/testing/hook/hook.h>

namespace NYT::NRpc {
namespace {

template <class TImpl>
using TRpcShutdownTest = TTestBase<TImpl>;

TYPED_TEST_SUITE(TRpcShutdownTest, TAllTransports);

Y_TEST_HOOK_BEFORE_RUN(GTEST_YT_RPC_SHUTDOWN)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
}

////////////////////////////////////////////////////////////////////////////////

template <class TMyProxy>
void TestShutdown(const IChannelPtr& channel)
{
    TMyProxy proxy(channel);

    std::vector<NYT::TFuture<typename TTypedClientResponse<NMyRpc::TRspSomeCall>::TResult>> futures;
    futures.reserve(100000);
    for (int i = 0; i < 100000; ++i) {
        auto req = proxy.SomeCall();
        req->SetTimeout(TDuration::Seconds(1));
        req->set_a(42);
        futures.push_back(req->Invoke());
    }

    NYT::Shutdown();

    for (auto& future : futures) {
        future.Cancel(TError{});
    }

    _exit(0);
}

TYPED_TEST(TRpcShutdownTest, Shutdown)
{
    EXPECT_EXIT(TestShutdown<TMyProxy>(this->CreateChannel()), testing::ExitedWithCode(0), "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
