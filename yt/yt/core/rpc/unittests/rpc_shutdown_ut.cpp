#include <yt/yt/core/rpc/unittests/lib/common.h>

#include <yt/yt/core/actions/future.h>

#include <library/cpp/testing/hook/hook.h>

namespace NYT::NRpc {
namespace {

template <class TImpl>
using TRpcShutdownTest = TRpcTestBase<TImpl>;

TYPED_TEST_SUITE(TRpcShutdownTest, TAllTransports);

Y_TEST_HOOK_BEFORE_RUN(GTEST_YT_RPC_SHUTDOWN)
{
    GTEST_FLAG_SET(death_test_style, "threadsafe");
}

////////////////////////////////////////////////////////////////////////////////

template <class TTestProxy>
void TestShutdown(const IChannelPtr& channel)
{
    TTestProxy proxy(channel);

    constexpr int numberOfRequests = 1000;
    std::vector<TFuture<typename TTypedClientResponse<NTestRpc::TRspSomeCall>::TResult>> futures;
    futures.reserve(numberOfRequests);

    for (int i = 0; i < numberOfRequests; ++i) {
        auto req = proxy.SomeCall();
        req->SetTimeout(TDuration::Seconds(1));
        req->set_a(42);
        futures.push_back(req->Invoke());
    }

    Shutdown(TShutdownOptions{TDuration::Seconds(30), true, 127});

    if (!NYT::IsShutdownStarted()) {
        Cerr << "Shutdown was not started" << Endl;
        _exit(2);
    }

    TFileInput shutdownLogInput((GetOutputPath() / "shutdown.log").GetPath());
    TString buffer;
    int exitCode = 1;

    while (shutdownLogInput.ReadLine(buffer)) {
        Cerr << buffer << Endl;
        if (exitCode && buffer.Contains("*** Shutdown completed")) {
            exitCode = 0;
        }
    }

    if (exitCode) {
        Cerr << "Shutdown was NOT completed" << Endl;
    }

    _exit(exitCode);
}

TYPED_TEST(TRpcShutdownTest, Shutdown)
{
    EXPECT_EXIT(TestShutdown<TTestProxy>(this->CreateChannel()), ::testing::ExitedWithCode(0), "");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc
