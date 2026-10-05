#include <yt/yt/core/rpc/unittests/lib/common.h>

#include <yt/yt/core/rpc/config.h>
#include <yt/yt/core/rpc/multi_protocol_channel_factory.h>
#include <yt/yt/core/rpc/multi_protocol_server.h>

namespace NYT::NRpc::NHttp {
namespace {

using namespace NConcurrency;

////////////////////////////////////////////////////////////////////////////////

// Exercises the multi-server and multi-channel-factory wiring over the "http"
// backend: a service registered on the multi-server must be reachable through a
// channel produced by the multi-channel factory.
class TMultiProtocolRpcTest
    : public ::testing::Test
{
public:
    void SetUp() final
    {
        WorkerPool_ = CreateThreadPool(1, "Worker");
        Port_ = NTesting::GetFreePort();

        TestService_ = CreateTestService(
            WorkerPool_->GetInvoker(),
            /*secure*/ false,
            /*createChannel*/ {},
            GetNullMemoryUsageTracker(),
            /*useAuthenticator*/ false);

        auto serverConfig = ConvertTo<TMultiProtocolServerConfigPtr>(NYTree::BuildYsonNodeFluently()
            .BeginMap()
                .Item("http").BeginMap()
                    .Item("port").Value(static_cast<int>(Port_))
                .EndMap()
            .EndMap());
        Server_ = CreateMultiProtocolServer(serverConfig);
        Server_->RegisterService(TestService_);
        Server_->Start();

        ChannelFactory_ = CreateMultiProtocolChannelFactory(New<TMultiProtocolClientConfig>());
    }

    void TearDown() final
    {
        WaitFor(Server_->Stop())
            .ThrowOnError();
    }

    IChannelPtr CreateChannel(const std::string& address)
    {
        return ChannelFactory_->CreateChannel(address);
    }

    std::string GetAddress() const
    {
        return Format("localhost:%v", static_cast<ui16>(Port_));
    }

private:
    IThreadPoolPtr WorkerPool_;
    NTesting::TPortHolder Port_;
    ITestServicePtr TestService_;
    IServerPtr Server_;
    IChannelFactoryPtr ChannelFactory_;
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TMultiProtocolRpcTest, SendWithExplicitProtocol)
{
    TTestProxy proxy(CreateChannel(Format("http://%v", GetAddress())));
    auto req = proxy.SomeCall();
    req->set_a(42);
    auto rspOrError = WaitFor(req->Invoke());
    EXPECT_TRUE(rspOrError.IsOK()) << ToString(rspOrError);
    EXPECT_EQ(142, rspOrError.Value()->b());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NRpc::NHttp
