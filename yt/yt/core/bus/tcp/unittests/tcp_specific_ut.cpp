#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/bus/unittests/lib/handlers.h>
#include <yt/yt/core/bus/unittests/lib/helpers.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <yt/yt/core/net/socket.h>

#include <library/cpp/testing/common/network.h>

namespace NYT::NBus::NTcp::NTests {
namespace {

using namespace NConcurrency;
using namespace NYT::NBus::NTests;

////////////////////////////////////////////////////////////////////////////////

class TBusTest
    : public ::testing::Test
{
protected:
    NTesting::TPortHolder Port_;
    std::string Address_;

    TBusTest()
        : Port_(NTesting::GetFreePort())
        , Address_(Format("localhost:%v", Port_))
    { }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TBusTest, ClientConfigDefaultConstructor)
{
    auto config = New<TBusClientConfig>();
    EXPECT_FALSE(config->Address);
    EXPECT_FALSE(config->UnixDomainSocketPath);
}

TEST_F(TBusTest, ServerConfigDefaultConstructor)
{
    auto config = New<TBusServerConfig>();
    EXPECT_FALSE(config->Port);
    EXPECT_FALSE(config->UnixDomainSocketPath);
}

TEST_F(TBusTest, CreateBusClientConfig)
{
    auto config = TBusClientConfig::CreateTcp(Address_);
    EXPECT_EQ(Address_, *config->Address);
    EXPECT_FALSE(config->UnixDomainSocketPath);
}

TEST_F(TBusTest, CreateUdsBusClientConfig)
{
    auto config = TBusClientConfig::CreateUds("unix-socket");
    EXPECT_EQ("unix-socket", *config->UnixDomainSocketPath);
}

TEST_F(TBusTest, TerminateBeforeAccept)
{
    auto serverSocket = socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP);
    EXPECT_NE(serverSocket, INVALID_SOCKET);
    NNet::SetReuseAddrFlag(serverSocket);
    NNet::BindSocket(serverSocket, NNet::TNetworkAddress::CreateIPv6Loopback(Port_));
    NNet::ListenSocket(serverSocket, 0);

    auto client = CreateBusClient(TBusClientConfig::CreateTcp(Address_));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    auto terminated = NewPromise<void>();
    bus->SubscribeTerminated(BIND([&] (const TError& error) {
        terminated.Set(error);
    }));
    auto error = TError(TErrorCode(54321), "Terminated");
    bus->Terminate(error);
    EXPECT_FALSE(terminated.IsSet());

    NNet::TNetworkAddress clientAddress;
    auto clientSocket = NNet::AcceptSocket(serverSocket, &clientAddress);
    EXPECT_NE(clientSocket, INVALID_SOCKET);

    EXPECT_EQ(WaitForFast(terminated.ToFuture()).GetCode(), error.GetCode());

    NNet::CloseSocket(clientSocket);
    NNet::CloseSocket(serverSocket);
}

TEST_F(TBusTest, BlackHole)
{
    auto serverConfig = TBusServerConfig::CreateTcp(Port_);
    auto server = CreateBusServer(serverConfig);
    server->Start(New<TEmptyBusHandler>());

    auto config = TBusClientConfig::CreateTcp(Address_);
    config->ReadStallTimeout = TDuration::Seconds(1);

    auto client = CreateBusClient(config);
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto options = TSendOptions{.TrackingLevel = EDeliveryTrackingLevel::Full};

    WaitForFast(bus->Send(message, options))
        .ThrowOnError();

    bus->SetTosLevel(BlackHoleTosLevel);

    auto result = WaitForFast(bus->Send(message, options));
    EXPECT_FALSE(result.IsOK());

    WaitForFast(server->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBus::NTcp::NTests
