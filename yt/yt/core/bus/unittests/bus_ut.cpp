#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/net/socket.h>

#include <yt/yt/core/misc/fs.h>

#include <library/cpp/testing/common/network.h>

#include <library/cpp/yt/threading/event_count.h>

namespace NYT::NBus {
namespace {

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateMessage(int numParts, int partSize = 1)
{
    auto data = TSharedMutableRef::Allocate(numParts * partSize);

    std::vector<TSharedRef> parts;
    for (int i = 0; i < numParts; ++i) {
        parts.push_back(data.Slice(i * partSize, (i + 1) * partSize));
    }

    return TSharedRefArray(std::move(parts), TSharedRefArray::TMoveParts{});
}

TSharedRefArray Serialize(TString str)
{
    return TSharedRefArray(TSharedRef::FromString(str));
}

TString Deserialize(TSharedRefArray message)
{
    YT_ASSERT(message.Size() == 1);
    const auto& part = message[0];
    return TString(part.Begin(), part.Size());
}

////////////////////////////////////////////////////////////////////////////////

class TEmptyBusHandler
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        Y_UNUSED(message);
        Y_UNUSED(replyBus);
    }
};

class TCountingBusHandler
    : public IMessageHandler
{
public:
    void HandleMessage(
        TSharedRefArray /*message*/,
        IBusPtr /*replyBus*/) noexcept override
    {
        Count++;
    }

    std::atomic<int> Count = 0;
};

class TReplying42BusHandler
    : public IMessageHandler
{
public:
    explicit TReplying42BusHandler(int numParts)
        : NumPartsExpecting_(numParts)
    { }

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        EXPECT_EQ(NumPartsExpecting_, std::ssize(message));
        auto replyMessage = Serialize("42");
        YT_UNUSED_FUTURE(replyBus->Send(replyMessage));
    }

private:
    const int NumPartsExpecting_;
};

class TChecking42BusHandler
    : public IMessageHandler
{
public:
    explicit TChecking42BusHandler(int numRepliesWaiting)
        : NumRepliesWaiting_(numRepliesWaiting)
    { }

    void WaitUntilDone()
    {
        Event_.Wait();
    }

private:
    std::atomic<int> NumRepliesWaiting_;
    NThreading::TEvent Event_;

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr /*replyBus*/) noexcept override
    {
        auto value = Deserialize(message);
        EXPECT_EQ("42", value);

        if (--NumRepliesWaiting_ == 0) {
            Event_.NotifyAll();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TBusTest
    : public testing::Test
{
public:
    NTesting::TPortHolder Port;
    TString Address;

    TBusTest()
    {
        Port = NTesting::GetFreePort();
        Address = Format("localhost:%v", Port);
    }

    IBusServerPtr StartBusServer(IMessageHandlerPtr handler)
    {
        auto config = TBusServerConfig::CreateTcp(Port);
        auto server = CreateBusServer(config);
        server->Start(handler);
        return server;
    }

    void TestReplies(int numRequests, int numParts, EDeliveryTrackingLevel level = EDeliveryTrackingLevel::Full)
    {
        auto server = StartBusServer(New<TReplying42BusHandler>(numParts));
        auto client = CreateBusClient(TBusClientConfig::CreateTcp(Address));
        auto handler = New<TChecking42BusHandler>(numRequests);
        auto bus = client->CreateBus(handler);
        auto message = CreateMessage(numParts);

        std::vector<TFuture<void>> results;
        for (int i = 0; i < numRequests; ++i) {
            auto result = bus->Send(message, {.TrackingLevel = level});
            if (result) {
                results.push_back(result);
            }
        }

        for (const auto& result : results) {
            auto error = result.Get();
            EXPECT_TRUE(error.IsOK());
        }

        handler->WaitUntilDone();

        server->Stop()
            .Get()
            .ThrowOnError();
    }
};

////////////////////////////////////////////////////////////////////////////////

TEST_F(TBusTest, ConfigDefaultConstructor)
{
    auto config = New<TBusClientConfig>();
}

TEST_F(TBusTest, CreateBusClientConfig)
{
    auto config = TBusClientConfig::CreateTcp(Address);
    EXPECT_EQ(Address, *config->Address);
    EXPECT_FALSE(config->UnixDomainSocketPath);
}

TEST_F(TBusTest, CreateUdsBusClientConfig)
{
    auto config = TBusClientConfig::CreateUds("unix-socket");
    EXPECT_EQ("unix-socket", *config->UnixDomainSocketPath);
}

TEST_F(TBusTest, OK)
{
    auto server = StartBusServer(New<TEmptyBusHandler>());
    auto client = CreateBusClient(TBusClientConfig::CreateTcp(Address));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full})
        .Get();
    EXPECT_TRUE(result.IsOK());
    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TBusTest, Terminate)
{
    auto server = StartBusServer(New<TEmptyBusHandler>());
    auto client = CreateBusClient(TBusClientConfig::CreateTcp(Address));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);

    auto terminated = NewPromise<void>();
    bus->SubscribeTerminated(
        BIND([&] (const TError& error) {
            terminated.Set(error);
        }));
    auto error = TError(TErrorCode(54321), "Terminated");
    bus->Terminate(error);
    bus->Terminate(TError(TErrorCode(12345), "Ignored"));
    EXPECT_EQ(terminated.Get().GetCode(), error.GetCode());
    bus->Terminate(TError(TErrorCode(12345), "Ignored"));

    auto result = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(result.IsSet());
    EXPECT_EQ(result.Get().GetCode(), error.GetCode());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TBusTest, TerminateBeforeAccept)
{
    /* make blocking server socket */
    auto serverSocket = socket(AF_INET6, SOCK_STREAM, IPPROTO_TCP);
    EXPECT_NE(serverSocket, INVALID_SOCKET);
    NNet::SetReuseAddrFlag(serverSocket);
    NNet::BindSocket(serverSocket, NNet::TNetworkAddress::CreateIPv6Loopback(Port));
    NNet::ListenSocket(serverSocket, 0);

    auto client = CreateBusClient(TBusClientConfig::CreateTcp(Address));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    auto terminated = NewPromise<void>();
    bus->SubscribeTerminated(
        BIND([&] (const TError& error) {
            terminated.Set(error);
        }));
    auto error = TError(TErrorCode(54321), "Terminated");
    bus->Terminate(error);
    EXPECT_FALSE(terminated.IsSet());

    NNet::TNetworkAddress clientAddress;
    auto clientSocket = NNet::AcceptSocket(serverSocket, &clientAddress);
    EXPECT_NE(clientSocket, INVALID_SOCKET);

    EXPECT_EQ(terminated.Get().GetCode(), error.GetCode());

    NNet::CloseSocket(clientSocket);
    NNet::CloseSocket(serverSocket);
}

TEST_F(TBusTest, Failed)
{
    auto port = NTesting::GetFreePort();

    auto client = CreateBusClient(TBusClientConfig::CreateTcp(Format("localhost:%v", port)));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}).Get();
    EXPECT_FALSE(result.IsOK());
}

TEST_F(TBusTest, BlackHole)
{
    auto server = StartBusServer(New<TEmptyBusHandler>());
    auto config = TBusClientConfig::CreateTcp(Address);

    config->ReadStallTimeout = TDuration::Seconds(1);

    auto client = CreateBusClient(config);
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto options = TSendOptions{.TrackingLevel = EDeliveryTrackingLevel::Full};

    bus->Send(message, options)
        .Get()
        .ThrowOnError();

    bus->SetTosLevel(BlackHoleTosLevel);

    auto result = bus->Send(message, options).Get();
    EXPECT_FALSE(result.IsOK());

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TBusTest, SendCancel)
{
    auto handler = New<TCountingBusHandler>();
    auto server = StartBusServer(handler);
    auto client = CreateBusClient(TBusClientConfig::CreateTcp(Address));
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(4, 16_MB);

    auto options = TSendOptions{.TrackingLevel = EDeliveryTrackingLevel::Full};
    options.EnableSendCancelation = true;

    for (int i = 0; i < 16; i++) {
        auto future = bus->Send(message, options);
        future.Cancel(TError("Canceled"));
    }

    Sleep(TDuration::Seconds(1));
    ASSERT_LE(handler->Count, 16);
    handler->Count = 0;

    options.TrackingLevel = EDeliveryTrackingLevel::None;
    for (int i = 0; i < 2; i++) {
        bus->Send(message, options).Cancel(TError("Canceled"));
    }

    Sleep(TDuration::Seconds(1));
    ASSERT_LE(handler->Count, 16);
    handler->Count = 0;

    server->Stop()
        .Get()
        .ThrowOnError();
}

TEST_F(TBusTest, OneReplyNoTracking)
{
    TestReplies(1, 1, EDeliveryTrackingLevel::None);
}

TEST_F(TBusTest, OneReplyFullTracking)
{
    TestReplies(1, 1, EDeliveryTrackingLevel::Full);
}

TEST_F(TBusTest, OneReplyErrorOnlyTracking)
{
    TestReplies(1, 1, EDeliveryTrackingLevel::ErrorOnly);
}

TEST_F(TBusTest, ManyReplies)
{
    TestReplies(1000, 100);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NBus
