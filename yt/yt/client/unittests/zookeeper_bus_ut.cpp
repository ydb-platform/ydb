#include <yt/yt/core/test_framework/framework.h>

#include <yt/yt/client/zookeeper/packet.h>

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/bus/tcp/config.h>
#include <yt/yt/core/bus/tcp/client.h>
#include <yt/yt/core/bus/tcp/server.h>

#include <yt/yt/core/net/socket.h>

#include <library/cpp/testing/common/network.h>

#include <library/cpp/yt/threading/event_count.h>

namespace NYT::NZookeeper {
namespace {

using namespace NBus;

////////////////////////////////////////////////////////////////////////////////

TSharedRefArray CreateMessage(int size)
{
    auto data = TSharedMutableRef::Allocate(size);
    return TSharedRefArray(TSharedRef(data));
}

TSharedRefArray Serialize(TString message)
{
    return TSharedRefArray(TSharedRef::FromString(message));
}

TString Deserialize(TSharedRefArray message)
{
    YT_ASSERT(message.Size() == 1);
    const auto& part = message[0];
    return TString(part.Begin(), part.Size());
}

////////////////////////////////////////////////////////////////////////////////

class TReplyingBusHandler
    : public IMessageHandler
{
public:
    TReplyingBusHandler(TString message)
        : Message_(std::move(message))
    { }

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr replyBus) noexcept override
    {
        EXPECT_EQ(1, std::ssize(message));
        auto replyMessage = Serialize(Message_);
        YT_UNUSED_FUTURE(replyBus->Send(replyMessage));
    }

private:
    const TString Message_;
};

class TCheckingBusHandler
    : public IMessageHandler
{
public:
    explicit TCheckingBusHandler(int numRepliesWaiting, TString message)
        : Message_(std::move(message))
        , NumRepliesWaiting(numRepliesWaiting)
    { }

    void WaitUntilDone()
    {
        Event_.Wait();
    }

private:
    const TString Message_;

    std::atomic<int> NumRepliesWaiting;
    NThreading::TEvent Event_;

    void HandleMessage(
        TSharedRefArray message,
        IBusPtr /*replyBus*/) noexcept override
    {
        auto value = Deserialize(message);
        EXPECT_EQ(Message_, value);

        if (--NumRepliesWaiting == 0) {
            Event_.NotifyAll();
        }
    }
};

////////////////////////////////////////////////////////////////////////////////

class TZookeeperBusTest
    : public testing::Test
{
public:
    NTesting::TPortHolder Port;
    TString Address;

    TZookeeperBusTest()
    {
        Port = NTesting::GetFreePort();
        Address = Format("localhost:%v", Port);
    }

    IBusServerPtr StartBusServer(IMessageHandlerPtr handler)
    {
        auto config = TBusServerConfig::CreateTcp(Port);
        auto server = CreateBusServer(
            config,
            GetZookeeperPacketTranscoderFactory());
        server->Start(handler);
        return server;
    }

    void TestReplies(int numRequests, const TString& message)
    {
        auto server = StartBusServer(New<TReplyingBusHandler>(message));
        auto client = CreateBusClient(
            TBusClientConfig::CreateTcp(Address),
            GetZookeeperPacketTranscoderFactory());
        auto handler = New<TCheckingBusHandler>(numRequests, message);
        auto bus = client->CreateBus(handler);

        std::vector<TFuture<void>> results;
        for (int i = 0; i < numRequests; ++i) {
            if (auto result = bus->Send(CreateMessage(10))) {
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

TEST_F(TZookeeperBusTest, Simple)
{
    TestReplies(1, "42");
    TestReplies(100, "abacaba");
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NZookeeper
