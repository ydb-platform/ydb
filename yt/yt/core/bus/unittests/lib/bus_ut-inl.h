#ifndef BUS_UT_INL_H_
#error "Direct inclusion of this file is not allowed, include bus_ut.h"
// For the sake of sane code completion.
#include "bus_ut.h"
#endif

#include "handlers.h"
#include "helpers.h"

#include <yt/yt/core/bus/bus.h>
#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <util/datetime/base.h>

#include <thread>
#include <vector>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST_P(TBusTest, OK)
{
    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = NConcurrency::WaitFor(
        bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}));
    EXPECT_TRUE(result.IsOK());
    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, Terminate)
{
    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);

    auto terminated = NewPromise<void>();
    bus->SubscribeTerminated(BIND([&] (const TError& error) {
        terminated.Set(error);
    }));
    auto error = TError(TErrorCode(54321), "Terminated");
    bus->Terminate(error);
    bus->Terminate(TError(TErrorCode(12345), "Ignored"));
    EXPECT_EQ(NConcurrency::WaitFor(terminated.ToFuture()).GetCode(), error.GetCode());
    bus->Terminate(TError(TErrorCode(12345), "Ignored"));

    auto result = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(result.IsSet());
    EXPECT_EQ(NConcurrency::WaitFor(result).GetCode(), error.GetCode());

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, Failed)
{
    auto client = this->Traits_.CreateUnreachableClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = NConcurrency::WaitFor(
        bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}));
    EXPECT_FALSE(result.IsOK());
}

TYPED_TEST_P(TBusTest, DnsResolutionFails)
{
    // RFC 6761 reserves the `.invalid` TLD to never resolve.
    auto client = this->Traits_.CreateClient("this-host-does-not-exist.invalid:1");
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = NConcurrency::WaitFor(
        bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}));
    EXPECT_FALSE(result.IsOK());
}

TYPED_TEST_P(TBusTest, SendCancel)
{
    auto handler = New<TCountingBusHandler>();
    auto server = this->Traits_.StartServer(handler);
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(4, 16_MB);

    auto options = TSendOptions{.TrackingLevel = EDeliveryTrackingLevel::Full};
    options.EnableSendCancelation = true;

    for (int i = 0; i < 16; i++) {
        auto future = bus->Send(message, options);
        future.Cancel(TError("Canceled"));
    }

    Sleep(TDuration::Seconds(1));
    ASSERT_LE(handler->GetCount(), 16);
    handler->ResetCount();

    options.TrackingLevel = EDeliveryTrackingLevel::None;
    for (int i = 0; i < 2; i++) {
        bus->Send(message, options).Cancel(TError("Canceled"));
    }

    Sleep(TDuration::Seconds(1));
    ASSERT_LE(handler->GetCount(), 16);

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

template <class TTraits>
void RunRepliesTest(
    TTraits& traits,
    int requestCount,
    int partCount,
    EDeliveryTrackingLevel level)
{
    auto server = traits.StartServer(New<TReplying42BusHandler>(partCount));
    auto client = traits.CreateClient();
    auto handler = New<TChecking42BusHandler>(requestCount);
    auto bus = client->CreateBus(handler);
    auto message = CreateMessage(partCount);

    std::vector<TFuture<void>> results;
    for (int i = 0; i < requestCount; ++i) {
        auto result = bus->Send(message, {.TrackingLevel = level});
        if (result) {
            results.push_back(result);
        }
    }

    for (const auto& result : results) {
        auto error = NConcurrency::WaitFor(result);
        EXPECT_TRUE(error.IsOK());
    }

    handler->WaitUntilDone();

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, OneReplyNoTracking)
{
    RunRepliesTest(this->Traits_, 1, 1, EDeliveryTrackingLevel::None);
}

TYPED_TEST_P(TBusTest, OneReplyFullTracking)
{
    RunRepliesTest(this->Traits_, 1, 1, EDeliveryTrackingLevel::Full);
}

TYPED_TEST_P(TBusTest, OneReplyErrorOnlyTracking)
{
    RunRepliesTest(this->Traits_, 1, 1, EDeliveryTrackingLevel::ErrorOnly);
}

TYPED_TEST_P(TBusTest, ManyReplies)
{
    RunRepliesTest(this->Traits_, 1000, 100, EDeliveryTrackingLevel::Full);
}

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST_P(TBusTest, LargeMessage)
{
    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    // A single 64 MB part exercises bulk transfer of a large message.
    auto message = CreateMessage(1, 64_MB);
    NConcurrency::WaitFor(
        bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}))
        .ThrowOnError();

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, ManyClients)
{
    constexpr int ClientCount = 16;

    auto handler = New<TCountingBusHandler>();
    auto server = this->Traits_.StartServer(handler);

    std::vector<IBusPtr> buses;
    std::vector<TFuture<void>> sends;
    buses.reserve(ClientCount);
    sends.reserve(ClientCount);
    for (int i = 0; i < ClientCount; ++i) {
        auto client = this->Traits_.CreateClient();
        auto bus = client->CreateBus(New<TEmptyBusHandler>());
        auto message = CreateMessage(1);
        sends.push_back(bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}));
        buses.push_back(std::move(bus));
    }

    for (const auto& send : sends) {
        EXPECT_TRUE(NConcurrency::WaitFor(send).IsOK());
    }

    WaitForPredicate(
        [&] { return ReadActiveConnectionCount(server) == ClientCount; },
        "Server did not see all incoming connections");
    EXPECT_EQ(handler->GetCount(), ClientCount);

    for (const auto& bus : buses) {
        bus->Terminate(TError("test done"));
    }
    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, ServerTerminationNotifiesBus)
{
    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    auto terminated = NewPromise<void>();
    TError observedError;
    bus->SubscribeTerminated(BIND([&] (const TError& error) {
        observedError = error;
        terminated.TrySet();
    }));

    // Force wire-up so the client actually has a live connection to fail.
    auto message = CreateMessage(1);
    NConcurrency::WaitFor(
        bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}))
        .ThrowOnError();

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();

    NConcurrency::WaitFor(terminated.ToFuture())
        .ThrowOnError();
    EXPECT_FALSE(observedError.IsOK());
}

TYPED_TEST_P(TBusTest, ConcurrentSendsOnSameBus)
{
    constexpr int ThreadCount = 8;
    constexpr int SendsPerThread = 64;

    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(4, 4_KB);

    std::vector<std::vector<TFuture<void>>> perThread(ThreadCount);
    std::vector<std::thread> threads;
    threads.reserve(ThreadCount);
    for (int t = 0; t < ThreadCount; ++t) {
        threads.emplace_back([&, t] {
            for (int i = 0; i < SendsPerThread; ++i) {
                perThread[t].push_back(
                    bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}));
            }
        });
    }
    for (auto& thread : threads) {
        thread.join();
    }
    for (const auto& futures : perThread) {
        for (const auto& future : futures) {
            EXPECT_TRUE(NConcurrency::WaitFor(future).IsOK());
        }
    }

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, SendAfterServerStop)
{
    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    // Force wire-up first so the bus has a connected endpoint.
    auto firstMessage = CreateMessage(1);
    NConcurrency::WaitFor(
        bus->Send(firstMessage, {.TrackingLevel = EDeliveryTrackingLevel::Full}))
        .ThrowOnError();

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();

    // The remote endpoint is gone; the second send must fail rather than hang.
    auto message = CreateMessage(1);
    auto result = NConcurrency::WaitFor(
        bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}));
    EXPECT_FALSE(result.IsOK());
}

TYPED_TEST_P(TBusTest, EmptyAndZeroSizedParts)
{
    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    // No parts at all.
    {
        auto message = TSharedRefArray();
        auto result = NConcurrency::WaitFor(
            bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}));
        EXPECT_TRUE(result.IsOK());
    }

    // One part of zero bytes.
    {
        auto message = CreateMessage(1, 0);
        auto result = NConcurrency::WaitFor(
            bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}));
        EXPECT_TRUE(result.IsOK());
    }

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, NullPartPreserved)
{
    auto handler = New<TDirectPlacementBusHandler>();
    auto server = this->Traits_.StartServer(handler);
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    // A plain send (no direct placement): the bus must preserve a null part as a
    // null ref, distinct from a zero-size part.
    auto dataPart = CreateMessage(1, 1024)[0];
    TSharedRefArrayBuilder builder(3);
    builder.Add(dataPart);
    builder.Add(TSharedRef());
    builder.Add(dataPart);
    auto message = builder.Finish();

    NConcurrency::WaitFor(
        bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}))
        .ThrowOnError();

    auto receivedParts = handler->WaitForReceivedParts();
    ASSERT_EQ(message.Size(), receivedParts.size());
    EXPECT_TRUE(receivedParts[0]);
    EXPECT_FALSE(receivedParts[1]);  // the null part is preserved as a null ref
    EXPECT_TRUE(receivedParts[2]);
    // A plain send never produces a direct placement transfer.
    EXPECT_FALSE(handler->SawDirectPlacementTransfer());

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, DirectPlacementPart)
{
    auto handler = New<TDirectPlacementBusHandler>();
    auto server = this->Traits_.StartServer(handler);
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    // A multi-part message whose trailing parts are requested to be delivered
    // via direct placement. DPT-capable transports deliver them through a transfer;
    // transports without DPT support fall back to inline delivery (compat mode).
    // Either way the handler reassembles them and the received layout must match
    // what was sent.
    constexpr int PartCount = 3;
    constexpr int DirectPlacementPartCount = 2;
    auto message = CreateMessage(PartCount, 1_MB);

    auto options = TSendOptions{
        .TrackingLevel = EDeliveryTrackingLevel::Full,
        .DirectPlacementTransferPartCount = DirectPlacementPartCount,
    };
    NConcurrency::WaitFor(bus->Send(message, options))
        .ThrowOnError();

    std::vector<i64> expectedPartSizes;
    for (const auto& part : message) {
        expectedPartSizes.push_back(std::ssize(part));
    }
    // The whole message must be reassembled identically regardless of whether
    // the trailing parts were delivered via a transfer (DPT-capable transports)
    // or inline (compat mode, e.g. TCP).
    EXPECT_EQ(expectedPartSizes, handler->WaitForReceivedPartSizes());

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, DirectPlacementEagerTail)
{
    auto handler = New<TDirectPlacementBusHandler>();
    auto server = this->Traits_.StartServer(handler);
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    // Tiny direct placement parts stay below any transport's direct placement threshold, so
    // UCX delivers the tail eagerly (exercising the copy-into-landing-buffers
    // path rather than ucp_am_recv_data_nbx). The handler reassembles all the
    // same.
    auto message = CreateMessage(3, 64);
    auto options = TSendOptions{
        .TrackingLevel = EDeliveryTrackingLevel::Full,
        .DirectPlacementTransferPartCount = 2,
    };
    NConcurrency::WaitFor(bus->Send(message, options))
        .ThrowOnError();

    std::vector<i64> expectedPartSizes;
    for (const auto& part : message) {
        expectedPartSizes.push_back(std::ssize(part));
    }
    // The whole message must be reassembled identically regardless of whether
    // the trailing parts were delivered via a transfer (DPT-capable transports)
    // or inline (compat mode, e.g. TCP).
    EXPECT_EQ(expectedPartSizes, handler->WaitForReceivedPartSizes());

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, DirectPlacementDropWithoutRun)
{
    // The handler ignores the direct placement transfer (never calls Run). The sender
    // must still observe successful delivery rather than hanging.
    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    auto message = CreateMessage(3, 1_MB);
    auto options = TSendOptions{
        .TrackingLevel = EDeliveryTrackingLevel::Full,
        .DirectPlacementTransferPartCount = 2,
    };

    auto result = NConcurrency::WaitFor(bus->Send(message, options));
    EXPECT_TRUE(result.IsOK());

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, DirectPlacementSendCancel)
{
    // Direct placement combined with send cancelation must not hang or crash. (TCP
    // falls back to a single packet for cancelable sends; UCX has no send
    // cancelation, so the direct placement proceeds.)
    auto handler = New<TCountingBusHandler>();
    auto server = this->Traits_.StartServer(handler);
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(4, 4_MB);

    auto options = TSendOptions{
        .TrackingLevel = EDeliveryTrackingLevel::Full,
        .DirectPlacementTransferPartCount = 2,
        .EnableSendCancelation = true,
    };

    for (int i = 0; i < 16; ++i) {
        bus->Send(message, options).Cancel(TError("Canceled"));
    }

    Sleep(TDuration::Seconds(1));
    ASSERT_LE(handler->GetCount(), 16);

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, DirectPlacementTerminateResolvesRun)
{
    // Tear the connection down while a direct placement receive is in flight: the
    // handler's Run future must resolve (completed or failed) rather than hang.
    auto handler = New<TDirectPlacementRunBusHandler>();
    auto server = this->Traits_.StartServer(handler);
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    // A large direct placement tail keeps the transfer in flight long enough that the
    // teardown below is likely to race it.
    auto message = CreateMessage(3, 8_MB);
    auto options = TSendOptions{
        .TrackingLevel = EDeliveryTrackingLevel::Full,
        .DirectPlacementTransferPartCount = 2,
    };
    auto sendFuture = bus->Send(message, options);

    // Wait until the handler received the head and called Run, then tear down.
    handler->WaitForHandled();
    bus->Terminate(TError("test done"));
    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();

    // Both the receiver-side Run future and the sender-side future must resolve.
    Y_UNUSED(NConcurrency::WaitFor(handler->GetRunFuture()));
    Y_UNUSED(NConcurrency::WaitFor(sendFuture));
}

TYPED_TEST_P(TBusTest, DirectPlacementNullPart)
{
    auto handler = New<TDirectPlacementBusHandler>();
    auto server = this->Traits_.StartServer(handler);
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    // A message whose direct-placement (trailing) region contains a null part.
    // The bus preserves null parts as null (not zero-size) end to end — across a
    // real transfer (UCX) and inline/compat delivery (TCP).
    auto dataPart = CreateMessage(1, 1024)[0];
    TSharedRefArrayBuilder builder(4);
    builder.Add(dataPart);
    builder.Add(dataPart);
    builder.Add(TSharedRef());
    builder.Add(dataPart);
    auto message = builder.Finish();

    auto options = TSendOptions{
        .TrackingLevel = EDeliveryTrackingLevel::Full,
        .DirectPlacementTransferPartCount = 2,
    };
    NConcurrency::WaitFor(bus->Send(message, options))
        .ThrowOnError();

    auto receivedParts = handler->WaitForReceivedParts();
    ASSERT_EQ(message.Size(), receivedParts.size());
    EXPECT_TRUE(receivedParts[0]);
    EXPECT_TRUE(receivedParts[1]);
    EXPECT_FALSE(receivedParts[2]);  // the null part is preserved as a null ref
    EXPECT_TRUE(receivedParts[3]);

    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, AcceptsIncomingConnection)
{
    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    EXPECT_EQ(ReadActiveConnectionCount(server), 0);

    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());

    // Touch the bus to force wire-up; some transports defer it until the first send.
    auto message = CreateMessage(1);
    NConcurrency::WaitFor(
        bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}))
        .ThrowOnError();

    WaitForPredicate(
        [&] { return ReadActiveConnectionCount(server) == 1; },
        "Server did not see the incoming connection");

    bus->Terminate(TError("test done"));
    NConcurrency::WaitFor(server->Stop())
        .ThrowOnError();
}

////////////////////////////////////////////////////////////////////////////////

REGISTER_TYPED_TEST_SUITE_P(
    TBusTest,
    OK,
    Terminate,
    Failed,
    SendCancel,
    OneReplyNoTracking,
    OneReplyFullTracking,
    OneReplyErrorOnlyTracking,
    ManyReplies,
    AcceptsIncomingConnection,
    LargeMessage,
    ManyClients,
    ServerTerminationNotifiesBus,
    ConcurrentSendsOnSameBus,
    SendAfterServerStop,
    EmptyAndZeroSizedParts,
    NullPartPreserved,
    DirectPlacementPart,
    DirectPlacementEagerTail,
    DirectPlacementDropWithoutRun,
    DirectPlacementSendCancel,
    DirectPlacementTerminateResolvesRun,
    DirectPlacementNullPart,
    DnsResolutionFails);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
