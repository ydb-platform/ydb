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
    LargeMessage,
    ServerTerminationNotifiesBus,
    ConcurrentSendsOnSameBus,
    SendAfterServerStop,
    EmptyAndZeroSizedParts,
    DnsResolutionFails);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
