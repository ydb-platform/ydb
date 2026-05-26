#ifndef BUS_UT_INL_H_
#error "Direct inclusion of this file is not allowed, include bus_ut.h"
// For the sake of sane code completion.
#include "bus_ut.h"
#endif

#include "handlers.h"
#include "helpers.h"

#include <yt/yt/core/bus/client.h>
#include <yt/yt/core/bus/server.h>

#include <yt/yt/core/concurrency/scheduler_api.h>

#include <util/datetime/base.h>

namespace NYT::NBus::NTests {

////////////////////////////////////////////////////////////////////////////////

TYPED_TEST_P(TBusTest, OK)
{
    auto server = this->Traits_.StartServer(New<TEmptyBusHandler>());
    auto client = this->Traits_.CreateClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = NConcurrency::WaitForFast(
        bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full}));
    EXPECT_TRUE(result.IsOK());
    NConcurrency::WaitForFast(server->Stop())
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
    EXPECT_EQ(NConcurrency::WaitForFast(terminated.ToFuture()).GetCode(), error.GetCode());
    bus->Terminate(TError(TErrorCode(12345), "Ignored"));

    auto result = bus->Send(message, {.TrackingLevel = EDeliveryTrackingLevel::Full});
    EXPECT_TRUE(result.IsSet());
    EXPECT_EQ(NConcurrency::WaitForFast(result).GetCode(), error.GetCode());

    NConcurrency::WaitForFast(server->Stop())
        .ThrowOnError();
}

TYPED_TEST_P(TBusTest, Failed)
{
    auto client = this->Traits_.CreateUnreachableClient();
    auto bus = client->CreateBus(New<TEmptyBusHandler>());
    auto message = CreateMessage(1);
    auto result = NConcurrency::WaitForFast(
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

    NConcurrency::WaitForFast(server->Stop())
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
        auto error = NConcurrency::WaitForFast(result);
        EXPECT_TRUE(error.IsOK());
    }

    handler->WaitUntilDone();

    NConcurrency::WaitForFast(server->Stop())
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

REGISTER_TYPED_TEST_SUITE_P(
    TBusTest,
    OK,
    Terminate,
    Failed,
    SendCancel,
    OneReplyNoTracking,
    OneReplyFullTracking,
    OneReplyErrorOnlyTracking,
    ManyReplies);

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NBus::NTests
