#include "helper.h"

#include <yt/yt/client/hedging/counter.h>
#include <yt/yt/client/hedging/hedging.h>
#include <yt/yt/client/hedging/hedging_executor.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/library/profiling/sensor.h>
#include <yt/yt/library/profiling/testing.h>
#include <yt/yt/library/profiling/solomon/registry.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NClient::NHedging::NRpc {

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

using namespace NYT::NProfiling;

using TStrictMockClient = StrictMock<NApi::TMockClient>;

////////////////////////////////////////////////////////////////////////////////

namespace {

const auto SleepQuantum = TDuration::MilliSeconds(100);

#define EXPECT_DURATION_NEAR(a, b) EXPECT_NEAR(a.MilliSeconds(), b.MilliSeconds(), 1)


NApi::IClientPtr CreateTestHedgingClient(
    std::vector<NApi::IClientPtr> clients,
    std::vector<TCounterPtr> counters,
    TDuration banDuration = SleepQuantum * 5)
{
    return NTest::CreateTestHedgingClient(
        clients,
        counters,
        {TDuration::Zero(), SleepQuantum},
        CreateDummyPenaltyProvider(),
        SleepQuantum * 2,
        banDuration);
}

} // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(THedgingClientCountersTest, CountersAfterSuccessFromFirstClient)
{
    NYPath::TYPath path = "/test/1234";
    NApi::TListNodeOptions options;
    options.Attributes = {"some_attribute"};

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(firstClientResult)));
    EXPECT_CALL(*secondMockClient, ListNode(path, _)).Times(0);

    auto solomon = New<TSolomonRegistry>();
    TRegistry registry(solomon, "/d");

    auto firstClientCounter = New<TCounter>(registry.WithTag("c", "first"));
    auto secondClientCounter = New<TCounter>(registry.WithTag("c", "first"));

    auto client = CreateTestHedgingClient(
        {firstMockClient, secondMockClient},
        {firstClientCounter, secondClientCounter});
    auto queryResult = NConcurrency::WaitFor(client->ListNode(path, options));

    // Wait for cancelled request finish.
    Sleep(SleepQuantum);

    // Success result from first client with effective initial penalty equals to 0 ms
    EXPECT_EQ(1, TTesting::ReadCounter(firstClientCounter->SuccessRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(firstClientCounter->ErrorRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(firstClientCounter->CancelRequestCount));
    EXPECT_DURATION_NEAR(TDuration::Zero(), TTesting::ReadTimeGauge(firstClientCounter->EffectivePenalty));

    // Cancel result from second client with effective initial penalty increment to ban duration
    EXPECT_EQ(0, TTesting::ReadCounter(secondClientCounter->SuccessRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(secondClientCounter->ErrorRequestCount));
    EXPECT_EQ(1, TTesting::ReadCounter(secondClientCounter->CancelRequestCount));
    EXPECT_DURATION_NEAR(SleepQuantum, TTesting::ReadTimeGauge(secondClientCounter->EffectivePenalty));
}

TEST(THedgingClientCountersTest, CountersAfterFirstClientHasFailed)
{
    NYPath::TYPath path = "/test/1234";
    NApi::TListNodeOptions options;
    options.Attributes = {"some_attribute"};

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(secondClientResult)));

    auto solomon = New<TSolomonRegistry>();
    TRegistry registry(solomon, "/d");

    auto firstClientCounter = New<TCounter>(registry.WithTag("c", "first"));
    auto secondClientCounter = New<TCounter>(registry.WithTag("c", "second"));

    auto client = CreateTestHedgingClient(
        {firstMockClient, secondMockClient},
        {firstClientCounter, secondClientCounter},
        TDuration::Seconds(5));
    auto queryResult = NConcurrency::WaitFor(client->ListNode(path, options));

    // Error result from first client with effective initial penalty equals to 0 ms
    EXPECT_EQ(0, TTesting::ReadCounter(firstClientCounter->SuccessRequestCount));
    EXPECT_EQ(1, TTesting::ReadCounter(firstClientCounter->ErrorRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(firstClientCounter->CancelRequestCount));
    EXPECT_DURATION_NEAR(TDuration::Zero(), TTesting::ReadTimeGauge(firstClientCounter->EffectivePenalty));

    // Success result from second client with effective initial penalty equals to 8 ms
    EXPECT_EQ(1, TTesting::ReadCounter(secondClientCounter->SuccessRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(secondClientCounter->ErrorRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(secondClientCounter->CancelRequestCount));
    EXPECT_DURATION_NEAR(SleepQuantum, TTesting::ReadTimeGauge(secondClientCounter->EffectivePenalty));
}

TEST(THedgingClientCountersTest, CountersWhenFirstClientIsBanned)
{
    NYPath::TYPath path = "/test/1234";
    NApi::TListNodeOptions options;
    options.Attributes = {"some_attribute"};

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(secondClientResult)))
        .WillOnce(Return(MakeFuture(secondClientResult)));

    auto solomon = New<TSolomonRegistry>();
    solomon->SetWindowSize(12);
    TRegistry registry(solomon, "/d");

    auto firstClientCounter = New<TCounter>(registry.WithTag("c", "first"));
    auto secondClientCounter = New<TCounter>(registry.WithTag("c", "second"));

    auto client = CreateTestHedgingClient(
        {firstMockClient, secondMockClient},
        {firstClientCounter, secondClientCounter});
    auto firstQueryResult = NConcurrency::WaitFor(client->ListNode(path, options));

    auto secondQueryResult = NConcurrency::WaitFor(client->ListNode(path, options));

    // Wait for cancelled request finish.
    Sleep(SleepQuantum);

    // Cancel result from banned client with effective initial penalty equals to BanDuration ms
    EXPECT_EQ(0, TTesting::ReadCounter(firstClientCounter->SuccessRequestCount));
    EXPECT_EQ(1, TTesting::ReadCounter(firstClientCounter->ErrorRequestCount));
    EXPECT_EQ(1, TTesting::ReadCounter(firstClientCounter->CancelRequestCount));
    EXPECT_DURATION_NEAR(SleepQuantum, TTesting::ReadTimeGauge(firstClientCounter->EffectivePenalty));

    // Success result from second client with effective initial penalty equals to 0 ms
    EXPECT_EQ(2, TTesting::ReadCounter(secondClientCounter->SuccessRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(secondClientCounter->ErrorRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(secondClientCounter->CancelRequestCount));
    EXPECT_DURATION_NEAR(TDuration::Zero(), TTesting::ReadTimeGauge(secondClientCounter->EffectivePenalty));
}

TEST(THedgingClientCountersTest, CountersAfterFirstClientBanHasElapsed)
{
    NYPath::TYPath path = "/test/1234";
    NApi::TListNodeOptions options;
    options.Attributes = {"some_attribute"};

    NYson::TYsonString firstClientResult(TStringBuf("FirstClientData"));
    NYson::TYsonString secondClientResult(TStringBuf("SecondClientData"));

    auto firstMockClient = New<TStrictMockClient>();
    auto secondMockClient = New<TStrictMockClient>();

    EXPECT_CALL(*firstMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))))
        .WillOnce(Return(MakeFuture(firstClientResult)));
    EXPECT_CALL(*secondMockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(secondClientResult)));

    auto solomon = New<TSolomonRegistry>();
    solomon->SetWindowSize(12);
    TRegistry registry(solomon, "/d");

    auto firstClientCounter = New<TCounter>(registry.WithTag("c", "first"));
    auto secondClientCounter = New<TCounter>(registry.WithTag("c", "second"));

    auto banDuration = SleepQuantum * 2;

    auto client = CreateTestHedgingClient(
        {firstMockClient, secondMockClient},
        {firstClientCounter, secondClientCounter},
        banDuration);
    auto firstQueryResult = NConcurrency::WaitFor(client->ListNode(path, options));

    Sleep(banDuration);
    auto secondQueryResult = NConcurrency::WaitFor(client->ListNode(path, options));

    // Wait for cancelled request finish.
    Sleep(SleepQuantum);

    // Success result from first client, after ban time has elapsed, with effective initial penalty equals to 0 ms
    EXPECT_EQ(1, TTesting::ReadCounter(firstClientCounter->SuccessRequestCount));
    EXPECT_EQ(1, TTesting::ReadCounter(firstClientCounter->ErrorRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(firstClientCounter->CancelRequestCount));
    EXPECT_DURATION_NEAR(TDuration::Zero(), TTesting::ReadTimeGauge(firstClientCounter->EffectivePenalty));

    // Cancel result from second client with effective initial penalty equals to 8 ms
    EXPECT_EQ(1, TTesting::ReadCounter(secondClientCounter->SuccessRequestCount));
    EXPECT_EQ(0, TTesting::ReadCounter(secondClientCounter->ErrorRequestCount));
    EXPECT_EQ(1, TTesting::ReadCounter(secondClientCounter->CancelRequestCount));
    EXPECT_DURATION_NEAR(SleepQuantum, TTesting::ReadTimeGauge(secondClientCounter->EffectivePenalty));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
