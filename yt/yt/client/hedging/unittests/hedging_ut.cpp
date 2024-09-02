#include "helper.h"

#include <yt/yt/client/hedging/cache.h>
#include <yt/yt/client/hedging/counter.h>
#include <yt/yt/client/hedging/hedging.h>
#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <library/cpp/iterator/zip.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NClient::NHedging::NRpc {
namespace {

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

using TStrictMockClient = StrictMock<NApi::TMockClient>;

////////////////////////////////////////////////////////////////////////////////

const auto SleepQuantum = TDuration::MilliSeconds(100);
const auto CheckPeriod = TDuration::Seconds(1);

class TMockClientsCache
    : public IClientsCache
{
public:
    MOCK_METHOD(NApi::IClientPtr, GetClient, (TStringBuf url), (override));
};

NApi::IClientPtr CreateTestHedgingClient(
    TDuration banPenalty,
    TDuration banDuration,
    std::vector<NApi::IClientPtr> clients,
    std::vector<TDuration> initialPenalties = {TDuration::Zero(), SleepQuantum},
    const IPenaltyProviderPtr& penaltyProvider = CreateDummyPenaltyProvider())
{
    std::vector<TCounterPtr> counters;
    counters.reserve(clients.size());
    for (int i = 0; i != std::ssize(clients); ++i) {
        counters.push_back(New<TCounter>(ToString(i)));
    }
    return NTest::CreateTestHedgingClient(
        clients, counters, initialPenalties, penaltyProvider, banPenalty, banDuration);
}

IPenaltyProviderPtr CreateReplicationLagPenaltyProvider(
    const NYPath::TYPath& path,
    const std::string& cluster,
    TDuration maxTabletLag,
    TDuration lagPenalty,
    NApi::IClientPtr client,
    const bool clearPenaltiesOnErrors = false,
    const TDuration checkPeriod = CheckPeriod)
{
    TReplicationLagPenaltyProviderConfig config;

    config.SetTablePath(path);
    config.AddReplicaClusters(TProtobufString(cluster));
    config.SetMaxTabletsWithLagFraction(0.5);
    config.SetMaxTabletLag(maxTabletLag.Seconds());
    config.SetCheckPeriod(checkPeriod.Seconds());
    config.SetLagPenalty(lagPenalty.MilliSeconds());
    config.SetClearPenaltiesOnErrors(clearPenaltiesOnErrors);

    return CreateReplicationLagPenaltyProvider(config, client);
}

////////////////////////////////////////////////////////////////////////////////

// Using LinkNode method for testing because it's return value is YsonString.
// It makes easier to check from which client result has come from just by comparing corresponding string values.
TEST(THedgingClientTest, GetResultFromClientWithMinEffectivePenalty)
{
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString clientResult1("FirstClientData"_sb);

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient1, ListNode(path, _))
        .WillOnce(Return(MakeFuture(clientResult1)));
    EXPECT_CALL(*mockClient2, ListNode(path, _)).Times(0);

    auto hedgingClient = CreateTestHedgingClient(
        SleepQuantum * 2,
        SleepQuantum,
        {mockClient1, mockClient2});

    auto queryResult = NConcurrency::WaitFor(hedgingClient->ListNode(path));
    // Check that query result is from first client, because it's effective initial penalty is minimal.
    ASSERT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), clientResult1.AsStringBuf());
}

TEST(THedgingClientTest, GetclientResult2WhenFirstClientHasFailed)
{
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString clientResult1("FirstClientData"_sb);
    NYson::TYsonString clientResult2("SecondClientData"_sb);

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient1, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));
    EXPECT_CALL(*mockClient2, ListNode(path, _))
        .WillOnce(Return(MakeFuture(clientResult2)));

    auto client = CreateTestHedgingClient(
        SleepQuantum * 2,
        SleepQuantum * 2,
        {mockClient1, mockClient2});

    auto queryResult = NConcurrency::WaitFor(client->ListNode(path));
    // Check that query result is from second client, because first client returned failure and got banned.
    ASSERT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), clientResult2.AsStringBuf());
}

TEST(THedgingClientTest, GetclientResult1AfterBanTimeHasElapsed)
{
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString clientResult1("FirstClientData"_sb);
    NYson::TYsonString clientResult2("SecondClientData"_sb);

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient1, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))))
        .WillOnce(Return(MakeFuture(clientResult1)));
    EXPECT_CALL(*mockClient2, ListNode(path, _))
        .WillOnce(Return(MakeFuture(clientResult2)));

    auto banDuration = SleepQuantum * 2;
    auto hedgingClient = CreateTestHedgingClient(
        banDuration,
        banDuration,
        {mockClient1, mockClient2});

    auto queryResult1 = NConcurrency::WaitFor(hedgingClient->ListNode(path));
    // Check that first query result is from second client, because first client returned failure and got banned.
    ASSERT_TRUE(queryResult1.IsOK());
    EXPECT_EQ(queryResult1.Value().AsStringBuf(), clientResult2.AsStringBuf());

    NConcurrency::TDelayedExecutor::WaitForDuration(banDuration);

    auto queryResult2 = NConcurrency::WaitFor(hedgingClient->ListNode(path));
    // Check that second query result is from first client, because ban time has elapsed and it's effective initial penalty is minimal again.
    ASSERT_TRUE(queryResult2.IsOK());
    EXPECT_EQ(queryResult2.Value().AsStringBuf(), clientResult1.AsStringBuf());
}

TEST(THedgingClientTest, GetclientResult2WhenFirstClientIsBanned)
{
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString clientResult1("FirstClientData"_sb);
    NYson::TYsonString clientResult2("SecondClientData"_sb);

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient1, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));
    EXPECT_CALL(*mockClient2, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(clientResult2)));

    auto hedgingClient = CreateTestHedgingClient(
        SleepQuantum * 2,
        TDuration::Seconds(2),
        {mockClient1, mockClient2});

    auto queryResult1 = NConcurrency::WaitFor(hedgingClient->ListNode(path));
    // Check that first query result is from second client, because first client returned failure and got banned.
    ASSERT_TRUE(queryResult1.IsOK());
    EXPECT_EQ(queryResult1.Value().AsStringBuf(), clientResult2.AsStringBuf());

    auto queryResult2 = NConcurrency::WaitFor(hedgingClient->ListNode(path));
    // Check that second query result is from second client, because first client is still banned.
    ASSERT_TRUE(queryResult2.IsOK());
    EXPECT_EQ(queryResult2.Value().AsStringBuf(), clientResult2.AsStringBuf());
}

TEST(THedgingClientTest, GetclientResult2WhenFirstClientIsSleeping)
{
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString clientResult1("FirstClientData"_sb);
    NYson::TYsonString clientResult2("SecondClientData"_sb);

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient1, ListNode(path, _))
        .WillOnce(Return(NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::Seconds(2)).Apply(BIND([=] { return clientResult1; }))));
    EXPECT_CALL(*mockClient2, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(clientResult2)));

    auto hedgingClient = CreateTestHedgingClient(
        SleepQuantum * 2,
        SleepQuantum,
        {mockClient1, mockClient2});

    auto queryResult = NConcurrency::WaitFor(hedgingClient->ListNode(path));
    // Check that query result is from second client, because first client is sleeping.
    ASSERT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), clientResult2.AsStringBuf());
}

TEST(THedgingClientTest, FirstClientIsBannedBecauseResponseWasCancelled)
{
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString clientResult1("FirstClientData"_sb);
    NYson::TYsonString clientResult2("SecondClientData"_sb);

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient1, ListNode(path, _))
        .WillOnce(Return(NConcurrency::TDelayedExecutor::MakeDelayed(SleepQuantum * 2).Apply(BIND([=] { return clientResult1; }))))
        .WillRepeatedly(Return(MakeFuture(clientResult1)));
    EXPECT_CALL(*mockClient2, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(clientResult2)));

    auto client = CreateTestHedgingClient(
        SleepQuantum * 2,
        TDuration::Seconds(2),
        {mockClient1, mockClient2});

    auto queryResult1 = NConcurrency::WaitFor(client->ListNode(path));
    // Check that query result is from second client, because first client is sleeping.
    ASSERT_TRUE(queryResult1.IsOK());
    EXPECT_EQ(queryResult1.Value().AsStringBuf(), clientResult2.AsStringBuf());

    // Wait for finish of all requests
    Sleep(SleepQuantum);

    auto queryResult2 = NConcurrency::WaitFor(client->ListNode(path));
    // Check that second query result is from second client, because first client was cancelled and got banned.
    ASSERT_TRUE(queryResult2.IsOK());
    EXPECT_EQ(queryResult2.Value().AsStringBuf(), clientResult2.AsStringBuf());
}

TEST(THedgingClientTest, AmnestyBanPenaltyIfClientSucceeded)
{
    NYPath::TYPath path = "/test/1234";

    NYson::TYsonString clientResult1("FirstClientData"_sb);
    NYson::TYsonString clientResult2("SecondClientData"_sb);
    NYson::TYsonString thirdClientResult("ThirdClientData"_sb);

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();
    auto mockClient3 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient1, ListNode(path, _))
        .WillOnce(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))))
        .WillRepeatedly(Return(MakeFuture(clientResult1)));
    EXPECT_CALL(*mockClient2, ListNode(path, _))
        .WillOnce(Return(MakeFuture(clientResult2)))
        .WillOnce(Return(NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::Seconds(100)).Apply(BIND([=] { return clientResult2; }))))
        .WillRepeatedly(Return(MakeFuture(clientResult2)));
    EXPECT_CALL(*mockClient3, ListNode(path, _))
        .WillRepeatedly(Return(NConcurrency::TDelayedExecutor::MakeDelayed(TDuration::Seconds(100)).Apply(BIND([=] { return thirdClientResult; }))));

    auto client = CreateTestHedgingClient(
        SleepQuantum * 2,
        TDuration::Seconds(30),
        {mockClient1, mockClient2, mockClient3},
        {TDuration::Zero(), SleepQuantum, SleepQuantum * 2});

    auto queryResult1 = NConcurrency::WaitFor(client->ListNode(path));
    // Check that query result is from second client, because first client finished with an error.
    ASSERT_TRUE(queryResult1.IsOK());
    EXPECT_EQ(queryResult1.Value().AsStringBuf(), clientResult2.AsStringBuf());

    // Wait for finish of all requests
    Sleep(SleepQuantum * 2);

    auto queryResult2 = NConcurrency::WaitFor(client->ListNode(path));
    // Check that second query result is from first client, because other clients were sleeping.
    ASSERT_TRUE(queryResult2.IsOK());
    EXPECT_EQ(queryResult2.Value().AsStringBuf(), clientResult1.AsStringBuf());

    // Wait for finish of all requests
    Sleep(SleepQuantum * 2);

    auto queryResult3 = NConcurrency::WaitFor(client->ListNode(path));
    // Check that third query result is from first client again, because it's penalty was amnestied.
    ASSERT_TRUE(queryResult3.IsOK());
    EXPECT_EQ(queryResult3.Value().AsStringBuf(), clientResult1.AsStringBuf());
}

TEST(THedgingClientTest, MultiThread)
{
    NYPath::TYPath path = "/test/1234";

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();
    NYson::TYsonString clientResult1("FirstClientData"_sb);
    NYson::TYsonString clientResult2("SecondClientData"_sb);

    EXPECT_CALL(*mockClient1, ListNode(path, _)).WillRepeatedly([=] (const NYPath::TYPath&, const NApi::TListNodeOptions& options) {
        if (options.Timeout) {
            return NConcurrency::TDelayedExecutor::MakeDelayed(*options.Timeout).Apply(BIND([=] {
                return clientResult1;
            }));
        }
        return MakeFuture(clientResult1);
    });
    EXPECT_CALL(*mockClient2, ListNode(path, _)).WillRepeatedly(Return(MakeFuture(clientResult2)));

    auto hedgingClient = CreateTestHedgingClient(
        TDuration::MilliSeconds(1),
        SleepQuantum,
        {mockClient1, mockClient2},
        {SleepQuantum, SleepQuantum * 3});

    auto threadPool = NConcurrency::CreateThreadPool(10, "test");
    std::vector<TFuture<void>> futures;
    for (int i = 0; i < 100; ++i) {
        futures.push_back(BIND([=] {
            for (int j = 0; j < 100; ++j) {
                NApi::TListNodeOptions options;
                // on each 5th request for 1st and 2nd thread, the first client will timeout
                if (i < 2 && (0 == j % 5)) {
                    options.Timeout = TDuration::Seconds(2);
                }
                auto v = NConcurrency::WaitFor(hedgingClient->ListNode(path, options)).ValueOrThrow();
                if (options.Timeout) {
                    EXPECT_EQ(clientResult2.AsStringBuf(), v.AsStringBuf());
                } else {
                    EXPECT_EQ(clientResult1.AsStringBuf(), v.AsStringBuf());
                }
            }
        }).AsyncVia(threadPool->GetInvoker()).Run());
    }

    for (const auto& future : futures) {
        EXPECT_NO_THROW(future.Get().ThrowOnError());
    }
}

TEST(THedgingClientTest, ResponseFromSecondClientWhenFirstHasReplicationLag)
{
    NYPath::TYPath path = "/test/1234";

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();
    NYson::TYsonString clientResult1("FirstClientData"_sb);
    NYson::TYsonString clientResult2("SecondClientData"_sb);

    EXPECT_CALL(*mockClient1, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(clientResult1)));
    EXPECT_CALL(*mockClient2, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(clientResult2)));

    auto hedgingClient = CreateTestHedgingClient(
        SleepQuantum * 2,
        SleepQuantum,
        {mockClient1, mockClient2});

    auto queryResult = NConcurrency::WaitFor(hedgingClient->ListNode(path));
    // Check that query result is from first client, because it's effective initial penalty is minimal.
    ASSERT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), clientResult1.AsStringBuf());

    auto maxTabletLag = TDuration::Seconds(10);
    auto lagPenalty = 2 * SleepQuantum;

    NYson::TYsonString replicasResult("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"cluster-0\"}}"_sb);
    NYson::TYsonString tabletCountResult("1"_sb);

    auto mockClient3 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient3, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*mockClient3, GetNode(path + "/@tablet_count", _))
        .WillRepeatedly(Return(MakeFuture(tabletCountResult)));

    std::vector<NApi::TTabletInfo> tabletInfos(1);
    tabletInfos[0].TableReplicaInfos = std::make_optional(std::vector<NApi::TTabletInfo::TTableReplicaInfo>());
    auto& replicaTabletsInfo = tabletInfos[0].TableReplicaInfos->emplace_back();
    replicaTabletsInfo.ReplicaId = NTabletClient::TTableReplicaId::FromString("575f-131-40502c5-201b420f");
    replicaTabletsInfo.LastReplicationTimestamp = NTransactionClient::TimestampFromUnixTime(
        TInstant::Now().Seconds() - 2 * maxTabletLag.Seconds());

    EXPECT_CALL(*mockClient3, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture(tabletInfos)));

    auto penaltyProvier = CreateReplicationLagPenaltyProvider(
        path,
        "cluster-0",
        maxTabletLag,
        lagPenalty,
        mockClient3);
    Sleep(2 * CheckPeriod);

    auto hedgingClientWithPenaltyProvider = CreateTestHedgingClient(
        TDuration::MilliSeconds(1),
        SleepQuantum,
        {mockClient1, mockClient2},
        {TDuration::Zero(), SleepQuantum},
        penaltyProvier);

    auto queryResultWithReplicationLagPolicy = NConcurrency::WaitFor(hedgingClientWithPenaltyProvider->ListNode(path));

    // Check that query result is from second client, because first client received penalty updater because of replication lag.
    ASSERT_TRUE(queryResultWithReplicationLagPolicy.IsOK());
    EXPECT_EQ(queryResultWithReplicationLagPolicy.Value().AsStringBuf(), clientResult2.AsStringBuf());
}

TEST(THedgingClientTest, CreatingHedgingClientWithPreinitializedClients)
{
    const TString clusterName = "test_cluster";
    NYPath::TYPath path = "/test/1234";
    NYson::TYsonString clientResult("ClientData"_sb);

    auto mockClient = New<TStrictMockClient>();
    EXPECT_CALL(*mockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(clientResult)));

    auto mockClientsCache = New<StrictMock<TMockClientsCache>>();
    EXPECT_CALL(*mockClientsCache, GetClient(clusterName)).WillOnce(Return(mockClient));

    THedgingClientConfig hedgingClientConfig;
    hedgingClientConfig.SetBanDuration(100);
    hedgingClientConfig.SetBanPenalty(200);
    auto clientOptions = hedgingClientConfig.AddClients();
    clientOptions->SetInitialPenalty(0);
    clientOptions->MutableClientConfig()->SetClusterName(clusterName);

    auto hedgingClient = CreateHedgingClient(hedgingClientConfig, mockClientsCache);

    auto queryResult = NConcurrency::WaitFor(hedgingClient->ListNode(path));
    // Check that query result is from preinitialized client.
    ASSERT_TRUE(queryResult.IsOK());
    EXPECT_EQ(queryResult.Value().AsStringBuf(), clientResult.AsStringBuf());
}

TEST(THedgingClientTest, ResponseFromFirstClientWhenReplicationLagUpdaterFails)
{
    NYPath::TYPath path = "/test/1234";

    auto mockClient1 = New<TStrictMockClient>();
    auto mockClient2 = New<TStrictMockClient>();
    NYson::TYsonString clientResult1("FirstClientData"_sb);
    NYson::TYsonString clientResult2("SecondClientData"_sb);

    EXPECT_CALL(*mockClient1, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(clientResult1)));
    EXPECT_CALL(*mockClient2, ListNode(path, _))
        .WillRepeatedly(Return(MakeFuture(clientResult2)));

    auto maxTabletLag = TDuration::Seconds(10);
    auto lagPenalty = 2 * SleepQuantum;

    NYson::TYsonString replicasResult("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"cluster-0\"}}"_sb);
    NYson::TYsonString tabletCountResult("1"_sb);

    auto mockClient3 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient3, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*mockClient3, GetNode(path + "/@tablet_count", _))
        .WillOnce(Return(MakeFuture(tabletCountResult)))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    std::vector<NApi::TTabletInfo> tabletInfos(1);
    tabletInfos[0].TableReplicaInfos = std::make_optional(std::vector<NApi::TTabletInfo::TTableReplicaInfo>());
    auto& replicaTabletsInfo = tabletInfos[0].TableReplicaInfos->emplace_back();
    replicaTabletsInfo.ReplicaId = NTabletClient::TTableReplicaId::FromString("575f-131-40502c5-201b420f");
    replicaTabletsInfo.LastReplicationTimestamp = NTransactionClient::TimestampFromUnixTime(TInstant::Now().Seconds() - 2 * maxTabletLag.Seconds());

    EXPECT_CALL(*mockClient3, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture(tabletInfos)));

    auto penaltyProvider = CreateReplicationLagPenaltyProvider(
        path,
        "cluster-0",
        maxTabletLag,
        lagPenalty,
        mockClient3,
        true,
        2 * CheckPeriod);
    Sleep(CheckPeriod);

    auto hedgingClientWithPenaltyProvider = CreateTestHedgingClient(
        TDuration::MilliSeconds(1),
        SleepQuantum,
        {mockClient1, mockClient2},
        {TDuration::Zero(), SleepQuantum},
        penaltyProvider);

    auto queryResultWithReplicationLagPolicy = NConcurrency::WaitFor(hedgingClientWithPenaltyProvider->ListNode(path));
    // Check that query result is from second client, because first client received penalty because of replication lag.
    ASSERT_TRUE(queryResultWithReplicationLagPolicy.IsOK());
    EXPECT_EQ(queryResultWithReplicationLagPolicy.Value().AsStringBuf(), clientResult2.AsStringBuf());

    Sleep(2 * CheckPeriod);

    auto queryResultWithCleanedPenalty = NConcurrency::WaitFor(hedgingClientWithPenaltyProvider->ListNode(path));
    // Check that query result is from first client, because replication lag was cleaned.
    ASSERT_TRUE(queryResultWithCleanedPenalty.IsOK());
    EXPECT_EQ(queryResultWithCleanedPenalty.Value().AsStringBuf(), clientResult1.AsStringBuf());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NClient::NHedging::NRpc
