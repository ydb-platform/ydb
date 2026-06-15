#include "helper.h"

#include <yt/yt/client/hedging/cache.h>
#include <yt/yt/client/hedging/counter.h>
#include <yt/yt/client/hedging/hedging.h>
#include <yt/yt/client/hedging/hedging_executor.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/concurrency/delayed_executor.h>
#include <yt/yt/core/concurrency/thread_pool.h>
#include <yt/yt/core/concurrency/scheduler.h>

#include <yt/yt/core/actions/cancelable_context.h>

#include <library/cpp/iterator/zip.h>

#include <library/cpp/testing/gtest/gtest.h>

#include <library/cpp/testing/common/env.h>

#include <util/generic/hash.h>
#include <util/random/random.h>
#include <util/string/printf.h>

#include <atomic>
#include <memory>

namespace NYT::NClient::NHedging::NRpc {
namespace {

using namespace NConcurrency;

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
    TDuration maxReplicaLag,
    TDuration lagPenalty,
    NApi::IClientPtr client,
    const bool clearPenaltiesOnErrors = false,
    const TDuration checkPeriod = CheckPeriod)
{
    auto config = New<TReplicationLagPenaltyProviderOptions>();

    config->TablePath = path;
    config->ReplicaClusters.push_back(cluster);
    config->MaxReplicaLag = maxReplicaLag;
    config->CheckPeriod = checkPeriod;
    config->LagPenalty = lagPenalty;
    config->ClearPenaltiesOnErrors = clearPenaltiesOnErrors;

    return CreateReplicationLagPenaltyProvider(config, client);
}

// A synchronous stand-in for the replication-lag penalty provider: reports a fixed external penalty
// per cluster (it would report LagPenalty for a lagging cluster).
class TFixedPenaltyProvider
    : public IPenaltyProvider
{
public:
    explicit TFixedPenaltyProvider(THashMap<std::string, TDuration> penaltyByCluster)
        : PenaltyByCluster_(std::move(penaltyByCluster))
    { }

    TDuration Get(const std::string& cluster) override
    {
        auto it = PenaltyByCluster_.find(cluster);
        return it != PenaltyByCluster_.end() ? it->second : TDuration::Zero();
    }

private:
    const THashMap<std::string, TDuration> PenaltyByCluster_;
};

// Builds a hedging client straight from THedgingExecutor. ClientPriority is set explicitly so the
// background DC resolver is a no-op; hedgingRequestDelays enables delays mode (node order picks the
// primary), remoteDataCenterPenalty is the random tie-break step between remotes.
NApi::IClientPtr CreateRemoteAwareHedgingClient(
    std::vector<NApi::IClientPtr> clients,
    std::vector<NApi::EClientPriority> priorities,
    std::vector<TDuration> hedgingRequestDelays,
    TDuration remoteDataCenterPenalty,
    const std::string& localDataCenter = "local-dc",
    TDuration banPenalty = SleepQuantum * 2,
    TDuration banDuration = SleepQuantum,
    const IPenaltyProviderPtr& penaltyProvider = CreateDummyPenaltyProvider(),
    double hedgingRatioLimit = 1.0)
{
    YT_VERIFY(clients.size() == priorities.size());

    std::vector<THedgingExecutor::TNode> nodes;
    nodes.reserve(clients.size());
    for (int nodeIndex = 0; nodeIndex < std::ssize(clients); ++nodeIndex) {
        nodes.push_back({
            .Client = clients[nodeIndex],
            .Counter = New<TCounter>(ToString(nodeIndex)),
            .ClusterName = ToString(nodeIndex),
            .InitialPenalty = TDuration::Zero(),
            .ClientPriority = priorities[nodeIndex],
        });
    }

    // Hedging-ratio counter: the shift period is far longer than the test run, so no bucket shift
    // happens and the ratio is a deterministic cumulative count; 5 buckets so a stray shift would
    // degrade gracefully instead of zeroing the whole window.
    return CreateHedgingClient(New<THedgingExecutor>(
        nodes,
        banPenalty,
        banDuration,
        penaltyProvider,
        /*ratioCounterBucketCount*/ 5,
        /*ratioCounterShiftPeriod*/ TDuration::Hours(1),
        hedgingRatioLimit,
        hedgingRequestDelays,
        remoteDataCenterPenalty,
        localDataCenter));
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
        EXPECT_NO_THROW(WaitForFast(future).ThrowOnError());
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

    auto maxReplicaLag = TDuration::Seconds(10);
    auto lagPenalty = 2 * SleepQuantum;

    NYson::TYsonString replicasResult(TStringBuf(
        "{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"cluster-0\"; \"content_type\" = \"data\"; \"replication_lag_time\" = 20000 }}"));

    auto mockClient3 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient3, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    auto penaltyProvier = CreateReplicationLagPenaltyProvider(
        path,
        "cluster-0",
        maxReplicaLag,
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
    const std::string clusterName = "test_cluster";
    NYPath::TYPath path = "/test/1234";
    NYson::TYsonString clientResult("ClientData"_sb);

    auto mockClient = New<TStrictMockClient>();
    EXPECT_CALL(*mockClient, ListNode(path, _))
        .WillOnce(Return(MakeFuture(clientResult)));

    auto mockClientsCache = New<StrictMock<TMockClientsCache>>();
    EXPECT_CALL(*mockClientsCache, GetClient(TStringBuf(clusterName))).WillOnce(Return(mockClient));

    auto hedgingClientConfig = New<THedgingClientOptions>();
    hedgingClientConfig->BanDuration = TDuration::MilliSeconds(100);
    hedgingClientConfig->BanPenalty = TDuration::MilliSeconds(200);

    auto connectionConfig = New<TConnectionWithPenaltyConfig>();
    connectionConfig->InitialPenalty = TDuration();
    connectionConfig->ClusterUrl = clusterName;

    hedgingClientConfig->Connections.push_back(connectionConfig);

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

    auto maxReplicaLag = TDuration::Seconds(10);
    auto lagPenalty = 2 * SleepQuantum;

    NYson::TYsonString replicasResult(TStringBuf(
        "{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"cluster-0\"; \"content_type\" = \"data\"; \"replication_lag_time\" = 20000 }}"));

    auto mockClient3 = New<TStrictMockClient>();

    EXPECT_CALL(*mockClient3, GetNode(path + "/@replicas", _))
        .WillOnce(Return(MakeFuture(replicasResult)))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    auto penaltyProvider = CreateReplicationLagPenaltyProvider(
        path,
        "cluster-0",
        maxReplicaLag,
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

// Canonizes the served-by distribution over 100 requests, "local fresh" vs "local lagging" crossed
// with the second remote's error rate (the first always fails 1/10). A fresh local serves everything;
// a lagging local gets LagPenalty (42), above the remote slots (+10/+20), so it sorts last and - since
// only the top two of three clusters are dispatched (one hedge delay) - is tried only once a remote is
// heavily banned. Hedging is capped at 20% of requests (HedgingRatioLimit), so the total-requests
// column shows backend load held down once the cap is hit.
//
// Determinism: per-request error flags + a fixed lag penalty + a seeded tie-break, so background hedge
// tasks never affect the outcome.
//
// Regenerate the golden with:
//   ya make -tA --test-param GTEST_UPDATE_GOLDEN=1 yt/yt/client/hedging/unittests
TEST(THedgingClientTest, HedgingSimulation)
{
    NYPath::TYPath path = "/test/1234";
    NYson::TYsonString remote1Result("remote-1"_sb);
    NYson::TYsonString remote2Result("remote-2"_sb);
    NYson::TYsonString localResult("local"_sb);
    constexpr int iterations = 100;
    // Production eagle LagPenalty; it exceeds RemoteDataCenterPenalty so a lagging local sorts last.
    auto lagPenalty = TDuration::MilliSeconds(42);
    // CreateRemoteAwareHedgingClient names clusters by position; the local one is passed 3rd -> "2".
    std::string localClusterName = "2";

    // The primary is the only request dispatched synchronously inside ListNode(); the first client hit
    // (before the call returns) wins this compare-exchange and is recorded as the primary. Hedges fire
    // later, off-thread, and lose the exchange.
    auto primaryCluster = std::make_shared<std::atomic<int>>(-1);
    // Counts every backend request that actually runs (primary + each hedge that fires), to show how
    // the hedging ratio limit caps total load.
    auto totalRequests = std::make_shared<std::atomic<int>>(0);

    // A client fails iff its error flag is set; flags are set per request, so outcomes don't depend on
    // background hedge-task timing.
    auto makeClient = [&] (std::shared_ptr<std::atomic<bool>> errorFlag, NYson::TYsonString result, int clusterId) {
        auto client = New<TStrictMockClient>();
        EXPECT_CALL(*client, ListNode(path, _))
            .WillRepeatedly([=] (const NYPath::TYPath&, const NApi::TListNodeOptions&) -> TFuture<NYson::TYsonString> {
                totalRequests->fetch_add(1);
                int expected = -1;
                primaryCluster->compare_exchange_strong(expected, clusterId);
                if (errorFlag->load()) {
                    return MakeFuture<NYson::TYsonString>(TError("Failure"));
                }
                return MakeFuture(result);
            });
        return client;
    };

    // Pseudo-random but deterministic error placement: a well-mixed hash of the request index gives a
    // stable bucket in [0, 10), so "bucket < rate" yields the requested failure rate at scattered (not
    // block-contiguous) positions. Independent of the executor's tie-break RNG.
    auto errorAt = [] (int index, ui64 salt, int per10) {
        ui64 x = static_cast<ui64>(index) * 0x9E3779B97F4A7C15ULL + salt * 0xBF58476D1CE4E5B9ULL;
        x = (x ^ (x >> 30)) * 0xBF58476D1CE4E5B9ULL;
        x = (x ^ (x >> 27)) * 0x94D049BB133111EBULL;
        x ^= x >> 31;
        return static_cast<int>(x % 10) < per10;
    };

    TString table = "localLagging remote1Err remote2Err -> local remote1 remote2 failed || primLocal primR1 primR2 || total\n";
    // Local is either fresh or lagging; remote1 always fails 1/10, remote2 sweeps 0/10 .. 10/10.
    for (bool localLagging : {false, true}) {
        for (int remote2ErrorsPer10 = 0; remote2ErrorsPer10 <= 10; ++remote2ErrorsPer10) {
            int remote1ErrorsPer10 = 1;

            // Fix the RNG so every row uses the same tie-break sequence and only the inputs differ.
            SetRandomSeed(42);

            auto remote1Error = std::make_shared<std::atomic<bool>>(false);
            auto remote2Error = std::make_shared<std::atomic<bool>>(false);
            auto localNeverErrors = std::make_shared<std::atomic<bool>>(false);

            auto remote1 = makeClient(remote1Error, remote1Result, /*clusterId*/ 0);
            auto remote2 = makeClient(remote2Error, remote2Result, /*clusterId*/ 1);
            auto local = makeClient(localNeverErrors, localResult, /*clusterId*/ 2);

            IPenaltyProviderPtr penaltyProvider = CreateDummyPenaltyProvider();
            if (localLagging) {
                penaltyProvider = New<TFixedPenaltyProvider>(
                    THashMap<std::string, TDuration>{{localClusterName, lagPenalty}});
            }

            // Production-eagle config: one hedge delay, BanPenalty (3) < RemoteDataCenterPenalty (10),
            // long ban duration, hedging capped at 20% of requests. Only the hedge delay is scaled down
            // for test speed.
            auto hedgingClient = CreateRemoteAwareHedgingClient(
                {remote1, remote2, local},
                {NApi::EClientPriority::Remote, NApi::EClientPriority::Remote, NApi::EClientPriority::Local},
                /*hedgingRequestDelays*/ {TDuration::MilliSeconds(2)},
                /*remoteDataCenterPenalty*/ TDuration::MilliSeconds(10),
                /*localDataCenter*/ "local-dc",
                /*banPenalty*/ TDuration::MilliSeconds(3),
                /*banDuration*/ TDuration::Hours(1),
                penaltyProvider,
                /*hedgingRatioLimit*/ 0.2);

            int servedLocal = 0;
            int servedRemote1 = 0;
            int servedRemote2 = 0;
            int failed = 0;
            int primaryLocal = 0;
            int primaryRemote1 = 0;
            int primaryRemote2 = 0;
            totalRequests->store(0);
            for (int requestIndex = 0; requestIndex < iterations; ++requestIndex) {
                remote1Error->store(errorAt(requestIndex, /*salt*/ 1, remote1ErrorsPer10));
                remote2Error->store(errorAt(requestIndex, /*salt*/ 2, remote2ErrorsPer10));

                primaryCluster->store(-1);
                auto resultFuture = hedgingClient->ListNode(path);
                // The primary was dispatched synchronously by the call above, so it is recorded by now.
                switch (primaryCluster->load()) {
                    case 0: ++primaryRemote1; break;
                    case 1: ++primaryRemote2; break;
                    case 2: ++primaryLocal; break;
                }

                auto queryResult = NConcurrency::WaitFor(resultFuture);
                if (!queryResult.IsOK()) {
                    ++failed;
                } else if (queryResult.Value().AsStringBuf() == localResult.AsStringBuf()) {
                    ++servedLocal;
                } else if (queryResult.Value().AsStringBuf() == remote1Result.AsStringBuf()) {
                    ++servedRemote1;
                } else {
                    ++servedRemote2;
                }
            }

            table += Sprintf("%12s %10d %10d -> %5d %7d %7d %6d || %5d %7d %7d || %5d\n",
                localLagging ? "true" : "false",
                remote1ErrorsPer10,
                remote2ErrorsPer10,
                servedLocal,
                servedRemote1,
                servedRemote2,
                failed,
                primaryLocal,
                primaryRemote1,
                primaryRemote2,
                totalRequests->load());
        }
    }

    EXPECT_THAT(table, NGTest::GoldenFileEq(SRC_("canondata/hedging_simulation.txt")));
}

////////////////////////////////////////////////////////////////////////////////

} // namespace
} // namespace NYT::NClient::NHedging::NRpc
