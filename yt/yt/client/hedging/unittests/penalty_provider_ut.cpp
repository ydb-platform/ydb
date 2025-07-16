#include <yt/yt/client/hedging/penalty_provider.h>

#include <yt/yt/client/transaction_client/helpers.h>

#include <yt/yt/client/unittests/mock/client.h>

#include <yt/yt/core/profiling/timing.h>

#include <library/cpp/testing/gtest/gtest.h>

namespace NYT::NClient::NHedging::NRpc {

using ::testing::_;
using ::testing::Return;
using ::testing::StrictMock;

using TStrictMockClient = StrictMock<NApi::TMockClient>;

////////////////////////////////////////////////////////////////////////////////

namespace {
    const auto CheckPeriod = TDuration::Seconds(1);

    TReplicationLagPenaltyProviderOptionsPtr GenerateReplicationLagPenaltyProviderConfig(
        const NYPath::TYPath& path,
        const std::string& cluster,
        const TDuration maxLagInSeconds = TDuration::Seconds(10),
        const bool clearPenaltiesOnErrors = false,
        const TDuration checkPeriod = CheckPeriod)
    {
        auto config = New<TReplicationLagPenaltyProviderOptions>();

        config->TablePath = path;
        config->ReplicaClusters.push_back(cluster);
        config->MaxReplicaLag = maxLagInSeconds;
        config->CheckPeriod = checkPeriod;
        config->ClearPenaltiesOnErrors = clearPenaltiesOnErrors;

        return config;
    }
} // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TLagPenaltyProviderTest, UpdateExternalPenaltyWhenReplicaHasLag)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    auto maxLagInSeconds = TDuration::Seconds(10);
    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster, maxLagInSeconds);

    NYson::TYsonString replicasResult(TStringBuf(
        "{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"; \"content_type\" = \"data\"; \"replication_lag_time\" = 20000 }}"));

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), config->LagPenalty);
}

TEST(TLagPenaltyProviderTest, UpdateExternalPenaltyWhenReplicaHasLagWoContentType)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    auto maxLagInSeconds = TDuration::Seconds(10);
    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster, maxLagInSeconds);

    NYson::TYsonString replicasResult(TStringBuf(
        "{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"; \"replication_lag_time\" = 20000 }}"));

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), config->LagPenalty);
}


TEST(TLagPenaltyProviderTest, UpdateExternalPenaltyWhenReplicaHasLagWrongContentType)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    auto maxLagInSeconds = TDuration::Seconds(10);
    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster, maxLagInSeconds);

    NYson::TYsonString replicasResult(TStringBuf(
        "{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"; \"content_type\" = \"queue\"; \"replication_lag_time\" = 20000 }}"));

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), TDuration::Zero());
}

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenReplicaHasNoLag)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf(
        "{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"; \"content_type\" = \"data\"; \"replication_lag_time\" = 5000 }}"));

    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), TDuration::Zero());
}

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenGetReplicaIdFailed)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), TDuration::Zero());
}

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenGetReplicasInfoFailed)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), TDuration::Zero());
}

TEST(TLagPenaltyProviderTest, ClearPenaltiesAfterError)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf(
        "{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"; \"content_type\" = \"data\"; \"replication_lag_time\" = 20000 }}"));

    TDuration maxLagInSeconds = TDuration::Seconds(10);
    auto config =
        GenerateReplicationLagPenaltyProviderConfig(path, cluster, maxLagInSeconds, true, 2 * CheckPeriod);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillOnce(Return(MakeFuture(replicasResult)))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), config->LagPenalty);

    Sleep(2 * CheckPeriod);
    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
