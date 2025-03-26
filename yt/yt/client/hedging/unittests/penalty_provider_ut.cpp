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
        config->MaxTabletsWithLagFraction = 0.5;
        config->MaxTabletLag = maxLagInSeconds;
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

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    auto maxLagInSeconds = TDuration::Seconds(10);
    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster, maxLagInSeconds);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*client, GetNode(path + "/@tablet_count", _))
        .WillRepeatedly(Return(MakeFuture(tabletCountResult)));

    std::vector<NApi::TTabletInfo> tabletInfos(1);
    tabletInfos[0].TableReplicaInfos = std::make_optional(std::vector<NApi::TTabletInfo::TTableReplicaInfo>());
    auto& replicaTabletsInfo = tabletInfos[0].TableReplicaInfos->emplace_back();
    replicaTabletsInfo.ReplicaId = NTabletClient::TTableReplicaId::FromString("575f-131-40502c5-201b420f");
    replicaTabletsInfo.LastReplicationTimestamp = NTransactionClient::TimestampFromUnixTime(
        (TInstant::Now() - 2 * maxLagInSeconds).Seconds());

    EXPECT_CALL(*client, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture(tabletInfos)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), config->LagPenalty);
}

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenReplicaHasNoLag)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*client, GetNode(path + "/@tablet_count", _))
        .WillRepeatedly(Return(MakeFuture(tabletCountResult)));

    std::vector<NApi::TTabletInfo> tabletInfos(1);
    tabletInfos[0].TableReplicaInfos = std::make_optional(std::vector<NApi::TTabletInfo::TTableReplicaInfo>());
    auto& replicaTabletsInfo = tabletInfos[0].TableReplicaInfos->emplace_back();
    replicaTabletsInfo.ReplicaId = NTabletClient::TTableReplicaId::FromString("575f-131-40502c5-201b420f");
    replicaTabletsInfo.LastReplicationTimestamp = NTransactionClient::TimestampFromUnixTime(TInstant::Now().Seconds());

    EXPECT_CALL(*client, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture(tabletInfos)));

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

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenGetTabletsCountFailed)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));

    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*client, GetNode(path + "/@tablet_count", _))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), TDuration::Zero());
}

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenGetTabletsInfoFailed)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    auto config = GenerateReplicationLagPenaltyProviderConfig(path, cluster);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*client, GetNode(path + "/@tablet_count", _))
        .WillRepeatedly(Return(MakeFuture(tabletCountResult)));

    EXPECT_CALL(*client, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture<std::vector<NApi::TTabletInfo>>(TError("Failure"))));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), TDuration::Zero());
}

TEST(TLagPenaltyProviderTest, ClearPenaltiesAfterError)
{
    NYPath::TYPath path = "/test/1234";
    std::string cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    TDuration maxLagInSeconds = TDuration::Seconds(10);
    auto config =
        GenerateReplicationLagPenaltyProviderConfig(path, cluster, maxLagInSeconds, true, 2 * CheckPeriod);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*client, GetNode(path + "/@tablet_count", _))
        .WillOnce(Return(MakeFuture(tabletCountResult)))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    std::vector<NApi::TTabletInfo> tabletInfos(1);
    tabletInfos[0].TableReplicaInfos = std::make_optional(std::vector<NApi::TTabletInfo::TTableReplicaInfo>());
    auto& replicaTabletsInfo = tabletInfos[0].TableReplicaInfos->emplace_back();
    replicaTabletsInfo.ReplicaId = NTabletClient::TTableReplicaId::FromString("575f-131-40502c5-201b420f");
    replicaTabletsInfo.LastReplicationTimestamp = NTransactionClient::TimestampFromUnixTime(
        (TInstant::Now() - 2 * maxLagInSeconds).Seconds());

    EXPECT_CALL(*client, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture(tabletInfos)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), config->LagPenalty);

    Sleep(2 * CheckPeriod);
    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), TDuration::Zero());
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
