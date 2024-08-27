#include <yt/yt/client/hedging/penalty_provider.h>
#include <yt/yt_proto/yt/client/hedging/proto/config.pb.h>

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

    TReplicationLagPenaltyProviderConfig GenerateReplicationLagPenaltyProviderConfig(
            const NYPath::TYPath& path,
            const std::string& cluster,
            const ui32 maxLagInSeconds = 10,
            const bool clearPenaltiesOnErrors = false,
            const TDuration checkPeriod = CheckPeriod)
    {
        TReplicationLagPenaltyProviderConfig config;

        config.SetTablePath(path);
        config.AddReplicaClusters(TProtobufString(cluster));
        config.SetMaxTabletsWithLagFraction(0.5);
        config.SetMaxTabletLag(maxLagInSeconds);
        config.SetCheckPeriod(checkPeriod.Seconds());
        config.SetClearPenaltiesOnErrors(clearPenaltiesOnErrors);

        return config;
    }
} // namespace

////////////////////////////////////////////////////////////////////////////////

TEST(TLagPenaltyProviderTest, UpdateExternalPenaltyWhenReplicaHasLag)
{
    NYPath::TYPath path = "/test/1234";
    TString cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    ui32 maxLagInSeconds = 10;
    TReplicationLagPenaltyProviderConfig config = GenerateReplicationLagPenaltyProviderConfig(path, cluster, maxLagInSeconds);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*client, GetNode(path + "/@tablet_count", _))
        .WillRepeatedly(Return(MakeFuture(tabletCountResult)));

    std::vector<NApi::TTabletInfo> tabletInfos(1);
    tabletInfos[0].TableReplicaInfos = std::make_optional(std::vector<NApi::TTabletInfo::TTableReplicaInfo>());
    auto& replicaTabletsInfo = tabletInfos[0].TableReplicaInfos->emplace_back();
    replicaTabletsInfo.ReplicaId = NTabletClient::TTableReplicaId::FromString("575f-131-40502c5-201b420f");
    replicaTabletsInfo.LastReplicationTimestamp = NTransactionClient::TimestampFromUnixTime(TInstant::Now().Seconds() - 2 * maxLagInSeconds);

    EXPECT_CALL(*client, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture(tabletInfos)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), NProfiling::DurationToCpuDuration(TDuration::MilliSeconds(config.GetLagPenalty())));
}

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenReplicaHasNoLag)
{
    NYPath::TYPath path = "/test/1234";
    TString cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    TReplicationLagPenaltyProviderConfig config = GenerateReplicationLagPenaltyProviderConfig(path, cluster);

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

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), 0);
}

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenGetReplicaIdFailed)
{
    NYPath::TYPath path = "/test/1234";
    TString cluster = "seneca-vla";

    NClient::NHedging::NRpc::TReplicationLagPenaltyProviderConfig config = GenerateReplicationLagPenaltyProviderConfig(path, cluster);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), 0);
}

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenGetTabletsCountFailed)
{
    NYPath::TYPath path = "/test/1234";
    TString cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));

    TReplicationLagPenaltyProviderConfig config = GenerateReplicationLagPenaltyProviderConfig(path, cluster);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*client, GetNode(path + "/@tablet_count", _))
        .WillRepeatedly(Return(MakeFuture<NYson::TYsonString>(TError("Failure"))));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), 0);
}

TEST(TLagPenaltyProviderTest, DoNotUpdatePenaltyWhenGetTabletsInfoFailed)
{
    NYPath::TYPath path = "/test/1234";
    TString cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    NClient::NHedging::NRpc::TReplicationLagPenaltyProviderConfig config =
        GenerateReplicationLagPenaltyProviderConfig(path, cluster);

    auto client = New<TStrictMockClient>();

    EXPECT_CALL(*client, GetNode(path + "/@replicas", _))
        .WillRepeatedly(Return(MakeFuture(replicasResult)));

    EXPECT_CALL(*client, GetNode(path + "/@tablet_count", _))
        .WillRepeatedly(Return(MakeFuture(tabletCountResult)));

    EXPECT_CALL(*client, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture<std::vector<NApi::TTabletInfo>>(TError("Failure"))));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(2 * CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), 0);
}

TEST(TLagPenaltyProviderTest, ClearPenaltiesAfterError)
{
    NYPath::TYPath path = "/test/1234";
    TString cluster = "seneca-vla";

    NYson::TYsonString replicasResult(TStringBuf("{\"575f-131-40502c5-201b420f\" = {\"cluster_name\" = \"seneca-vla\"}}"));
    NYson::TYsonString tabletCountResult(TStringBuf("1"));

    ui32 maxLagInSeconds = 10;
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
    replicaTabletsInfo.LastReplicationTimestamp = NTransactionClient::TimestampFromUnixTime(TInstant::Now().Seconds() - 2 * maxLagInSeconds);

    EXPECT_CALL(*client, GetTabletInfos(path, _, _))
        .WillRepeatedly(Return(MakeFuture(tabletInfos)));

    auto PenaltyProviderPtr = CreateReplicationLagPenaltyProvider(config, client);
    Sleep(CheckPeriod);

    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), NProfiling::DurationToCpuDuration(TDuration::MilliSeconds(config.GetLagPenalty())));

    Sleep(2 * CheckPeriod);
    EXPECT_EQ(PenaltyProviderPtr->Get(cluster), 0);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT::NClient::NHedging::NRpc
