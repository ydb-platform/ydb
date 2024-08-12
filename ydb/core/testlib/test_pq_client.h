#pragma once
#include "test_client.h"

#include <ydb/core/client/flat_ut_client.h>
#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/protos/flat_tx_scheme.pb.h>
#include <ydb/core/mind/address_classification/net_classifier.h>
#include <ydb/public/api/protos/draft/persqueue_error_codes.pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>
#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_table/table.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/testing/unittest/registar.h>

#include <util/string/printf.h>
#include <util/system/tempfile.h>

namespace NKikimr {
namespace NPersQueueTests {

using namespace NNetClassifier;
using namespace NKikimr::Tests;

const static ui32 PQ_DEFAULT_NODE_COUNT = 2;

inline Tests::TServerSettings PQSettings(ui16 port = 0, ui32 nodesCount = PQ_DEFAULT_NODE_COUNT, const TString& yql_timeout = "10", const THolder<TTempFileHandle>& netDataFile = nullptr) {
    NKikimrPQ::TPQConfig pqConfig;
    NKikimrProto::TAuthConfig authConfig;
    authConfig.SetUseBuiltinDomain(true);
    authConfig.SetUseBlackBox(false);
    authConfig.SetUseAccessService(false);
    authConfig.SetUseAccessServiceTLS(false);
    authConfig.SetUseStaff(false);

    pqConfig.SetEnabled(true);
    pqConfig.SetMaxReadCookies(10);

    // NOTE(shmel1k@): KIKIMR-14221
    pqConfig.SetRequireCredentialsInNewProtocol(false);
    pqConfig.SetClusterTablePath("/Root/PQ/Config/V2/Cluster");
    pqConfig.SetVersionTablePath("/Root/PQ/Config/V2/Versions");
    pqConfig.SetTopicsAreFirstClassCitizen(false);
    pqConfig.SetUseSrcIdMetaMappingInFirstClass(false);
    pqConfig.SetRoot("/Root/PQ");
    pqConfig.MutableQuotingConfig()->SetEnableQuoting(false);
    pqConfig.MutableQuotingConfig()->SetQuotersDirectoryPath("/Root/PersQueue/System/Quoters");

    for (int i = 0; i < 12; ++i) {
        auto profile = pqConfig.AddChannelProfiles();
        Y_UNUSED(profile);
        profile->SetPoolKind("test");
    }

    Tests::TServerSettings settings(port, authConfig, pqConfig);
    settings.SetDomainName("Root").SetNodeCount(nodesCount);

    TVector<NKikimrKqp::TKqpSetting> kqpSettings;
    NKikimrKqp::TKqpSetting kqpSetting;
    kqpSetting.SetName("_KqpQueryTimeout");

    kqpSetting.SetValue(yql_timeout);
    kqpSettings.push_back(kqpSetting);
    settings.SetKqpSettings(kqpSettings);
    settings.PQClusterDiscoveryConfig.SetEnabled(true);
    settings.PQClusterDiscoveryConfig.SetTimedCountersUpdateIntervalSeconds(1);

    if (netDataFile)
        settings.NetClassifierConfig.SetNetDataFilePath(netDataFile->Name());

    return settings;
}

// deprecated.
inline Tests::TServerSettings PQSettings(ui16 port, ui32 nodesCount, bool roundrobin, const TString& yql_timeout = "10", const THolder<TTempFileHandle>& netDataFile = nullptr) {
    Y_UNUSED(roundrobin);

    return PQSettings(port, nodesCount, yql_timeout, netDataFile);
}

const TString TopicPrefix = "/Root/PQ/";
const static TString DEFAULT_SRC_IDS_PATH = "/Root/PQ/SourceIdMeta2";


struct TRequestCreatePQ {
    TRequestCreatePQ(
        const TString& topic,
        ui32 numParts,
        ui32 cacheSize = 0,
        ui64 lifetimeS = 86400,
        ui32 lowWatermark = 8 * 1024 * 1024,
        ui64 writeSpeed = 20000000,
        const TString& user = "",
        ui64 readSpeed = 20000000,
        const TVector<TString>& readRules = {"user"},
        const TVector<TString>& important = {},
        std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom = {},
        ui64 sourceIdMaxCount = 6000000,
        ui64 sourceIdLifetime = 86400
    )
        : Topic(topic)
        , NumParts(numParts)
        , CacheSize(cacheSize)
        , LifetimeS(lifetimeS)
        , LowWatermark(lowWatermark)
        , WriteSpeed(writeSpeed)
        , User(user)
        , ReadSpeed(readSpeed)
        , ReadRules(readRules)
        , Important(important)
        , MirrorFrom(mirrorFrom)
        , SourceIdMaxCount(sourceIdMaxCount)
        , SourceIdLifetime(sourceIdLifetime)
    {}

    TString Topic;
    ui32 NumParts;
    ui32 CacheSize;
    ui64 LifetimeS;
    ui32 LowWatermark;

    ui64 WriteSpeed;

    TString User;
    ui64 ReadSpeed;

    TVector<TString> ReadRules;
    TVector<TString> Important;

    std::optional<NKikimrPQ::TMirrorPartitionConfig> MirrorFrom;

    ui64 SourceIdMaxCount;
    ui64 SourceIdLifetime;

    THolder<NMsgBusProxy::TBusPersQueue> GetRequest() const {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutableMetaRequest()->MutableCmdCreateTopic();
        req->SetTopic(Topic);
        req->SetNumPartitions(NumParts);
        auto config = req->MutableConfig();
        if (CacheSize)
            config->SetCacheSize(CacheSize);
        //config->SetTopicName(Topic);
        //config->SetTopicPath(TString("/Root/PQ/") + Topic);
        config->MutablePartitionConfig()->SetLifetimeSeconds(LifetimeS);
        config->MutablePartitionConfig()->SetSourceIdLifetimeSeconds(SourceIdLifetime);
        config->MutablePartitionConfig()->SetSourceIdMaxCounts(SourceIdMaxCount);
        config->MutablePartitionConfig()->SetLowWatermark(LowWatermark);

        config->SetLocalDC(true);

        auto codec = config->MutableCodecs();
        codec->AddIds(0);
        codec->AddCodecs("raw");
        codec->AddIds(1);
        codec->AddCodecs("gzip");
        codec->AddIds(2);
        codec->AddCodecs("lzop");

        for (auto& i : Important) {
            config->MutablePartitionConfig()->AddImportantClientId(i);
        }

        config->MutablePartitionConfig()->SetWriteSpeedInBytesPerSecond(WriteSpeed);
        config->MutablePartitionConfig()->SetBurstSize(WriteSpeed);
        for (auto& rr : ReadRules) {
            config->AddReadRules(rr);
            config->AddReadFromTimestampsMs(0);
            config->AddConsumerFormatVersions(0);
            config->AddReadRuleVersions(0);
            config->AddConsumerCodecs();
        }
//        if (!ReadRules.empty()) {
//            config->SetRequireAuthRead(true);
//        }
        if (!User.empty()) {
            auto rq = config->MutablePartitionConfig()->AddReadQuota();
            rq->SetSpeedInBytesPerSecond(ReadSpeed);
            rq->SetBurstSize(ReadSpeed);
            rq->SetClientId(User);
        }

        if (MirrorFrom) {
            auto mirrorFromConfig = config->MutablePartitionConfig()->MutableMirrorFrom();
            mirrorFromConfig->CopyFrom(MirrorFrom.value());
        }
        return request;
    }
};


struct TRequestAlterPQ {
    TRequestAlterPQ(
        const TString& topic,
        ui32 numParts,
        ui64 cacheSize = 0,
        ui64 lifetimeS = 86400,
        bool fillPartitionConfig = false,
        std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom = {}
    )
        : Topic(topic)
        , NumParts(numParts)
        , CacheSize(cacheSize)
        , LifetimeS(lifetimeS)
        , FillPartitionConfig(fillPartitionConfig)
        , MirrorFrom(mirrorFrom)
    {}

    TString Topic;
    ui32 NumParts;
    ui64 CacheSize;
    ui64 LifetimeS;
    bool FillPartitionConfig;
    std::optional<NKikimrPQ::TMirrorPartitionConfig> MirrorFrom;

    THolder<NMsgBusProxy::TBusPersQueue> GetRequest() {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutableMetaRequest()->MutableCmdChangeTopic();
        req->SetTopic(Topic);
        req->SetNumPartitions(NumParts);

        if (CacheSize) {
            auto* config = req->MutableConfig();
//            config->SetTopicName(Topic);
            config->SetCacheSize(CacheSize);
        }
        if (FillPartitionConfig) {
            auto* config = req->MutableConfig();
//            config->SetTopicName(Topic);
            config->MutablePartitionConfig()->SetLifetimeSeconds(LifetimeS);
            if (MirrorFrom) {
                config->MutablePartitionConfig()->MutableMirrorFrom()->CopyFrom(MirrorFrom.value());
            }
        }
        return request;
    }
};

struct TRequestDeletePQ {
    TRequestDeletePQ(const TString& topic)
        : Topic(topic)
    {}

    TString Topic;

    THolder<NMsgBusProxy::TBusPersQueue> GetRequest() {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutableMetaRequest()->MutableCmdDeleteTopic();
        req->SetTopic(Topic);
        return request;
    }
};

struct TRequestGetOwnership {
    TRequestGetOwnership(const TString& topic, ui32 partition)
        : Topic(topic)
        , Partition(partition)
    {}

    TString Topic;
    ui32 Partition;

    THolder<NMsgBusProxy::TBusPersQueue> GetRequest() const {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutablePartitionRequest();
        req->SetTopic(Topic);
        req->SetPartition(Partition);
        req->MutableCmdGetOwnership();
        return request;
    }
};


struct TRequestWritePQ {
    TRequestWritePQ(const TString& topic, ui32 partition, const TString& sourceId, ui64 seqNo)
        : Topic(topic)
        , Partition(partition)
        , SourceId(sourceId)
        , SeqNo(seqNo)
    {}

    TString Topic;
    ui32 Partition;
    TString SourceId;
    ui64 SeqNo;

    THolder<NMsgBusProxy::TBusPersQueue> GetRequest(const TString& data, const TString& cookie) const {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutablePartitionRequest();
        req->SetTopic(Topic);
        req->SetPartition(Partition);
        req->SetMessageNo(0);
        req->SetOwnerCookie(cookie);
        auto write = req->AddCmdWrite();
        write->SetSourceId(SourceId);
        write->SetSeqNo(SeqNo);
        write->SetData(data);
        return request;
    }
};

struct TRequestReadPQ {
    TRequestReadPQ(
        const TString& topic,
        ui32 partition,
        ui64 startOffset,
        ui32 count,
        const TString& user,
        ui64 readTimestampMs = 0
    )
        : Topic(topic)
        , Partition(partition)
        , StartOffset(startOffset)
        , Count(count)
        , User(user)
        , ReadTimestampMs(readTimestampMs)
    {}

    TString Topic;
    ui32 Partition;
    ui64 StartOffset;
    ui32 Count;
    TString User;
    ui64 ReadTimestampMs;

    THolder<NMsgBusProxy::TBusPersQueue> GetRequest() const {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutablePartitionRequest();
        req->SetTopic(Topic);
        req->SetPartition(Partition);
        auto read = req->MutableCmdRead();
        read->SetOffset(StartOffset);
        read->SetCount(Count);
        read->SetClientId(User);
        read->SetReadTimestampMs(ReadTimestampMs);
        return request;
    }
};

struct TRequestSetClientOffsetPQ {
    TRequestSetClientOffsetPQ(
        const TString& topic,
        ui32 partition,
        ui64 offset,
        const TString& user
    )
        : Topic(topic)
        , Partition(partition)
        , Offset(offset)
        , User(user)
    {}

    TString Topic;
    ui32 Partition;
    ui64 Offset;
    TString User;

    THolder<NMsgBusProxy::TBusPersQueue> GetRequest() const {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutablePartitionRequest();
        req->SetTopic(Topic);
        req->SetPartition(Partition);
        auto cmd = req->MutableCmdSetClientOffset();
        cmd->SetOffset(Offset);
        cmd->SetClientId(User);
        return request;
    }
};

struct FetchPartInfo {
    TString Topic;
    i32 Partition;
    ui64 Offset;
    ui32 MaxBytes;
};

struct TFetchRequestPQ {
    THolder<NMsgBusProxy::TBusPersQueue> GetRequest(const TVector<FetchPartInfo>& fetchParts, ui32 maxBytes, ui32 waitMs) {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutableFetchRequest();
        req->SetWaitMs(waitMs);
        req->SetTotalMaxBytes(maxBytes);
        req->SetClientId("user");
        for (const auto& t : fetchParts) {
            auto part = req->AddPartition();
            part->SetTopic(t.Topic);
            part->SetPartition(t.Partition);
            part->SetOffset(t.Offset);
            part->SetMaxBytes(t.MaxBytes);
        }
        return request;
    }
};

struct TRequestGetPartOffsets {
    THolder<NMsgBusProxy::TBusPersQueue> GetRequest(const TVector<std::pair<TString, TVector<ui32>>>& topicsAndParts) {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutableMetaRequest();
        auto partOff = req->MutableCmdGetPartitionOffsets();
        partOff->SetClientId("user");
        for (const auto& t : topicsAndParts) {
            auto req = partOff->AddTopicRequest();
            req->SetTopic(t.first);
            for (const auto& p : t.second) {
                req->AddPartition(p);
            }
        }
        return request;
    }
};

struct TRequestGetClientInfo {
    THolder<NMsgBusProxy::TBusPersQueue> GetRequest(const TVector<TString>& topics, const TString& user) {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutableMetaRequest();
        auto partOff = req->MutableCmdGetReadSessionsInfo();
        partOff->SetClientId(user);
        for (const auto& t : topics) {
            partOff->AddTopic(t);
        }
        return request;
    }
};


struct TRequestGetPartStatus {
    THolder<NMsgBusProxy::TBusPersQueue> GetRequest(const TVector<std::pair<TString, TVector<ui32>>>& topicsAndParts) {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutableMetaRequest();
        auto partOff = req->MutableCmdGetPartitionStatus();
        partOff->SetClientId("user1");
        for (const auto& t : topicsAndParts) {
            auto req = partOff->AddTopicRequest();
            req->SetTopic(t.first);
            for (const auto& p : t.second) {
                req->AddPartition(p);
            }
        }
        return request;
    }
};

struct TRequestGetPartLocations {
    THolder<NMsgBusProxy::TBusPersQueue> GetRequest(const TVector<std::pair<TString, TVector<ui32>>>& topicsAndParts) {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutableMetaRequest();
        auto partOff = req->MutableCmdGetPartitionLocations();
        for (const auto& t : topicsAndParts) {
            auto req = partOff->AddTopicRequest();
            req->SetTopic(t.first);
            for (const auto& p : t.second) {
                req->AddPartition(p);
            }
        }
        return request;
    }
};

struct TRequestDescribePQ {
    THolder<NMsgBusProxy::TBusPersQueue> GetRequest(const TVector<TString>& topics) const {
        THolder<NMsgBusProxy::TBusPersQueue> request(new NMsgBusProxy::TBusPersQueue);
        auto req = request->Record.MutableMetaRequest();
        auto partOff = req->MutableCmdGetTopicMetadata();
        for (const auto& t : topics) {
            partOff->AddTopic(t);
        }
        return request;
    }
};

struct TPQTestClusterInfo {
    TString Balancer;
    bool Enabled;
    ui64 Weight = 1000;
};

static THashMap<TString, TPQTestClusterInfo> DEFAULT_CLUSTERS_LIST = {
    {"dc1", {"localhost", true}},
    {"dc2", {"dc2.logbroker.yandex.net", true}}
};

static THashMap<TString, TPQTestClusterInfo> CLUSTERS_LIST_ONE_DC = {
        {"dc1", {"localhost", true}}
};

class TFlatMsgBusPQClient : public NFlatTests::TFlatMsgBusClient {
private:
    static constexpr ui32 FlatDomain = 0;
    static constexpr bool FlatSupportsRedirect = true;
    const Tests::TServerSettings Settings;
    const ui16 GRpcPort;
    NClient::TKikimr Kikimr;
    THolder<NYdb::TDriver> Driver;
    std::unique_ptr<NKikimrClient::TGRpcServer::Stub> Stub;

    ui64 TopicsVersion = 0;
    bool UseConfigTables = true;
public:
    void RunYqlSchemeQuery(TString query, bool expectSuccess = true) {
        auto tableClient = NYdb::NTable::TTableClient(*Driver);

        NYdb::TStatus result(NYdb::EStatus::SUCCESS, NYql::TIssues());
        for (size_t i = 0; i < 10; ++i) {
            result = tableClient.RetryOperationSync([&](NYdb::NTable::TSession session) {
                return session.ExecuteSchemeQuery(query).GetValueSync();
            });
            if (!expectSuccess || result.IsSuccess()) {
                break;
            }
            Sleep(TDuration::Seconds(1));
        }

        if (expectSuccess) {
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        } else {
            UNIT_ASSERT(!result.IsSuccess());
        }
    }

    TMaybe<NYdb::TResultSet> RunYqlDataQueryWithParams(TString query, const NYdb::TParams& params) {
        auto tableClient = NYdb::NTable::TTableClient(*Driver);
        TMaybe<NYdb::TResultSet> rs;
        auto result = tableClient.RetryOperationSync([&](NYdb::NTable::TSession session) {
            auto qr = session.ExecuteDataQuery(
                query,
                NYdb::NTable::TTxControl::BeginTx(NYdb::NTable::TTxSettings::SerializableRW()).CommitTx(),
                params).GetValueSync();

            if (qr.IsSuccess() && qr.GetResultSets().size() > 0) {
                rs = qr.GetResultSet(0);
            }
            return qr;
        });
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        return rs;
    }

    TMaybe<NYdb::TResultSet> RunYqlDataQuery(TString query) {
        NYdb::TParamsBuilder builder;
        return RunYqlDataQueryWithParams(query, builder.Build());
    }


    TFlatMsgBusPQClient(const Tests::TServerSettings& settings, ui16 grpc, TMaybe<TString> databaseName = Nothing())
        : TFlatMsgBusClient(settings)
        , Settings(settings)
        , GRpcPort(grpc)
        , Kikimr(GetClientConfig())
    {
        TString endpoint = TStringBuilder() << "localhost:" << GRpcPort;
        auto driverConfig = NYdb::TDriverConfig()
            .SetEndpoint(endpoint)
            .SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG));
        if (databaseName)
            driverConfig.SetDatabase(*databaseName);
        Driver.Reset(MakeHolder<NYdb::TDriver>(driverConfig));

        grpc::ChannelArguments args;
        if (settings.GrpcMaxMessageSize != 0)
        {
            args.SetMaxReceiveMessageSize(settings.GrpcMaxMessageSize);
            args.SetMaxSendMessageSize(settings.GrpcMaxMessageSize);
        }
        auto channel = grpc::CreateCustomChannel(endpoint, grpc::InsecureChannelCredentials(), args);

        Stub = NKikimrClient::TGRpcServer::NewStub(channel);

        Cerr << "PQClient connected to " << endpoint << Endl;
    }

    ~TFlatMsgBusPQClient() {
        Driver->Stop(true);
    }

    void SetNoConfigMode() {
        UseConfigTables = false;
    }

    void FullInit(
            THashMap<TString, TPQTestClusterInfo> clusters = DEFAULT_CLUSTERS_LIST,
            const TString& srcIdsPath = DEFAULT_SRC_IDS_PATH,
            const TString& localCluster = TString()
    ) {
        InitRoot();
        if (UseConfigTables) {
            InitSourceIds(srcIdsPath);
            InitDCs(clusters, localCluster);
        } else {
            InitSourceIds({});
        }
    }
    void InitRoot() {
        InitRootScheme();
        MkDir("/Root", "PQ");
    }

    NYdb::TDriver* GetDriver() {
        return Driver.Get();
    }

    void InitSourceIds(const TString& path = DEFAULT_SRC_IDS_PATH) {
        TFsPath fsPath(path);
        CreateTable(fsPath.Dirname(),
           "Name: \"" + fsPath.Basename() + "\""
           "Columns { Name: \"Hash\"             Type: \"Uint32\"}"
           "Columns { Name: \"SourceId\"         Type: \"Utf8\"}"
           "Columns { Name: \"Topic\"            Type: \"Utf8\"}"
           "Columns { Name: \"Partition\"        Type: \"Uint32\"}"
           "Columns { Name: \"CreateTime\"       Type: \"Uint64\"}"
           "Columns { Name: \"AccessTime\"       Type: \"Uint64\"}"
           "Columns { Name: \"SeqNo\"            Type: \"Uint64\"}"
           "KeyColumnNames: [\"Hash\", \"SourceId\", \"Topic\"]"
        );
    }

    void InsertSourceId(ui32 hash, TString sourceId, ui64 accessTime, const TString& path = "/Root/PQ/SourceIdMeta2") {
        TString query =
            "DECLARE $Hash AS Uint32; "
            "DECLARE $SourceId AS Utf8; "
            "DECLARE $AccessTime AS Uint64; "
            "UPSERT INTO [" + path + "] (Hash, SourceId, Topic, Partition, CreateTime, AccessTime) "
            "VALUES($Hash, $SourceId, \"1\", 0, 0, $AccessTime); ";

        NYdb::TParamsBuilder builder;
        auto params = builder
            .AddParam("$Hash").Uint32(hash).Build()
            .AddParam("$SourceId").Utf8(sourceId).Build()
            .AddParam("$AccessTime").Uint64(accessTime).Build()
            .Build();

        RunYqlDataQueryWithParams(query, params);
    }

    THashMap<TString, TInstant> ListSourceIds(const TString& path = "/Root/PQ/SourceIdMeta2") {
        auto result = RunYqlDataQuery("SELECT SourceId, AccessTime FROM [" + path + "];");
        NYdb::TResultSetParser parser(*result);
        THashMap<TString, TInstant> sourceIds;
        while(parser.TryNextRow()) {
            TString sourceId = *parser.ColumnParser("SourceId").GetOptionalUtf8();
            TInstant accessTime = TInstant::MilliSeconds(*parser.ColumnParser("AccessTime").GetOptionalUint64());
            sourceIds[sourceId] = accessTime;
        }
        return sourceIds;
    }

    void InitDCs(THashMap<TString, TPQTestClusterInfo> clusters = DEFAULT_CLUSTERS_LIST, const TString& localCluster = TString()) {
        MkDir("/Root/PQ", "Config");
        MkDir("/Root/PQ/Config", "V2");
        RunYqlSchemeQuery(R"___(
            CREATE TABLE `/Root/PQ/Config/V2/Cluster` (
                name Utf8,
                balancer Utf8,
                local Bool,
                enabled Bool,
                weight Uint64,
                PRIMARY KEY (name)
            );
            CREATE TABLE `/Root/PQ/Config/V2/Topics` (
                path Utf8,
                dc Utf8,
                PRIMARY KEY (path, dc)
            );
        )___");

        RunYqlSchemeQuery(R"___(
            CREATE TABLE `/Root/PQ/Config/V2/Versions` (
                name Utf8,
                version Int64,
                PRIMARY KEY (name)
            );
        )___");

        TStringBuilder upsertClusters;
        upsertClusters <<  "UPSERT INTO `/Root/PQ/Config/V2/Cluster` (name, balancer, local, enabled, weight) VALUES ";
        bool first = true;
        for (auto& [cluster, info] : clusters) {
            bool isLocal = localCluster.empty() ? first : localCluster == cluster;
            if (!first)
                upsertClusters << ", ";
            upsertClusters << "(\"" << cluster << "\", \"" << info.Balancer << "\", " << (isLocal ? "true" : "false")
                           << ", " << (info.Enabled ? "true" : "false") << ", " << info.Weight << ")";
            first = false;
        }
        upsertClusters << ";\n";
        TString clustersStr = clusters.empty() ? "" : TString(upsertClusters);
        Cerr << "=== Init DC: " << clustersStr << Endl;
        RunYqlDataQuery(clustersStr + R"___(
            UPSERT INTO `/Root/PQ/Config/V2/Versions` (name, version) VALUES ("Cluster", 1);
            UPSERT INTO `/Root/PQ/Config/V2/Versions` (name, version) VALUES ("Topics", 0);
        )___");
    }

    void CheckClustersList(TTestActorRuntime* runtime, bool waitForUpdate = true, THashMap<TString, TPQTestClusterInfo> clusters = DEFAULT_CLUSTERS_LIST) {
        UNIT_ASSERT(runtime != nullptr);

        auto compareInfo = [](const TString& name, const TPQTestClusterInfo& info, const NPQ::NClusterTracker::TClustersList::TCluster& trackerInfo) {
            UNIT_ASSERT_EQUAL(name, trackerInfo.Name);
            UNIT_ASSERT_EQUAL(name, trackerInfo.Datacenter);
            UNIT_ASSERT_EQUAL(info.Balancer, trackerInfo.Balancer);
            UNIT_ASSERT_EQUAL(info.Enabled, trackerInfo.IsEnabled);
            UNIT_ASSERT_EQUAL(info.Weight, trackerInfo.Weight);
        };

        TInstant now = TInstant::Now();


        auto edgeActor = runtime->AllocateEdgeActor();
        Cerr << "=== CheckClustersList. Subcribe to ClusterTracker from " << edgeActor << " \n";
        runtime->Send(new IEventHandle(NKikimr::NPQ::NClusterTracker::MakeClusterTrackerID(), edgeActor, new NPQ::NClusterTracker::TEvClusterTracker::TEvSubscribe));

        while (true) {
            auto trackerResponse = runtime->GrabEdgeEvent<NKikimr::NPQ::NClusterTracker::TEvClusterTracker::TEvClustersUpdate>();

            if (!waitForUpdate || trackerResponse->ClustersListUpdateTimestamp && trackerResponse->ClustersListUpdateTimestamp.GetRef() >= now + TDuration::Seconds(5)) {
                for (auto& clusterInfo : trackerResponse->ClustersList->Clusters) {
                    auto it = clusters.find(clusterInfo.Name);
                    UNIT_ASSERT(it != clusters.end());
                    compareInfo(it->first, it->second, clusterInfo);
                }
                Cerr << "=== CheckClustersList. Ok\n";
                break;
            }
        }
    }

    void UpdateDcEnabled(const TString& name, bool enabled) {
        TStringBuilder query;
        query << "UPDATE `/Root/PQ/Config/V2/Cluster` SET enabled = " << (enabled ? "true" : "false")
              << " where name = \"" << name << "\";";
        Cerr << "===Update clusters: " << query << Endl;
        RunYqlDataQuery(query);
    }

    TPQTestClusterInfo GetDcInfo(const TString& name) {
        TStringBuilder query;
        query << "SELECT balancer, enabled, weight FROM `/Root/PQ/Config/V2/Cluster` where name = \"" << name << "\";";
        auto result = RunYqlDataQuery(query);
        NYdb::TResultSetParser parser(*result);
        UNIT_ASSERT_VALUES_EQUAL(parser.RowsCount(), 1);
        parser.TryNextRow();

        TPQTestClusterInfo info;
        info.Balancer = *parser.ColumnParser("balancer").GetOptionalUtf8();
        info.Enabled = *parser.ColumnParser("enabled").GetOptionalBool();
        info.Weight = *parser.ColumnParser("weight").GetOptionalUint64();
        return info;
    }

    void InitUserRegistry() {
        MkDir("/Root/PQ", "Config");
        MkDir("/Root/PQ/Config", "V2");

        RunYqlSchemeQuery(R"___(
            CREATE TABLE `/Root/PQ/Config/V2/Consumer` (
                name Utf8,
                tvmClientId Utf8,
                PRIMARY KEY (name)
            );
            CREATE TABLE `/Root/PQ/Config/V2/Producer` (
                name Utf8,
                tvmClientId Utf8,
                PRIMARY KEY (name)
            );
        )___");

    }

    void UpdateDC(const TString& name, bool local, bool enabled) {
        const TString query = Sprintf(
            R"___(
                UPSERT INTO `/Root/PQ/Config/V2/Cluster` (name, local, enabled) VALUES
                    ("%s", %s, %s);
                UPSERT INTO `/Root/PQ/Config/V2/Versions` (name, version)
                    SELECT name, version + 1 FROM `/Root/PQ/Config/V2/Versions` WHERE name == "Cluster";
            )___", name.c_str(), (local ? "true" : "false"), (enabled ? "true" : "false"));

        RunYqlDataQuery(query);
    }

    void DisableDC() {
        UpdateDC("dc1", true, false);
    }

    void RestartSchemeshard(TTestActorRuntime* runtime) {
        TActorId sender = runtime->AllocateEdgeActor();
        const ui64 schemeRoot = GetPatchedSchemeRoot(Tests::SchemeRoot, Settings.Domain, Settings.SupportsRedirect);
        ForwardToTablet(*runtime, schemeRoot, sender, new TEvents::TEvPoisonPill(), 0);
        TDispatchOptions options;
        runtime->DispatchEvents(options);
    }

    NKikimrClient::TResponse RequestTopicMetadata(const TString& name) {
        NKikimrClient::TPersQueueRequest request;
        request.MutableMetaRequest()->MutableCmdGetTopicMetadata()->AddTopic(name);

        return CallPersQueueGRPC(request);
    }

    ui32 GetTopicVersionFromMetadata(const TString& name, ui64 cacheSize = 0)
    {
        auto response = RequestTopicMetadata(name);

        if (response.GetErrorCode() != (ui32)NPersQueue::NErrorCode::OK)
            return 0;

        UNIT_ASSERT(response.HasMetaResponse());
        const auto& metaResp = response.GetMetaResponse();
        UNIT_ASSERT(metaResp.HasCmdGetTopicMetadataResult());
        const auto& resp = metaResp.GetCmdGetTopicMetadataResult();
        UNIT_ASSERT(resp.TopicInfoSize() == 1);
        const auto& topicInfo = resp.GetTopicInfo(0);
        UNIT_ASSERT(topicInfo.GetTopic() == name);
        //UNIT_ASSERT(topicInfo.GetConfig().GetTopicName() == name);
        if (cacheSize) {
            UNIT_ASSERT(topicInfo.GetConfig().HasCacheSize());
            ui64 actualSize = topicInfo.GetConfig().GetCacheSize();
            if (actualSize != cacheSize)
                return 0;
        }
        Cerr << "=== Topic created, have version: " << topicInfo.GetConfig().GetVersion() << Endl;
        return topicInfo.GetConfig().GetVersion();
    }

    ui32 GetTopicVersionFromPath(const TString& name) {
        TAutoPtr<NMsgBusProxy::TBusResponse> res = Ls("/Root/PQ/" + name);
        ui32 version = res->Record.GetPathDescription().GetPersQueueGroup().GetAlterVersion();
        Cerr << "GetTopicVersionFromPath: " << " record " <<  res->Record.DebugString()  << "\n name " << name << " version" << version << "\n";
        return version;
    }

    void RestartBalancerTablet(TTestActorRuntime* runtime, const TString& topic) {
        TAutoPtr<NMsgBusProxy::TBusResponse> res = Ls("/Root/PQ/" + topic);
        Cerr << res->Record << "\n";
        const ui64 tablet = res->Record.GetPathDescription().GetPersQueueGroup().GetBalancerTabletID();
        TActorId sender = runtime->AllocateEdgeActor();
        ForwardToTablet(*runtime, tablet, sender, new TEvents::TEvPoisonPill(), 0);
        TDispatchOptions options;
        runtime->DispatchEvents(options);
    }


    void RestartPartitionTablets(TTestActorRuntime* runtime, const TString& topic) {
        TAutoPtr<NMsgBusProxy::TBusResponse> res = Ls("/Root/PQ/" + topic);
        Cerr << res->Record << "\n";
        const auto& pq = res->Record.GetPathDescription().GetPersQueueGroup();
        THashSet<ui64> tablets;
        for (ui32 i = 0; i < pq.PartitionsSize(); ++i) {
            tablets.insert(pq.GetPartitions(i).GetTabletId());
        }
        TActorId sender = runtime->AllocateEdgeActor();
        for (auto & tablet : tablets) {
            ForwardToTablet(*runtime, tablet, sender, new TEvents::TEvPoisonPill(), 0);
            TDispatchOptions options;
            try {
                runtime->DispatchEvents(options);
            } catch (TEmptyEventQueueException&) {
            }
        }
    }

    bool IsTopicDeleted(const TString& name) {
        auto response = RequestTopicMetadata(name);

        return response.GetErrorCode() == (ui32)NPersQueue::NErrorCode::UNKNOWN_TOPIC;
    }

    NKikimrClient::TResponse CallPersQueueGRPC(const NKikimrClient::TPersQueueRequest& request, ui64 maxPrintSize = 1000) {
        Cerr << "CallPersQueueGRPC request to localhost:" << GRpcPort << "\n"
             << PrintToString(request, maxPrintSize) << Endl;

        NKikimrClient::TResponse response;
        grpc::ClientContext context;
        grpc::Status status = Stub->PersQueueRequest(&context, request, &response);

        UNIT_ASSERT_C(status.ok(), status.error_message());
        UNIT_ASSERT(response.HasErrorCode());

        Cerr << "CallPersQueueGRPC response:\n" << PrintToString(response, maxPrintSize) << Endl;

        return response;
    }

    void CreateConsumer(const TString& oldName) {
        auto name = NPersQueue::ConvertOldConsumerName(oldName);
        RunYqlSchemeQuery("CREATE TABLE `/Root/PQ/" + name + "` (" + "Topic Utf8, Partition Uint32, Offset Uint64,  PRIMARY KEY (Topic,Partition) );");
    }

    void GrantConsumerAccess(const TString& oldName, const TString& subj) {
        NACLib::TDiffACL acl;
        // in future use right UseConsumer
        acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, subj);
        auto name = NPersQueue::ConvertOldConsumerName(oldName);
        auto pos = name.rfind("/");
        Y_ABORT_UNLESS(pos != TString::npos);
        auto pref = "/Root/PQ/" + name.substr(0, pos);
        ModifyACL(pref, name.substr(pos + 1), acl.SerializeAsString());
    }

    void CreateTopicNoLegacy(const TString& name, ui32 partsCount, bool doWait = true, bool canWrite = true,
                             const TMaybe<TString>& dc = Nothing(), TVector<TString> rr = {"user"},
                             const TMaybe<TString>& account = Nothing(), bool expectFail = false)
    {
        CreateTopicNoLegacy({
            .Name = name,
            .PartsCount = partsCount,
            .DoWait = doWait,
            .CanWrite = canWrite,
            .Dc = dc,
            .ReadRules = rr,
            .Account = account,
            .ExpectFail = expectFail
        });
    }

    void WaitTopicInit(const TString& topic) {
        auto pqClient = NYdb::NPersQueue::TPersQueueClient(*Driver);
        do {
            auto writer = pqClient.CreateWriteSession(NYdb::NPersQueue::TWriteSessionSettings().Path(topic)
                                    .MessageGroupId("src").ClusterDiscoveryMode(NYdb::NPersQueue::EClusterDiscoveryMode::Off));
            auto ev = *(writer->GetEvent(true));
            if (std::holds_alternative<NYdb::NPersQueue::TWriteSessionEvent::TReadyToAcceptEvent>(ev))
                break;
            if (std::holds_alternative<NYdb::NPersQueue::TSessionClosedEvent>(ev)) {
                Cerr << std::get<NYdb::NPersQueue::TSessionClosedEvent>(ev).DebugString() << "\n";
            }
            Sleep(TDuration::MilliSeconds(100));
        } while (true);
    }

    void CreateTopic(const TRequestCreatePQ& createRequest, bool doWait = true) {
        const TInstant start = TInstant::Now();

        ui32 prevVersion = GetTopicVersionFromMetadata(createRequest.Topic);

        CallPersQueueGRPC(createRequest.GetRequest()->Record);

        AddTopic(createRequest.Topic);
        while (doWait && GetTopicVersionFromPath(createRequest.Topic) != prevVersion + 1) {
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT(TInstant::Now() - start < ::DEFAULT_DISPATCH_TIMEOUT);
        }
        while (doWait && GetTopicVersionFromMetadata(createRequest.Topic, prevVersion) != prevVersion + 1) {
            Sleep(TDuration::MilliSeconds(500));
            UNIT_ASSERT(TInstant::Now() - start < ::DEFAULT_DISPATCH_TIMEOUT);
        }
    }

    void CreateTopic(
        const TString& name,
        ui32 nParts,
        ui32 lowWatermark = 8*1024*1024,
        ui64 lifetimeS = 86400,
        ui64 writeSpeed = 20000000,
        TString user = "",
        ui64 readSpeed = 200000000,
        TVector<TString> rr = {"user"},
        TVector<TString> important = {},
        std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom = {},
        ui64 sourceIdMaxCount = 6000000,
        ui64 sourceIdLifetime = 86400
    ) {
        Y_ABORT_UNLESS(name.StartsWith("rt3."));

        Cerr << "PQ Client: create topic: " << name << " with " << nParts << " partitions" << Endl;
        auto request = TRequestCreatePQ(
                name, nParts, 0, lifetimeS, lowWatermark, writeSpeed, user, readSpeed, rr, important, mirrorFrom,
                sourceIdMaxCount, sourceIdLifetime
        );
        return CreateTopic(request);
    }

    void AlterTopicNoLegacy(const TString& name, ui32 nParts, ui64 lifetimeS = 86400) {
        TString path = name;
        if (!UseConfigTables) {
            path = TStringBuilder() << "/Root/PQ/" << name;
        }
        auto settings = NYdb::NPersQueue::TAlterTopicSettings().PartitionsCount(nParts);
        settings.RetentionPeriod(TDuration::Seconds(lifetimeS));
        auto pqClient = NYdb::NPersQueue::TPersQueueClient(*Driver);
        auto res = pqClient.AlterTopic(path, settings);
        if (UseConfigTables) {  // ToDo - legacy
            AlterTopic();
        }
        res.Wait();
        Cerr << "Alter topic (" << path << ") response: " << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << Endl;
    }

    void AlterTopic(
        const TString& name,
        ui32 nParts,
        ui32 cacheSize = 0,
        ui64 lifetimeS = 86400,
        bool fillPartitionConfig = false,
        std::optional<NKikimrPQ::TMirrorPartitionConfig> mirrorFrom = {}
    ) {
        Y_ABORT_UNLESS(name.StartsWith("rt3."));
        TRequestAlterPQ requestDescr(name, nParts, cacheSize, lifetimeS, fillPartitionConfig, mirrorFrom);
        THolder<NMsgBusProxy::TBusPersQueue> alterRequest = requestDescr.GetRequest();

        ui32 prevVersion = GetTopicVersionFromMetadata(name);
        while (prevVersion == 0) {
            Sleep(TDuration::MilliSeconds(500));

            prevVersion = GetTopicVersionFromMetadata(name);
        }
        CallPersQueueGRPC(alterRequest->Record);
        Cerr << "Alter got " << prevVersion << "\n";

        const TInstant start = TInstant::Now();
        AlterTopic();
        auto ver = GetTopicVersionFromMetadata(name, cacheSize);
        while (ver != prevVersion + 1) {
            Cerr << "Alter1 got " << ver << "\n";

            Sleep(TDuration::MilliSeconds(500));
            ver = GetTopicVersionFromMetadata(name, cacheSize);
            UNIT_ASSERT(TInstant::Now() - start < ::DEFAULT_DISPATCH_TIMEOUT);
        }
        auto ver2 = GetTopicVersionFromPath(name);
        while (ver2 != prevVersion + 1) {
            Cerr << "Alter2 got " << ver << "\n";

            Sleep(TDuration::MilliSeconds(500));
            ver2 = GetTopicVersionFromPath(name);

            UNIT_ASSERT(TInstant::Now() - start < ::DEFAULT_DISPATCH_TIMEOUT);
        }

    }

    NYdb::TStatus DropTopic(const TString& path) {
        auto pqClient = NYdb::NPersQueue::TPersQueueClient(*Driver);
        Cerr << "Drop topic: " << path << Endl;
        auto res = pqClient.DropTopic(path).GetValueSync();
        UNIT_ASSERT(res.IsSuccess());
        return res;
    }

    void DeleteTopic2(
            const TString& name, NPersQueue::NErrorCode::EErrorCode expectedStatus = NPersQueue::NErrorCode::OK,
            bool waitForTopicDeletion = true
    ) {

        Y_ABORT_UNLESS(name.StartsWith("rt3."));
        THolder<NMsgBusProxy::TBusPersQueue> deleteRequest = TRequestDeletePQ{name}.GetRequest();

        CallPersQueueGRPC(deleteRequest->Record);

        // wait for drop completion
        if (expectedStatus == NPersQueue::NErrorCode::OK) {
            ui32 i = 0;
            for (; i < 500; ++i) {
                TAutoPtr<NMsgBusProxy::TBusResponse> r = TryDropPersQueueGroup("/Root/PQ", name);
                UNIT_ASSERT(r);
                if (r->Record.GetSchemeStatus() == NKikimrScheme::StatusPathDoesNotExist) {
                    break;
                }
                Sleep(TDuration::MilliSeconds(50));
            }
            UNIT_ASSERT_C(i < 500, "Drop is taking too long"); //25 seconds
        }
        RemoveTopic(name);
        const TInstant start = TInstant::Now();
        while (waitForTopicDeletion && !IsTopicDeleted(name)) {
            Sleep(TDuration::MilliSeconds(50));
            UNIT_ASSERT(TInstant::Now() - start < ::DEFAULT_DISPATCH_TIMEOUT);
        }
    }

    TString GetOwnership(const TRequestGetOwnership& getOwnership, NMsgBusProxy::EResponseStatus expectedStatus = NMsgBusProxy::MSTATUS_OK) {
        auto response = CallPersQueueGRPC(getOwnership.GetRequest()->Record);

        if (expectedStatus == NMsgBusProxy::MSTATUS_OK) {
            UNIT_ASSERT_VALUES_EQUAL_C((ui32)response.GetErrorCode(), (ui32)NPersQueue::NErrorCode::OK, "write failure");
            return response.GetPartitionResponse().GetCmdGetOwnershipResult().GetOwnerCookie();
        }
        return "";
    }

    void ChooseProxy() {
        NKikimrClient::TChooseProxyRequest request;
        NKikimrClient::TResponse response;

        Cerr << "ChooseProxy request to server " << Client->GetConfig().Ip << ":" << Client->GetConfig().Port << "\n"
                << PrintToString(request) << Endl;

        grpc::ClientContext context;
        auto status = Stub->ChooseProxy(&context, request, &response);

        Cerr << "ChooseProxy response:\n" << PrintToString(response) << Endl;

        UNIT_ASSERT_C(status.ok(), status.error_message());

        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)response.GetStatus(), NMsgBusProxy::MSTATUS_OK, "proxy failure");
    }


    void WriteToPQ(
            const TRequestWritePQ& writeRequest, const TString& data,
            const TString& ticket = "",
            NMsgBusProxy::EResponseStatus expectedStatus = NMsgBusProxy::MSTATUS_OK,
            NMsgBusProxy::EResponseStatus expectedOwnerStatus = NMsgBusProxy::MSTATUS_OK
    ) {

        TString cookie = GetOwnership({writeRequest.Topic, writeRequest.Partition}, expectedOwnerStatus);

        THolder<NMsgBusProxy::TBusPersQueue> request = writeRequest.GetRequest(data, cookie);
        if (!ticket.empty())
            request.Get()->Record.SetTicket(ticket);

        auto response = CallPersQueueGRPC(request->Record);

        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)response.GetStatus(), expectedStatus,
                                   "proxy failure");
        if (expectedStatus == NMsgBusProxy::MSTATUS_OK) {
            UNIT_ASSERT_VALUES_EQUAL_C((ui32)response.GetErrorCode(), (ui32)NPersQueue::NErrorCode::OK,
                                       "write failure");
        }
    }

    void WriteToPQ(const TString& topic, ui32 partition, const TString& sourceId, const ui64 seqNo, const TString& data,
                const TString& ticket = "",
                NMsgBusProxy::EResponseStatus expectedStatus = NMsgBusProxy::MSTATUS_OK,
                NMsgBusProxy::EResponseStatus expectedOwnerStatus = NMsgBusProxy::MSTATUS_OK) {
        WriteToPQ({topic, partition, sourceId, seqNo}, data, ticket, expectedStatus, expectedOwnerStatus);
    }

    struct TReadDebugInfo {
        ui32 BlobsFromDisk = 0;
        ui32 BlobsFromCache = 0;
        TVector<TString> Values;
    };

    TReadDebugInfo ReadFromPQ(
            const TRequestReadPQ& readRequest, ui32 readCount,
            const TString& ticket = "",
            NMsgBusProxy::EResponseStatus expectedStatus = NMsgBusProxy::MSTATUS_OK,
            NPersQueue::NErrorCode::EErrorCode expectedError = NPersQueue::NErrorCode::OK
    ) {
        THolder<NMsgBusProxy::TBusPersQueue> request = readRequest.GetRequest();
        if (!ticket.empty()) {
            request.Get()->Record.SetTicket(ticket);
        }

        auto response = CallPersQueueGRPC(request->Record);

        auto status = response.GetStatus();
        auto errorCode = response.GetErrorCode();
        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)status, expectedStatus, response.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)errorCode, (ui32)expectedError, response.GetErrorReason());

        if (expectedStatus == NMsgBusProxy::MSTATUS_OK) {
            UNIT_ASSERT(response.GetPartitionResponse().HasCmdReadResult());
            if (readCount > 0) UNIT_ASSERT_VALUES_EQUAL(response.GetPartitionResponse().GetCmdReadResult().ResultSize(), readCount);
        }

        TReadDebugInfo info;
        auto result = response.GetPartitionResponse().GetCmdReadResult();
        if (result.HasBlobsFromDisk())
            info.BlobsFromDisk = result.GetBlobsFromDisk();
        if (result.HasBlobsFromCache())
            info.BlobsFromCache = result.GetBlobsFromCache();

        for (ui32 i = 0; i < result.ResultSize(); ++i) {
            auto r = result.GetResult(i);
            if (r.HasData())
                info.Values.push_back(r.GetData());
        }
        return info;
    }


    TReadDebugInfo ReadFromPQ(const TString& topic, ui32 partition, ui64 startOffset, ui32 count, ui32 readCount, const TString& ticket = "") {
        return ReadFromPQ({topic, partition, startOffset, count, "user", 0}, readCount, ticket);
    }

    void SetClientOffsetPQ(
            const TRequestSetClientOffsetPQ& cmdRequest, const TString& ticket = "",
            NMsgBusProxy::EResponseStatus expectedStatus = NMsgBusProxy::MSTATUS_OK,
            NPersQueue::NErrorCode::EErrorCode expectedError = NPersQueue::NErrorCode::OK
    ) {
        THolder<NMsgBusProxy::TBusPersQueue> request = cmdRequest.GetRequest();
        if (!ticket.empty()) {
            request.Get()->Record.SetTicket(ticket);
        }

        auto response = CallPersQueueGRPC(request->Record);

        auto status = response.GetStatus();
        auto errorCode = response.GetErrorCode();
        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)status, expectedStatus, response.GetErrorReason());
        UNIT_ASSERT_VALUES_EQUAL_C((ui32)errorCode, (ui32)expectedError, response.GetErrorReason());
    }

    void SetClientOffsetPQ(const TString& topic, ui32 partition, ui64 offset, const TString& ticket = "",
                    NMsgBusProxy::EResponseStatus expectedStatus = NMsgBusProxy::MSTATUS_OK,
                    NPersQueue::NErrorCode::EErrorCode expectedError = NPersQueue::NErrorCode::OK) {
        return SetClientOffsetPQ({topic, partition, offset, "user"}, ticket, expectedStatus, expectedError);
    }

    void FetchRequestPQ(const TVector<FetchPartInfo>& fetchParts, ui32 maxBytes, ui32 waitMs) {
        THolder<NMsgBusProxy::TBusPersQueue> request = TFetchRequestPQ().GetRequest(fetchParts, maxBytes, waitMs);
        CallPersQueueGRPC(request->Record);
    }

    void GetPartOffset(const TVector<std::pair<TString, TVector<ui32>>>& topicsAndParts, ui32 resCount, ui32 hasClientOffset, bool ok) {
        THolder<NMsgBusProxy::TBusPersQueue> request = TRequestGetPartOffsets().GetRequest(topicsAndParts);

        auto response = CallPersQueueGRPC(request->Record);

        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)response.GetStatus(), ok ? NMsgBusProxy::MSTATUS_OK : NMsgBusProxy::MSTATUS_ERROR,
                                   "proxy failure");

        if (!ok)
            return;

        auto res = response.GetMetaResponse().GetCmdGetPartitionOffsetsResult();
        ui32 count = 0;
        ui32 clientOffsetCount = 0;
        for (ui32 i = 0; i < res.TopicResultSize(); ++i) {
            auto t = res.GetTopicResult(i);
            count += t.PartitionResultSize();
            for (ui32 j = 0; j < t.PartitionResultSize(); ++j) {
                if (t.GetPartitionResult(j).HasClientOffset() && t.GetPartitionResult(j).GetClientOffset() > 0)
                    ++clientOffsetCount;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(count, resCount);
        UNIT_ASSERT_VALUES_EQUAL(clientOffsetCount, hasClientOffset);
    }

    NKikimrClient::TResponse GetClientInfo(const TVector<TString>& topics, const TString& user, bool ok, const TVector<TString>& badTopics = {}) {
        THolder<NMsgBusProxy::TBusPersQueue> request = TRequestGetClientInfo().GetRequest(topics, user);
        Cerr << "Request: " << request->Record << Endl;

        auto response = CallPersQueueGRPC(request->Record);

        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)response.GetStatus(), ok ? NMsgBusProxy::MSTATUS_OK : NMsgBusProxy::MSTATUS_ERROR,
                                   "proxy failure");
        THashSet<TString> good;
        THashSet<TString> bad;
        for (auto& t : badTopics) {
            bad.insert(t);
        }
        for (auto& t : topics) {
            if (!bad.contains(t)) {
                good.insert(t);
            }
        }
        for (auto& tt : response.GetMetaResponse().GetCmdGetReadSessionsInfoResult().GetTopicResult()) {
            const auto& topic = tt.GetTopic();
            if (bad.contains(topic)) {
                UNIT_ASSERT(tt.GetErrorCode() != (ui32)NPersQueue::NErrorCode::OK);
            } else {
                UNIT_ASSERT(tt.GetErrorCode() == (ui32)NPersQueue::NErrorCode::OK);
            }
        }
        return response;
    }


    void GetPartStatus(const TVector<std::pair<TString, TVector<ui32>>>& topicsAndParts, ui32 resCount, bool ok) {
        THolder<NMsgBusProxy::TBusPersQueue> request = TRequestGetPartStatus().GetRequest(topicsAndParts);

        auto response = CallPersQueueGRPC(request->Record);

        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)response.GetStatus(), ok ? NMsgBusProxy::MSTATUS_OK : NMsgBusProxy::MSTATUS_ERROR,
                                   "proxy failure");
        if (!ok)
            return;

        auto res = response.GetMetaResponse().GetCmdGetPartitionStatusResult();
        ui32 count = 0;
        for (ui32 i = 0; i < res.TopicResultSize(); ++i) {
            auto t = res.GetTopicResult(i);
            count += t.PartitionResultSize();
        }
        UNIT_ASSERT_VALUES_EQUAL(count, resCount);
    }

    TVector<ui32> GetPartLocation(const TVector<std::pair<TString, TVector<ui32>>>& topicsAndParts, ui32 resCount, bool ok) {
        bool doRetry = true;

        TVector<ui32> nodeIds;
        const TInstant start = TInstant::Now();
        while (doRetry) {
            doRetry = false;
            nodeIds.clear();

            THolder<NMsgBusProxy::TBusPersQueue> request = TRequestGetPartLocations().GetRequest(topicsAndParts);

            auto response = CallPersQueueGRPC(request->Record);

            if (response.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING) {
                doRetry = true;
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)response.GetStatus(), ok ? NMsgBusProxy::MSTATUS_OK : NMsgBusProxy::MSTATUS_ERROR,
                                       "proxy failure");

            if (!ok)
                return {};

            auto res = response.GetMetaResponse().GetCmdGetPartitionLocationsResult();

            for (ui32 i = 0; i < res.TopicResultSize(); ++i) {
                auto t = res.GetTopicResult(i);
                if (t.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING)
                    doRetry = true;
                for (ui32 pi = 0; pi < t.PartitionLocationSize(); ++pi) {
                    if (!t.GetPartitionLocation(pi).HasHostId()) {
                        // Retry until the requested partiotions are successfully resolved
                        doRetry = true;
                    } else {
                        nodeIds.push_back(t.GetPartitionLocation(pi).GetHostId());
                    }
                }
            }
            UNIT_ASSERT(TInstant::Now() - start < ::DEFAULT_DISPATCH_TIMEOUT);
            if (doRetry) {
                Sleep(TDuration::MilliSeconds(50));
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(nodeIds.size(), resCount);
        return nodeIds;
    }

     NKikimrClient::TPersQueueMetaResponse::TCmdGetTopicMetadataResult DescribeTopic(const TVector<TString>& topics, bool error = false) {
        THolder<NMsgBusProxy::TBusPersQueue> request = TRequestDescribePQ().GetRequest(topics);

        TAutoPtr<NBus::TBusMessage> reply;
        auto response = CallPersQueueGRPC(request->Record);

        if ((NMsgBusProxy::EResponseStatus)response.GetStatus() != NMsgBusProxy::MSTATUS_OK) {
            UNIT_ASSERT(error);
            return {};
        }

        UNIT_ASSERT_VALUES_EQUAL_C((NMsgBusProxy::EResponseStatus)response.GetStatus(), NMsgBusProxy::MSTATUS_OK,
                                   "proxy failure");

        auto res = response.GetMetaResponse().GetCmdGetTopicMetadataResult();

        UNIT_ASSERT(topics.size() <= res.TopicInfoSize());
        for (ui32 i = 0; i < res.TopicInfoSize(); ++i) {
            const auto& topicInfo = res.GetTopicInfo(i);
            if (error) {
                UNIT_ASSERT(topicInfo.GetErrorCode() == NPersQueue::NErrorCode::INITIALIZING);
            } else {
                UNIT_ASSERT(topicInfo.GetNumPartitions() > 0 || topicInfo.GetErrorCode() != (ui32)NPersQueue::NErrorCode::OK);
                UNIT_ASSERT(topicInfo.GetConfig().HasPartitionConfig() || topicInfo.GetErrorCode() != (ui32)NPersQueue::NErrorCode::OK);
            }
            ui32 j = 0;
            for (; j < topics.size() && topics[j] != topicInfo.GetTopic(); ++j);
            UNIT_ASSERT(j == 0 || j != topics.size());
        }
        return res;
    }

    void TestCase(const TVector<std::pair<TString, TVector<ui32>>>& topicsAndParts, ui32 resCount, ui32 hasClientOffset, bool ok) {
        GetPartOffset(topicsAndParts, resCount, hasClientOffset, ok);
        GetPartLocation(topicsAndParts, resCount, ok);
        GetPartStatus(topicsAndParts, resCount, ok);
    }

private:
    static TString GetAlterTopicsVersionQuery() {
        return "UPSERT INTO `/Root/PQ/Config/V2/Versions` (name, version) VALUES (\"Topics\", $version);";
    }

public:
    void AddTopic(const TString& topic, const TMaybe<TString>& dc = Nothing()) {
        Cerr << "AddTopic: " << topic << Endl;
        return AddOrRemoveTopic(topic, true, dc);
    }

    void RemoveTopic(const TString& topic) {
        Cerr << "RemoveTopic: " << topic << Endl;
        return AddOrRemoveTopic(topic, false);
    }

    void AddOrRemoveTopic(const TString& topic, bool add, const TMaybe<TString>& dc = Nothing()) {
        TStringBuilder query;
        query << "DECLARE $version as Int64; DECLARE $path AS Utf8; DECLARE $cluster as Utf8; ";
        if (add) {
            query << "UPSERT INTO `/Root/PQ/Config/V2/Topics` (path, dc) VALUES ($path, $cluster); ";
        } else {
            query << "DELETE FROM `/Root/PQ/Config/V2/Topics` WHERE path = $path AND dc = $cluster; ";
        }
        TString cluster = dc.GetOrElse(NPersQueue::GetDC(topic));
        query << GetAlterTopicsVersionQuery();
        NYdb::TParamsBuilder builder;
        auto params = builder
                .AddParam("$version").Int64(++TopicsVersion).Build()
                .AddParam("$path").Utf8(NPersQueue::GetTopicPath(topic)).Build()
                .AddParam("$cluster").Utf8(cluster).Build()
                .Build();

        Cerr << "===Run query:``" << query << "`` with topic = " << NPersQueue::GetTopicPath(topic) << ", dc = " << NPersQueue::GetDC(topic) << Endl;
        RunYqlDataQueryWithParams(query, params);
        Cerr << "===Query complete" << Endl;
    }

    void AlterTopic() {
        NYdb::TParamsBuilder builder;
        auto params = builder
                .AddParam("$version").Int64(++TopicsVersion).Build()
                .Build();
        TStringBuilder query;
        query << "DECLARE $version as Int64; " << GetAlterTopicsVersionQuery();
        RunYqlDataQueryWithParams(query, params);
    }

    struct CreateTopicNoLegacyParams {
        TString Name;
        ui32 PartsCount;
        bool DoWait = true;
        bool CanWrite = true;
        TMaybe<TString> Dc = Nothing();
        TVector<TString> ReadRules = {"user"};
        TMaybe<TString> Account = Nothing();
        bool ExpectFail = false;
        TVector<NYdb::NPersQueue::ECodec> Codecs = NYdb::NPersQueue::GetDefaultCodecs();
    };

    void CreateTopicNoLegacy(const CreateTopicNoLegacyParams& params)
    {
        Cerr << "CreateTopicNoLegacy: " << params.Name << Endl;

        TString path = params.Name;
        if (UseConfigTables && !path.StartsWith("/Root") && !params.Account.Defined()) {
            path = TStringBuilder() << "/Root/PQ/" << params.Name;
        }

        auto pqClient = NYdb::NPersQueue::TPersQueueClient(*Driver);
        auto settings = NYdb::NPersQueue::TCreateTopicSettings().PartitionsCount(params.PartsCount).ClientWriteDisabled(!params.CanWrite);
        settings.FederationAccount(params.Account);
        settings.SupportedCodecs(params.Codecs);
        //settings.MaxPartitionWriteSpeed(50_MB);
        //settings.MaxPartitionWriteBurst(50_MB);
        TVector<NYdb::NPersQueue::TReadRuleSettings> rrSettings;
        for (auto &user : params.ReadRules) {
            rrSettings.push_back({NYdb::NPersQueue::TReadRuleSettings{}.ConsumerName(user)});
        }
        settings.ReadRules(rrSettings);

        Cerr << "Create topic: " << path << Endl;
        auto res = pqClient.CreateTopic(path, settings);
        //ToDo - hack, cannot avoid legacy compat yet as PQv1 still uses RequestProcessor from core/client/server
        if (UseConfigTables && !params.ExpectFail) {
            AddTopic(params.Name, params.Dc);
        }
        if (params.ExpectFail) {
            res.Wait();
            UNIT_ASSERT(!res.GetValue().IsSuccess());
        } else {
            res.Wait();
            Cerr << "Create topic result: " << res.GetValue().IsSuccess() << " " << res.GetValue().GetIssues().ToString() << "\n";
            UNIT_ASSERT(res.GetValue().IsSuccess());
        }
    }
};

} // namespace NPersQueueTests
} // namespace NKikimr
