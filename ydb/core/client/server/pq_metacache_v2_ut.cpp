#include "msgbus_server_pq_metacache.h"

#include <ydb/core/testlib/basics/runtime.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/core/scheme_types/scheme_type_registry.h>
#include <ydb/core/tx/scheme_board/cache.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NMsgBusProxy {
using namespace NPqMetaCacheV2;
using namespace Tests;
using namespace NYdb;
using namespace NYdb::NTable;
using namespace NKikimr::NSchemeCache;
namespace NTests {

class TPqMetaCacheV2Test: public TTestBase {
    void SetUp() override {
        TTestBase::SetUp();
        TPortManager pm{};
        MainServerPort = pm.GetPort(2134);
        GrpcServerPort = pm.GetPort(2135);
        NKikimr::NPersQueueTests::TServerSettings settings = NKikimr::NPersQueueTests::PQSettings(MainServerPort);
        settings.SetDomainName("Root");
        settings.SetDomain(0);
        settings.SetUseRealThreads(true);
        //settings.PQConfig.SetMetaCacheSkipVersionCheck(true);
        Server = MakeHolder<NKikimr::Tests::TServer>(settings);

        Server->EnableGRpc(NGrpc::TServerOptions().SetHost("localhost").SetPort(GrpcServerPort));
        auto* runtime = Server->GetRuntime();
        //runtime->SetLogPriority(NKikimrServices::PERSQUEUE, NActors::NLog::PRI_DEBUG);
//        runtime->SetLogPriority(NKikimrServices::KQP_PROXY, NActors::NLog::PRI_DEBUG);
//        runtime->SetLogPriority(NKikimrServices::KQP_COMPILE_SERVICE, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::PQ_METACACHE, NActors::NLog::PRI_DEBUG);
        runtime->SetLogPriority(NKikimrServices::PQ_WRITE_PROXY, NActors::NLog::PRI_EMERG);
        runtime->SetLogPriority(NKikimrServices::PERSQUEUE_CLUSTER_TRACKER, NActors::NLog::PRI_EMERG);

        Client = std::make_shared<NKikimr::NPersQueueTests::TFlatMsgBusPQClient>(settings, GrpcServerPort);

        Client->InitRootScheme();

        NYdb::TDriverConfig driverCfg;
        driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << GrpcServerPort);
        YdbDriver = std::make_shared<NYdb::TDriver>(driverCfg);
        TableClient.Reset(new NYdb::NTable::TTableClient(*YdbDriver));
        PQClient = std::make_shared<NYdb::NPersQueue::TPersQueueClient>(*YdbDriver);

        Client->MkDir("/Root", "PQ");
        Client->MkDir("/Root/PQ", "Config");
        Client->MkDir("/Root/PQ/Config", "V2");

        auto tableDesc = TTableBuilder()
                .AddNullableColumn("path", EPrimitiveType::Utf8)
                .AddNullableColumn("dc", EPrimitiveType::Utf8)
                .SetPrimaryKeyColumns({"path", "dc"})
                .Build();

        auto session = CheckYdbResult(std::move(TableClient->CreateSession())).GetSession();
        CheckYdbResult(session.CreateTable("/Root/PQ/Config/V2/Topics", std::move(tableDesc)));

        tableDesc = TTableBuilder()
                .AddNullableColumn("name", EPrimitiveType::String)
                .AddNullableColumn("version", EPrimitiveType::Int64)
                .SetPrimaryKeyColumns({"name", "version"})
                .Build();
        CheckYdbResult(session.CreateTable("/Root/PQ/Config/V2/Versions", std::move(tableDesc)));

        EdgeActorId = runtime->AllocateEdgeActor();
        auto config = MakeIntrusive<TSchemeCacheConfig>();
        config->Counters = new NMonitoring::TDynamicCounters;
        config->Roots.emplace_back(settings.Domain, 8716544, "Root");
        auto& domainsInfo = settings.AppConfig.GetDomainsConfig();
        for (const auto& domain : domainsInfo.GetDomain()) {
            config->Roots.emplace_back(domain.GetDomainId(), domain.GetSchemeRoot(), domain.GetName());
        }
        SchemeCacheId = runtime->Register(CreateSchemeBoardSchemeCache(config.Get()));
        MetaCacheId = runtime->Register(
                NPqMetaCacheV2::CreatePQMetaCache(GrpcServerPort, config->Counters, TDuration::MilliSeconds(50))
        );
        runtime->EnableScheduleForActor(SchemeCacheId, true);
        runtime->EnableScheduleForActor(MetaCacheId, true);
    }

    struct TTopicInfo {
        TString Path;
        TString Cluster;
        bool DoCreate;
    };

    void AddTopics(const TVector<TTopicInfo>& topics, bool shiftVersion = true) {
        auto session = TableClient->GetSession().GetValueSync().GetSession();
        auto topicsPrepared = CheckYdbResult(session.PrepareDataQuery(UpsertTopicQuery)).GetQuery();
        TVector<TAsyncStatus> createTopicStatus;
        auto tx = session.BeginTransaction().GetValueSync().GetTransaction();
        auto txControl = TTxControl::Tx(tx);
        for (const auto& topic : topics) {
            auto fullPath = "/Root/PQ/" + ::NPersQueue::BuildFullTopicName(topic.Path, topic.Cluster);
            if (topic.DoCreate) {
                createTopicStatus.push_back(PQClient->CreateTopic(fullPath));
            }
            auto builder = topicsPrepared.GetParamsBuilder();
            {
                auto &param = builder.AddParam("$Path");
                param.Utf8(topic.Path);
                param.Build();
            }
            {
                auto &param = builder.AddParam("$Cluster");
                param.Utf8(topic.Cluster);
                param.Build();
            }
            CheckYdbResult(topicsPrepared.Execute(txControl, builder.Build()));
        }
        if (shiftVersion) {
            auto versionPrepared = CheckYdbResult(session.PrepareDataQuery(UpdateVersionQuery)).GetQuery();
            auto builder = versionPrepared.GetParamsBuilder();
            {
                auto &param = builder.AddParam("$Version");
                param.Int64(++Version);
                param.Build();
            }
            CheckYdbResult(versionPrepared.Execute(txControl, builder.Build()));
        }
        CheckYdbResult(tx.Commit());
        for (auto& status : createTopicStatus) {
            CheckYdbResult(std::move(status));
        }
    }

    template<class TEvType>
    THolder<TEvType> DoMetaCacheRequest(const TVector<TTopicInfo>& topicList = {}) {
        IEventBase* ev;
        if (topicList.empty()) {
            ev = new TEvPqNewMetaCache::TEvDescribeAllTopicsRequest();
        } else {
            TVector<TString> topicNames;
            for (const auto &topic : topicList) {
                topicNames.emplace_back(TString("/Root/PQ/") + ::NPersQueue::BuildFullTopicName(topic.Path, topic.Cluster));
            }
            ev = new TEvPqNewMetaCache::TEvDescribeTopicsRequest(topicNames);
        }
        auto handle = new IEventHandle(MetaCacheId, EdgeActorId, ev);
        Server->GetRuntime()->Send(handle);
        auto response = Server->GetRuntime()->GrabEdgeEvent<TEvType>();
        return std::move(response);
    }

    THolder<TEvPqNewMetaCache::TEvDescribeTopicsResponse> DescribeTopics(const TVector<TTopicInfo>& topicList) {
        return DoMetaCacheRequest<TEvPqNewMetaCache::TEvDescribeTopicsResponse>(topicList);
    }

    THolder<TEvPqNewMetaCache::TEvDescribeAllTopicsResponse> DescribeAllTopics() {
        return DoMetaCacheRequest<TEvPqNewMetaCache::TEvDescribeAllTopicsResponse>();
    }

    void CheckTopicInfo(const TVector<TTopicInfo>& expected, std::shared_ptr<NSchemeCache::TSchemeCacheNavigate> result) {
        ui64 i = 0;
        Cerr << "=== Got cache navigate response: \n" << result->ToString(NKikimr::NScheme::TTypeRegistry()) << Endl;
        Cerr << "=== Expect to have " << expected.size() << " records, got: " << result->ResultSet.size() << " records" << Endl;
        UNIT_ASSERT_VALUES_EQUAL_C(expected.size(), result->ResultSet.size(), "Result size mismatch");
        for (const auto& topic : expected) {
            Cerr << "===Check topic: " << topic.Path << ":" << topic.Cluster << Endl;
            auto& response = result->ResultSet[i++];
            if (topic.DoCreate) {
                UNIT_ASSERT(response.Status == TSchemeCacheNavigate::EStatus::Ok);
                UNIT_ASSERT(response.PQGroupInfo);
                UNIT_ASSERT(response.PQGroupInfo->Kind == NSchemeCache::TSchemeCacheNavigate::KindTopic);
            } else {
                UNIT_ASSERT_VALUES_EQUAL(response.Status, TSchemeCacheNavigate::EStatus::PathErrorUnknown);
            }
        }
    }

    void WaitForVersion(ui64 version, TDuration timeout = TDuration::Seconds(5)) {
        ui64 currentVersion = 0;
        auto endTime = TInstant::Now() + timeout;
        auto* runtime = Server->GetRuntime();
        while (endTime > TInstant::Now()) {
            auto handle = new IEventHandle(MetaCacheId, EdgeActorId, new TEvPqNewMetaCache::TEvGetVersionRequest());
            runtime->Send(handle);
            auto response = runtime->GrabEdgeEvent<TEvPqNewMetaCache::TEvGetVersionResponse>();
            currentVersion = response->TopicsVersion;
            if (currentVersion >= version) {
                Cerr << "=== Got current topics version: " << currentVersion << Endl;
                return;
            }
            Sleep(TDuration::MilliSeconds(100));
        }
        UNIT_FAIL("Wait for topics version timed out");
    }

    TActorId MakeEdgeTargetedMetaCache() {
        auto anotherMetaCacheId = Server->GetRuntime()->Register(
                NPqMetaCacheV2::CreatePQMetaCache(TableClient, EdgeActorId)

        );
        return anotherMetaCacheId;
    }

    void TestDescribeTopics() {
        auto topics = TVector<TTopicInfo>({
                                                  {"topic1", "man", true},
                                                  {"topic2", "man", true},
                                                  {"topic3", "man", false}
                                          });
        AddTopics(topics);
        auto ev = DescribeTopics(topics);
        CheckTopicInfo(topics, ev->Result);
        UNIT_ASSERT(ev);
    }

    void TestDescribeManyTopics() {
        auto topics = TVector<TTopicInfo>({
                                                  {"topic1", "man", true},
                                                  {"topic2", "man", false},
                                                  {"topic3", "man", false}
                                          });

        AddTopics(topics);

        Cerr << "===Wait base response\n";
        auto baseResponse = DescribeTopics(topics);
        Cerr << "===Got base response\n";
        auto baseEntry = baseResponse->Result->ResultSet[0];

        TVector<TTopicInfo> lotOfTopics;
        for (auto i = 0u; i < 10000u; i++) {
            lotOfTopics.emplace_back(TTopicInfo{TString("topic0") + ToString(i), "man", false});
        }
        AddTopics(lotOfTopics);
        ui64 totalTopics = topics.size() + lotOfTopics.size();
        auto secondMetaCache = MakeEdgeTargetedMetaCache();
        Cerr << "===Registered secondary meta cache: " << secondMetaCache.ToString() << Endl;
        auto* runtime = Server->GetRuntime();

        auto sendRequest = [&]() {
            auto* ev = new TEvPqNewMetaCache::TEvDescribeAllTopicsRequest();
            auto handle = new IEventHandle(secondMetaCache, EdgeActorId, ev);
            runtime->Send(handle);
        };
        Cerr << "===Send first request\n";
        sendRequest();

        auto scRequest = runtime->GrabEdgeEvent<TEvTxProxySchemeCache::TEvNavigateKeySet>();
        Cerr << "===Got SC request\n";
        auto& entries = scRequest->Request->ResultSet;
        UNIT_ASSERT_VALUES_EQUAL(entries.size(), totalTopics);
        for (auto i = 0u; i < entries.size(); i++) {
            auto initialPath = entries[i].Path;
            entries[i] = baseEntry;
            entries[i].Path = initialPath;
        }
        auto* scResponse = new TEvTxProxySchemeCache::TEvNavigateKeySetResult(scRequest->Request);

        auto handle = new IEventHandle(secondMetaCache, EdgeActorId, scResponse);
        Cerr << "===Send fake SC response\n";
        runtime->Send(handle);

        Cerr << "===Wait secondary metacache response\n";
        auto response = runtime->GrabEdgeEvent<TEvPqNewMetaCache::TEvDescribeAllTopicsResponse>();
        Cerr << "===Got secondary metacache response\n";

        // Preparation done, now can send many requests
        Cerr << "===Started pushing requests at: " << TInstant::Now() << Endl;
        for (auto i = 0u; i < 1000; i++) {
            sendRequest();
        }
        Cerr << "===Done pushing requests at: " << TInstant::Now() << Endl;
        for (auto i = 0u; i < 1000; i++) {
            response = runtime->GrabEdgeEvent<TEvPqNewMetaCache::TEvDescribeAllTopicsResponse>();
            if (!i) {
                Cerr << "===Done gathering first response at: " << TInstant::Now() << Endl;
            }
        }
        Cerr << "===Done gathering responses at: " << TInstant::Now() << Endl;
    }

    void TestDescribeAllTopics() {
        auto topics = TVector<TTopicInfo>({
              {"topic1", "man", true},
              {"topic2", "man", true},
              {"topic3", "man", false}
        });
        AddTopics(topics);
        auto ev = DescribeAllTopics();
        CheckTopicInfo(topics, ev->Result);
        UNIT_ASSERT(ev);
    }

    void TestTopicsUpdate() {
        auto topics = TVector<TTopicInfo>({
                {"topic1", "man", true},
                {"topic2", "man", true},
                {"topic3", "man", false}
          });
        AddTopics(topics);
        CheckTopicInfo(topics, DescribeTopics(topics)->Result);
        WaitForVersion(Version);

        TTopicInfo topic{"topic1", "sas", true};
        AddTopics({topic}, false);

        CheckTopicInfo(topics, DescribeAllTopics()->Result);

        AddTopics({}, true);
        WaitForVersion(Version);
        topics.insert(topics.end() - 1, topic);
        CheckTopicInfo(topics, DescribeAllTopics()->Result);
    }


    template<class T>
    T CheckYdbResult(NThreading::TFuture<T>&& asyncResult) {
        auto res = asyncResult.GetValueSync();
        UNIT_ASSERT_C(res.IsSuccess(), res.GetIssues().ToString());
        return res;
    }

    TString GetUpsertTopicQuery(const TTopicInfo& info) {
        return TStringBuilder()
                << "--!syntax_v1\n"
                << "UPSERT INTO `/Root/PQ/Config/V2/Topics` "
                << "(path, dc) VALUES (" << info.Path << ", " << info.Cluster << ");";
    }

    UNIT_TEST_SUITE(TPqMetaCacheV2Test)
        UNIT_TEST(TestDescribeTopics)
        UNIT_TEST(TestDescribeAllTopics)
        UNIT_TEST(TestTopicsUpdate)
    UNIT_TEST_SUITE_END();
private:
    ui16 MainServerPort;
    ui16 GrpcServerPort;
    THolder<Tests::TServer> Server;

    TActorId EdgeActorId;
    TActorId SchemeCacheId;
    TActorId MetaCacheId;
    ui64 Version = 1;
    std::shared_ptr<NKikimr::NPersQueueTests::TFlatMsgBusPQClient> Client;

    std::shared_ptr<NYdb::TDriver> YdbDriver;
    TAtomicSharedPtr<NYdb::NTable::TTableClient> TableClient;
    std::shared_ptr<NYdb::NPersQueue::TPersQueueClient> PQClient;
    TString UpsertTopicQuery = TStringBuilder()
            << "--!syntax_v1\n"
            << "DECLARE $Path as Utf8; DECLARE $Cluster as Utf8; "
            << "UPSERT INTO `/Root/PQ/Config/V2/Topics`"
            << "(path, dc) VALUES ($Path, $Cluster);";
    TString UpdateVersionQuery = TStringBuilder()
            << "--!syntax_v1\n"
            << "DECLARE $Version as Int64; "
            << "UPSERT INTO `/Root/PQ/Config/V2/Versions`"
            << "(name, version) VALUES ('Topics', $Version);";


};
UNIT_TEST_SUITE_REGISTRATION(TPqMetaCacheV2Test);

}// namespace NTests
} // namespace NKikimr::NMsgBusProxy
