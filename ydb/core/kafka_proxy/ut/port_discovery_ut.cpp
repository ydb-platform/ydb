#include <library/cpp/testing/unittest/registar.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>

#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/core/discovery/discovery.h>
#include <ydb/core/grpc_services/grpc_endpoint.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/kafka_proxy/kafka_messages.h>
#include <ydb/core/kafka_proxy/actors/actors.h>
#include <ydb/core/kafka_proxy/actors/kafka_metadata_actor.h>
#include <ydb/core/discovery/discovery.h>


using namespace NKikimr;

auto UnpackDiscoveryData(const TString& data) {
    Ydb::Discovery::ListEndpointsResponse leResponse;
    Ydb::Discovery::ListEndpointsResult leResult;
    auto ok = leResponse.ParseFromString(data);
    UNIT_ASSERT(ok);
    ok = leResponse.operation().result().UnpackTo(&leResult);
    UNIT_ASSERT(ok);
    return leResult;
}

class TFakeDiscoveryCache: public TActorBootstrapped<TFakeDiscoveryCache> {
    std::shared_ptr<NDiscovery::TCachedMessageData> CachedMessage;

public:
    TFakeDiscoveryCache(const Ydb::Discovery::ListEndpointsResult& leResult, bool triggerError)
    {
        if (!triggerError) {
            Ydb::Discovery::ListEndpointsResponse response;
            TString out;
            auto deferred = response.mutable_operation();
            deferred->set_ready(true);
            deferred->set_status(Ydb::StatusIds::SUCCESS);

            auto data = deferred->mutable_result();
            data->PackFrom(leResult);

            Y_PROTOBUF_SUPPRESS_NODISCARD response.SerializeToString(&out);

            TMap<TActorId, TEvStateStorage::TBoardInfoEntry> infoEntries;
            infoEntries.insert(std::make_pair(SelfId(), TEvStateStorage::TBoardInfoEntry("/Root")));
            CachedMessage.reset(new NDiscovery::TCachedMessageData(out, "b", std::move(infoEntries)));

        } else {
            CachedMessage.reset(new NDiscovery::TCachedMessageData("", "", {}));
        }
    }

    void Bootstrap() {
        Become(&TFakeDiscoveryCache::StateWork);
    }

    STATEFN(StateWork) {
        Handle(ev);
    }
    void Handle(TAutoPtr<NActors::IEventHandle>& ev) {
        Cerr << "Fake discovery cache: handle request\n";
        Send(ev->Sender, new TEvDiscovery::TEvDiscoveryData(CachedMessage), 0, ev->Cookie);
    }
};

namespace NKafka::NTests {
    Y_UNIT_TEST_SUITE(DiscoveryIsNotBroken) {
        void CheckEndpointsInDiscovery(bool withSsl, bool expectKafkaEndpoints) {
            auto pm = MakeSimpleShared<TPortManager>();
            ui16 kafkaPort = pm->GetPort();
            auto serverSettings = NPersQueueTests::PQSettings(0).SetDomainName("Root").SetNodeCount(1);
            serverSettings.AppConfig->MutableKafkaProxyConfig()->SetEnableKafkaProxy(true);
            serverSettings.AppConfig->MutableKafkaProxyConfig()->SetListeningPort(kafkaPort);
            if (withSsl) {
                serverSettings.AppConfig->MutableKafkaProxyConfig()->SetSslCertificate("12345");
            }
            NPersQueue::TTestServer server(serverSettings, true, {}, NActors::NLog::PRI_INFO, pm);
            auto port = server.GrpcPort;
            Cerr << "Run with port = " << port << ", kafka port = " << kafkaPort << Endl;
            auto* runtime = server.GetRuntime();
            auto edge = runtime->AllocateEdgeActor();

            TActorId discoveryCacheActorID;
            if (expectKafkaEndpoints) {
                discoveryCacheActorID = runtime->Register(CreateDiscoveryCache(NGRpcService::KafkaEndpointId));
            } else {
                discoveryCacheActorID = runtime->Register(CreateDiscoveryCache());
            }
            auto discoverer = runtime->Register(CreateDiscoverer(&MakeEndpointsBoardPath, "/Root", edge, discoveryCacheActorID));
            Y_UNUSED(discoverer);
            TAutoPtr<IEventHandle> handle;
            auto* ev = runtime->GrabEdgeEvent<TEvDiscovery::TEvDiscoveryData>(handle);
            UNIT_ASSERT(ev);
            auto discoveryData = UnpackDiscoveryData(ev->CachedMessageData->CachedMessage);
            auto discoverySslData = UnpackDiscoveryData(ev->CachedMessageData->CachedMessageSsl);

            auto checkEnpoints = [&] (ui32 port, ui32 sslPort) {
                if (port) {
                    UNIT_ASSERT_VALUES_EQUAL(discoveryData.endpoints_size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(discoveryData.endpoints(0).port(), port);
                    UNIT_ASSERT_VALUES_EQUAL(discoverySslData.endpoints_size(), 0);
                }
                if (sslPort) {
                    UNIT_ASSERT_VALUES_EQUAL(discoverySslData.endpoints_size(), 1);
                    UNIT_ASSERT_VALUES_EQUAL(discoverySslData.endpoints(0).port(), sslPort);
                    UNIT_ASSERT_VALUES_EQUAL(discoveryData.endpoints_size(), 0);
                }
            };
            if (expectKafkaEndpoints) {
                if (withSsl) {
                    checkEnpoints(0, kafkaPort);
                } else {
                    checkEnpoints(kafkaPort, 0);
                }
            } else {
                checkEnpoints(port, 0);
            }
        }

        Y_UNIT_TEST(NoKafkaEndpointInDiscovery) {
            CheckEndpointsInDiscovery(false, false);
        }

        Y_UNIT_TEST(NoKafkaSslEndpointInDiscovery) {
            CheckEndpointsInDiscovery(true, false);
        }

        Y_UNIT_TEST(HaveKafkaEndpointInDiscovery) {
            CheckEndpointsInDiscovery(false, true);
        }
        Y_UNIT_TEST(HaveKafkaSslEndpointInDiscovery) {
            CheckEndpointsInDiscovery(true, true);
        }
    }

    Y_UNIT_TEST_SUITE(PublishKafkaEndpoints) {
        Y_UNIT_TEST(HaveEndpointInLookup) {
            auto pm = MakeSimpleShared<TPortManager>();
            ui16 kafkaPort = pm->GetPort();
            auto serverSettings = NPersQueueTests::PQSettings(0).SetDomainName("Root").SetNodeCount(1);
            serverSettings.AppConfig->MutableKafkaProxyConfig()->SetEnableKafkaProxy(true);
            serverSettings.AppConfig->MutableKafkaProxyConfig()->SetListeningPort(kafkaPort);
            NPersQueue::TTestServer server(serverSettings, true, {}, NActors::NLog::PRI_INFO, pm);

            auto* runtime = server.GetRuntime();
            auto edge = runtime->AllocateEdgeActor();
            runtime->Register(CreateBoardLookupActor(MakeEndpointsBoardPath("/Root"), edge, EBoardLookupMode::Second));
            TAutoPtr<IEventHandle> handle;
            auto* ev = runtime->GrabEdgeEvent<TEvStateStorage::TEvBoardInfo>(handle);
            UNIT_ASSERT(ev);
            Cerr << "ev for path: " << ev->Path << ", is unknown: " << (ev->Status == TEvStateStorage::TEvBoardInfo::EStatus::Unknown)
                 << ", is unavalable: " << (ev->Status == TEvStateStorage::TEvBoardInfo::EStatus::NotAvailable) << Endl;
            UNIT_ASSERT(ev->Status == TEvStateStorage::TEvBoardInfo::EStatus::Ok);
            UNIT_ASSERT_VALUES_EQUAL(ev->InfoEntries.size(), 2);
            bool hasKafkaPort = false;
            for (const auto& [k, v] : ev->InfoEntries) {
                NKikimrStateStorage::TEndpointBoardEntry entry;
                UNIT_ASSERT(entry.ParseFromString(v.Payload));
                Cerr << "Got entry, actor: " << k.ToString() << ", entry: " << entry.DebugString() << Endl;
                if (entry.GetPort() == kafkaPort) {
                    UNIT_ASSERT_STRINGS_EQUAL(entry.GetEndpointId(), NGRpcService::KafkaEndpointId);
                    hasKafkaPort = true;
                }
            }
            UNIT_ASSERT(hasKafkaPort);
        }
        struct TMetarequestTestParams {
            NPersQueue::TTestServer Server;
            ui64 KafkaPort;
            NKikimrConfig::TKafkaProxyConfig KafkaConfig;
            TString FullTopicName;
        };

        TMetarequestTestParams SetupServer(const TString shortTopicName) {
            TStringBuilder fullTopicName;
            fullTopicName << "rt3.dc1--" << shortTopicName;
            auto pm = MakeSimpleShared<TPortManager>();
            ui16 kafkaPort = pm->GetPort();
            auto serverSettings = NPersQueueTests::PQSettings(0).SetDomainName("Root").SetNodeCount(1);
            serverSettings.AppConfig->MutableKafkaProxyConfig()->SetEnableKafkaProxy(true);

            serverSettings.AppConfig->MutableKafkaProxyConfig()->SetListeningPort(kafkaPort);
            NPersQueue::TTestServer server(serverSettings, true, {}, NActors::NLog::PRI_INFO, pm);

            server.AnnoyingClient->CreateTopic(fullTopicName, 1);
            server.WaitInit(shortTopicName);

            return {std::move(server), kafkaPort, serverSettings.AppConfig->GetKafkaProxyConfig(), fullTopicName};
        }

        void CreateMetarequestActor(
                const TActorId& edge, const TVector<TString>& topics, auto* runtime, const auto& kafkaConfig, const TActorId& fakeCacheId = {}
        ) {
            TMetadataRequestData::TPtr metaRequest = std::make_shared<TMetadataRequestData>();
            for (const auto& topicPath : topics) {
                metaRequest->Topics.emplace_back();
                auto& topic = metaRequest->Topics.back();
                topic.Name = topicPath;
            }

            auto context = std::make_shared<TContext>(kafkaConfig);
            context->ConnectionId = edge;
            context->DatabasePath = "/Root";
            context->UserToken = new NACLib::TUserToken("root@builtin", {});

            TActorId actorId;
            if (fakeCacheId) {
                actorId = runtime->Register(new NKafka::TKafkaMetadataActor(
                    context, 1, TMessagePtr<TMetadataRequestData>(std::make_shared<TBuffer>(), metaRequest), fakeCacheId
                ));
            } else {
                actorId = runtime->Register(new NKafka::TKafkaMetadataActor(
                    context, 1, TMessagePtr<TMetadataRequestData>(std::make_shared<TBuffer>(), metaRequest),
                    NKafka::MakeKafkaDiscoveryCacheID()
                ));
            }
            runtime->EnableScheduleForActor(actorId);
        }

        void CheckKafkaMetaResponse(TTestActorRuntime* runtime, ui64 kafkaPort, bool error = false, ui64 expectedCount = 1) {
            TAutoPtr<IEventHandle> handle;
            auto* ev = runtime->GrabEdgeEvent<TEvKafka::TEvResponse>(handle);
            UNIT_ASSERT(ev);
            auto response = dynamic_cast<TMetadataResponseData*>(ev->Response.get());
            UNIT_ASSERT_VALUES_EQUAL(response->Topics.size(), expectedCount);
            if (!error) {
                for (const auto& topic : response->Topics) {
                    UNIT_ASSERT(topic.ErrorCode == EKafkaErrors::NONE_ERROR);
                }
            } else {
                UNIT_ASSERT(response->Topics[0].ErrorCode == EKafkaErrors::LISTENER_NOT_FOUND);
                UNIT_ASSERT(ev->ErrorCode == EKafkaErrors::LISTENER_NOT_FOUND);
                return;
            }
            UNIT_ASSERT_VALUES_EQUAL(response->Brokers.size(), 1);
            Cerr << "Broker " << response->Brokers[0].NodeId << " - " << response->Brokers[0].Host << ":" << response->Brokers[0].Port  << Endl;
            UNIT_ASSERT_VALUES_EQUAL(response->Brokers[0].Port, kafkaPort);
        }

        Y_UNIT_TEST(MetadataActorGetsEndpoint) {
            auto [server, kafkaPort, config, topicName] = SetupServer("topic1");

            auto* runtime = server.GetRuntime();
            auto edge = runtime->AllocateEdgeActor();

            CreateMetarequestActor(edge, {NKikimr::JoinPath({"/Root/PQ/", topicName})}, runtime,
                                   config);

            CheckKafkaMetaResponse(runtime, kafkaPort);
        }

        Y_UNIT_TEST(DiscoveryResponsesWithNoNode) {
            auto [server, kafkaPort, config, topicName] = SetupServer("topic1");

            auto* runtime = server.GetRuntime();
            auto edge = runtime->AllocateEdgeActor();

            Ydb::Discovery::ListEndpointsResult leResult;
            auto* ep = leResult.add_endpoints();
            ep->set_address("wrong.host");
            ep->set_port(1);
            ep->set_node_id(9999);
            ep = leResult.add_endpoints();
            ep->set_address("wrong.host2");
            ep->set_port(2);
            ep->set_node_id(9998);
            auto fakeCache = runtime->Register(new TFakeDiscoveryCache(leResult, false));
            runtime->EnableScheduleForActor(fakeCache);
            CreateMetarequestActor(edge, {NKikimr::JoinPath({"/Root/PQ/", topicName})}, runtime,
                                   config, fakeCache);

            CheckKafkaMetaResponse(runtime, kafkaPort);
        }

        Y_UNIT_TEST(DiscoveryResponsesWithError) {
            auto [server, kafkaPort, config, topicName] = SetupServer("topic1");

            auto* runtime = server.GetRuntime();
            auto edge = runtime->AllocateEdgeActor();

            Ydb::Discovery::ListEndpointsResult leResult;
            auto fakeCache = runtime->Register(new TFakeDiscoveryCache(leResult, true));
            runtime->EnableScheduleForActor(fakeCache);
            CreateMetarequestActor(edge, {NKikimr::JoinPath({"/Root/PQ/", topicName})}, runtime,
                                   config, fakeCache);

            CheckKafkaMetaResponse(runtime, kafkaPort, true);
        }

        Y_UNIT_TEST(DiscoveryResponsesWithOtherPort) {
            auto [server, kafkaPort, config, topicName] = SetupServer("topic1");

            auto* runtime = server.GetRuntime();
            auto edge = runtime->AllocateEdgeActor();

            Ydb::Discovery::ListEndpointsResult leResult;
            auto* ep = leResult.add_endpoints();
            ep->set_address("localhost");
            ep->set_port(12345);
            ep->set_node_id(runtime->GetNodeId(0));
            auto fakeCache = runtime->Register(new TFakeDiscoveryCache(leResult, false));
            runtime->EnableScheduleForActor(fakeCache);
            CreateMetarequestActor(edge, {NKikimr::JoinPath({"/Root/PQ/", topicName})}, runtime,
                                   config, fakeCache);

            CheckKafkaMetaResponse(runtime, 12345);
        }


        Y_UNIT_TEST(MetadataActorDoubleTopic) {
            auto [server, kafkaPort, config, topicName] = SetupServer("topic1");

            auto* runtime = server.GetRuntime();
            auto edge = runtime->AllocateEdgeActor();

            auto path = NKikimr::JoinPath({"/Root/PQ/", topicName});
            CreateMetarequestActor(edge, {path, path}, runtime, config);

            CheckKafkaMetaResponse(runtime, kafkaPort, false, 2);
        }
    }
}
