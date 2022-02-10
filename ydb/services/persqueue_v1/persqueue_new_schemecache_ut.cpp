#include "grpc_pq_actor.h"
#include <ydb/services/persqueue_v1/ut/pq_data_writer.h>
#include <ydb/services/persqueue_v1/ut/test_utils.h>
#include <ydb/services/persqueue_v1/ut/persqueue_test_fixture.h>

#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/core/persqueue/cluster_tracker.h>

#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <ydb/library/aclib/aclib.h>
#include <ydb/library/persqueue/obfuscate/obfuscate.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_reader.h>

#include <util/string/join.h>

#include <grpc++/client_context.h>

#include <ydb/public/api/grpc/draft/ydb_persqueue_v1.grpc.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/data_plane_helpers.h>

namespace {
    const static TString DEFAULT_TOPIC_NAME = "rt3.dc1--topic1";
    const static TString DEFAULT_TOPIC_PATH = "/Root/PQ/rt3.dc1--topic1";

    const static TString SHORT_TOPIC_NAME = "topic1";
}


namespace NKikimr::NPersQueueTests {

    using namespace Tests;
    using namespace NKikimrClient;
    using namespace Ydb::PersQueue;
    using namespace Ydb::PersQueue::V1;
    using namespace NThreading;
    using namespace NNetClassifier;
    using namespace NYdb::NPersQueue; 
    using namespace NPersQueue;

    NJson::TJsonValue GetCountersNewSchemeCache(ui16 port, const TString& counters, const TString& subsystem, const TString& topicPath) {
        TString escapedPath = "%2F" + JoinStrings(SplitString(topicPath, "/"), "%2F");
        TString query = TStringBuilder() << "/counters/counters=" << counters
                                         << "/subsystem=" << subsystem
                                         << "/Topic=" << escapedPath << "/json";

        Cerr << "Will execute query " << query << Endl;
        TNetworkAddress addr("localhost", port);
        TSocket s(addr);

        SendMinimalHttpRequest(s, "localhost", query);
        TSocketInput si(s);
        THttpInput input(&si);
        Cerr << input.ReadAll() << Endl;
        unsigned httpCode = ParseHttpRetCode(input.FirstLine());
        UNIT_ASSERT_VALUES_EQUAL(httpCode, 200u);

        NJson::TJsonValue value;
        UNIT_ASSERT(NJson::ReadJsonTree(&input, &value));

        Cerr << "counters: " << value.GetStringRobust() << "\n";
        return value;
    }

    Y_UNIT_TEST_SUITE(TPersQueueNewSchemeCacheTest) {

        void PrepareForGrpcNoDC(TFlatMsgBusPQClient& annoyingClient) {
            annoyingClient.SetNoConfigMode();
            annoyingClient.FullInit();
            annoyingClient.InitUserRegistry();
            annoyingClient.MkDir("/Root", "account1");
            annoyingClient.MkDir("/Root/PQ", "account1");
            annoyingClient.CreateTopicNoLegacy(DEFAULT_TOPIC_PATH, 5, false);
            annoyingClient.CreateTopicNoLegacy("/Root/PQ/account1/topic1", 5, false);
            annoyingClient.CreateTopicNoLegacy("/Root/account2/topic2", 5); 
        }

        Y_UNIT_TEST(CheckGrpcWriteNoDC) {
            TTestServer server(false);
            server.ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);

            server.StartServer();
            server.EnableLogs({
                NKikimrServices::PQ_WRITE_PROXY, NKikimrServices::TX_PROXY_SCHEME_CACHE,
                NKikimrServices::FLAT_TX_SCHEMESHARD, NKikimrServices::PQ_METACACHE}
            );
            PrepareForGrpcNoDC(*server.AnnoyingClient);

            TPQDataWriter writer("source1", server, DEFAULT_TOPIC_PATH);

            writer.Write("/Root/account2/topic2", {"valuevaluevalue1"}, true, "topic1@" BUILTIN_ACL_DOMAIN); 
            writer.Write("/Root/PQ/account1/topic1", {"valuevaluevalue1"}, true, "topic1@" BUILTIN_ACL_DOMAIN);

            NACLib::TDiffACL acl;
            acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, "topic1@" BUILTIN_ACL_DOMAIN);
            server.AnnoyingClient->ModifyACL("/Root/account2", "topic2", acl.SerializeAsString()); 
            server.AnnoyingClient->ModifyACL("/Root/PQ/account1", "topic1", acl.SerializeAsString());

            WaitACLModification();
            writer.Write("/Root/account2/topic2", {"valuevaluevalue1"}, false, "topic1@" BUILTIN_ACL_DOMAIN); 

            writer.Write("/Root/PQ/account1/topic1", {"valuevaluevalue1"}, false, "topic1@" BUILTIN_ACL_DOMAIN);
            writer.Write("/Root/PQ/account1/topic1", {"valuevaluevalue2"}, false, "topic1@" BUILTIN_ACL_DOMAIN);

        }

        Y_UNIT_TEST(CheckGrpcReadNoDC) {
            TTestServer server(false);
            server.ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);
            server.StartServer();
            server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::TX_PROXY_SCHEME_CACHE});
            PrepareForGrpcNoDC(*server.AnnoyingClient);
            NYdb::TDriverConfig driverCfg;

            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort).SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG)).SetDatabase("/Root"); 
 
            auto ydbDriver = MakeHolder<NYdb::TDriver>(driverCfg);
            auto persQueueClient = MakeHolder<NYdb::NPersQueue::TPersQueueClient>(*ydbDriver);

            { 
                auto res = persQueueClient->AddReadRule("/Root/account2/topic2", TAddReadRuleSettings().ReadRule(TReadRuleSettings().ConsumerName("user1"))); 
                res.Wait(); 
                UNIT_ASSERT(res.GetValue().IsSuccess()); 
            } 

            {
                NACLib::TDiffACL acl;
                acl.AddAccess(NACLib::EAccessType::Allow, NACLib::SelectRow, "user1@" BUILTIN_ACL_DOMAIN);
                server.AnnoyingClient->ModifyACL("/Root/account2", "topic2", acl.SerializeAsString()); 
            }

            WaitACLModification();

            {
                auto writer = CreateSimpleWriter(*ydbDriver, "/Root/account2/topic2", "123", 1); 
                for (int i = 0; i < 4; ++i) {
                    bool res = writer->Write(TString(10, 'a')); 
                    UNIT_ASSERT(res); 
                }
                bool res = writer->Close(TDuration::Seconds(10)); 
                UNIT_ASSERT(res); 
            }

            auto testReadFromTopic = [&](const TString& topicPath) {
                NYdb::NPersQueue::TReadSessionSettings settings; 
                settings.ConsumerName("user1").AppendTopics(topicPath); 
                auto reader = CreateReader(*ydbDriver, settings); 

                for (int i = 0; i < 4; ++i) { 
                    auto msg = GetNextMessageSkipAssignment(reader); 
                    UNIT_ASSERT(msg); 
                    Cerr << "GOT MESSAGE: " << DebugString(*msg) << "\n"; 
                }
            };

            testReadFromTopic("/Root/account2/topic2"); 
            testReadFromTopic("account2/topic2"); 
        }

    }

 
    Y_UNIT_TEST_SUITE(TPersqueueDataPlaneTestSuite) { 
        Y_UNIT_TEST(WriteSession) {
            TPersQueueV1TestServer server(true);
 
            TString topic = "/Root/account1/write_topic"; 
            TString consumer = "consumer_aba"; 
            { 
                auto res = server.PersQueueClient->CreateTopic(topic);
                res.Wait(); 
                UNIT_ASSERT(res.GetValue().IsSuccess()); 
            } 
 
            { 
                auto res = server.PersQueueClient->AddReadRule(topic, TAddReadRuleSettings().ReadRule(TReadRuleSettings().ConsumerName(consumer)));
                res.Wait(); 
                UNIT_ASSERT(res.GetValue().IsSuccess()); 
            } 
 
            { 
                auto writer = server.PersQueueClient->CreateSimpleBlockingWriteSession(TWriteSessionSettings()
                                                                    .Path(topic).MessageGroupId("my_group_1") 
                                                                    .ClusterDiscoveryMode(EClusterDiscoveryMode::Off) 
                                                                    .RetryPolicy(IRetryPolicy::GetNoRetryPolicy())); 
                Cerr << "InitSeqNO " << writer->GetInitSeqNo() << "\n"; 
                writer->Write("somedata", 1); 
                writer->Close(); 
            } 
            { 
                auto reader = server.PersQueueClient->CreateReadSession(TReadSessionSettings().ConsumerName("non_existing")
                                                                        .AppendTopics(topic).DisableClusterDiscovery(true) 
                                                                        .RetryPolicy(IRetryPolicy::GetNoRetryPolicy())); 
 
 
                auto future = reader->WaitEvent(); 
                future.Wait(TDuration::Seconds(10)); 
                UNIT_ASSERT(future.HasValue()); 
 
                TMaybe<NYdb::NPersQueue::TReadSessionEvent::TEvent> event = reader->GetEvent(false); 
                UNIT_ASSERT(event.Defined()); 
 
                Cerr << "Got new read session event: " << DebugString(*event) << Endl; 
 
                UNIT_ASSERT(std::get_if<TSessionClosedEvent>(&*event)); 
            } 
            { 
                auto reader = server.PersQueueClient->CreateReadSession(TReadSessionSettings().ConsumerName(consumer)
                                                                        .AppendTopics(topic).DisableClusterDiscovery(true) 
                                                                        .RetryPolicy(IRetryPolicy::GetNoRetryPolicy())); 
 
 
                auto future = reader->WaitEvent(); 
                future.Wait(TDuration::Seconds(10)); 
                UNIT_ASSERT(future.HasValue()); 
 
                TMaybe<NYdb::NPersQueue::TReadSessionEvent::TEvent> event = reader->GetEvent(false); 
                UNIT_ASSERT(event.Defined()); 
 
                Cerr << "Got new read session event: " << DebugString(*event) << Endl; 
 
                UNIT_ASSERT(std::get_if<TReadSessionEvent::TCreatePartitionStreamEvent>(&*event)); 
            } 
        } 
    } 
 
    Y_UNIT_TEST_SUITE(TPersqueueControlPlaneTestSuite) { 
        Y_UNIT_TEST(SetupReadLockSessionWithDatabase) {
            TPersQueueV1TestServer server;
 
            { 
                auto res = server.PersQueueClient->AddReadRule("/Root/acc/topic1", TAddReadRuleSettings().ReadRule(TReadRuleSettings().ConsumerName("user")));
                res.Wait(); 
                Cerr << "ADD RESULT " << res.GetValue().GetIssues().ToString() << "\n"; 
                UNIT_ASSERT(res.GetValue().IsSuccess()); 
            } 
 
 
            auto stub = Ydb::PersQueue::V1::PersQueueService::NewStub(server.InsecureChannel);
            grpc::ClientContext grpcContext;
            grpcContext.AddMetadata("x-ydb-database", "/Root/acc");
            auto readStream = stub->MigrationStreamingRead(&grpcContext);
            UNIT_ASSERT(readStream);

            // init read session
            {
                MigrationStreamingReadClientMessage req;
                MigrationStreamingReadServerMessage resp;

                req.mutable_init_request()->add_topics_read_settings()->set_topic("topic1");

                req.mutable_init_request()->set_consumer("user");
                req.mutable_init_request()->mutable_read_params()->set_max_read_messages_count(3);

                if (!readStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }

                UNIT_ASSERT(readStream->Read(&resp));
                UNIT_ASSERT(resp.response_case() == MigrationStreamingReadServerMessage::kInitResponse);
            }
        }

        Y_UNIT_TEST(SetupWriteLockSessionWithDatabase) {
            TPersQueueV1TestServer server;

            auto stub = Ydb::PersQueue::V1::PersQueueService::NewStub(server.InsecureChannel);
            grpc::ClientContext grpcContext;
            grpcContext.AddMetadata("x-ydb-database", "/Root/acc");

            auto writeStream = stub->StreamingWrite(&grpcContext);
            UNIT_ASSERT(writeStream);

            {
                StreamingWriteClientMessage req;
                StreamingWriteServerMessage resp;

                req.mutable_init_request()->set_topic("topic1");
                req.mutable_init_request()->set_message_group_id("12345678");
                if (!writeStream->Write(req)) {
                    ythrow yexception() << "write fail";
                }

                UNIT_ASSERT(writeStream->Read(&resp));
                UNIT_ASSERT(resp.has_init_response());
            }
        }

        Y_UNIT_TEST(TestAddRemoveReadRule) {
            TPersQueueV1TestServer server;
            SET_LOCALS;

            pqClient->CreateConsumer("goodUser");

            TString path = server.GetTopicPath();

            Ydb::PersQueue::V1::AddReadRuleRequest addRuleRequest;
            Ydb::PersQueue::V1::AddReadRuleResponse addRuleResponse;
            addRuleRequest.set_path(path);

            auto stub = Ydb::PersQueue::V1::PersQueueService::NewStub(server.InsecureChannel);
            {
                grpc::ClientContext grpcContext;
                grpcContext.AddMetadata("x-ydb-database", "/Root/acc");
                addRuleRequest.set_path("topic1");
                auto* rr = addRuleRequest.mutable_read_rule();
                rr->set_consumer_name("goodUser");
                rr->set_version(0); 
                rr->set_important(true);
                rr->set_supported_format(TopicSettings::FORMAT_BASE);
                rr->add_supported_codecs(CODEC_ZSTD);
                auto status = stub->AddReadRule(&grpcContext, addRuleRequest, &addRuleResponse);
                Cerr << "ADD RR RESPONSE " << addRuleResponse << "\n"; 
                UNIT_ASSERT(status.ok() && addRuleResponse.operation().status() == Ydb::StatusIds::SUCCESS);
                addRuleRequest.set_path(path);
            }

            // don't allow add the same read rule twice
            {
                grpc::ClientContext grpcContext;
                auto status = stub->AddReadRule(&grpcContext, addRuleRequest, &addRuleResponse);
                UNIT_ASSERT(status.ok() && addRuleResponse.operation().status() == Ydb::StatusIds::ALREADY_EXISTS);
            }

            Ydb::PersQueue::V1::RemoveReadRuleRequest removeRuleRequest;
            Ydb::PersQueue::V1::RemoveReadRuleResponse removeRuleResponse;
            removeRuleRequest.set_path(path);
            removeRuleRequest.set_consumer_name("badUser");

            // trying to remove user that not exist
            {
                grpc::ClientContext grpcContext;
                auto status = stub->RemoveReadRule(&grpcContext, removeRuleRequest, &removeRuleResponse);
                UNIT_ASSERT(status.ok() && removeRuleResponse.operation().status() == Ydb::StatusIds::NOT_FOUND);
            }
            auto findReadRule = [&](const TString& consumerName, const TMaybe<i64> version, const TopicSettings& settings) { 
                for (const auto& rr : settings.read_rules()) {
                    if (rr.consumer_name() == consumerName) {
                        Cerr << rr << "\n"; 
                        return !version || rr.version() == *version; 
                    }
                }
                return false;
            };

            Ydb::PersQueue::V1::DescribeTopicRequest describeTopicRequest;
            Ydb::PersQueue::V1::DescribeTopicResponse describeTopicResponse;
            describeTopicRequest.set_path(path);
            {
                grpc::ClientContext grpcContext;
                grpcContext.AddMetadata("x-ydb-database", "/Root/acc");
                addRuleRequest.set_path("topic1");
                auto status = stub->DescribeTopic(&grpcContext, describeTopicRequest, &describeTopicResponse);
                Ydb::PersQueue::V1::DescribeTopicResult res;
                UNIT_ASSERT(status.ok() && describeTopicResponse.operation().status() == Ydb::StatusIds::SUCCESS);
                describeTopicResponse.operation().result().UnpackTo(&res);
                UNIT_ASSERT(findReadRule("goodUser", 1, res.settings())); 
                addRuleRequest.set_path(path);
            }

            {
                grpc::ClientContext grpcContext;
                grpcContext.AddMetadata("x-ydb-database", "/Root/acc");
                removeRuleRequest.set_consumer_name("goodUser");
                auto status = stub->RemoveReadRule(&grpcContext, removeRuleRequest, &removeRuleResponse);
                Cerr << removeRuleResponse.ShortDebugString() << Endl;
                UNIT_ASSERT(status.ok() && removeRuleResponse.operation().status() == Ydb::StatusIds::SUCCESS);
                removeRuleRequest.set_path(path);
            }
            {
                grpc::ClientContext grpcContext;
                stub->DescribeTopic(&grpcContext, describeTopicRequest, &describeTopicResponse);
                Ydb::PersQueue::V1::DescribeTopicResult res;
                describeTopicResponse.operation().result().UnpackTo(&res);
                UNIT_ASSERT(!findReadRule("goodUser", {}, res.settings())); 
            }
 
            { 
                grpc::ClientContext grpcContext; 
                auto* rr = addRuleRequest.mutable_read_rule(); 
                rr->set_consumer_name("goodUser"); 
                rr->set_version(0); 
                rr->set_important(true); 
                rr->set_supported_format(TopicSettings::FORMAT_BASE); 
                rr->add_supported_codecs(CODEC_ZSTD); 
                auto status = stub->AddReadRule(&grpcContext, addRuleRequest, &addRuleResponse); 
                Cerr << addRuleResponse << "\n"; 
                UNIT_ASSERT(status.ok() && addRuleResponse.operation().status() == Ydb::StatusIds::SUCCESS); 
            } 
 
            { 
                grpc::ClientContext grpcContext; 
                auto status = stub->DescribeTopic(&grpcContext, describeTopicRequest, &describeTopicResponse); 
                Ydb::PersQueue::V1::DescribeTopicResult res; 
                UNIT_ASSERT(status.ok() && describeTopicResponse.operation().status() == Ydb::StatusIds::SUCCESS); 
                describeTopicResponse.operation().result().UnpackTo(&res); 
                UNIT_ASSERT(findReadRule("goodUser", 3, res.settings())); // version is 3 : add, remove and add 
            } 
 
        }
    }
}
