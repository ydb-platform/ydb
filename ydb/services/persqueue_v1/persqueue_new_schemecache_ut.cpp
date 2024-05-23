#include "actors/read_session_actor.h"
#include <ydb/services/persqueue_v1/ut/pq_data_writer.h>
#include <ydb/services/persqueue_v1/ut/test_utils.h>
#include <ydb/services/persqueue_v1/ut/persqueue_test_fixture.h>

#include <ydb/core/testlib/test_pq_client.h>
#include <ydb/core/persqueue/cluster_tracker.h>
#include <ydb/core/mon/sync_http_mon.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <ydb/library/persqueue/obfuscate/obfuscate.h>
#include <ydb/library/persqueue/tests/counters.h>
#include <ydb/library/persqueue/topic_parser/topic_parser.h>

#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/json/json_reader.h>

#include <util/string/join.h>
#include <util/generic/overloaded.h>

#include <grpcpp/client_context.h>

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

            NYdb::TDriverConfig driverCfg;

            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort).SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG)).SetDatabase("/Root");

            auto ydbDriver = MakeHolder<NYdb::TDriver>(driverCfg);


            ModifyTopicACL(ydbDriver.Get(), "/Root/account2/topic2", {{"topic1@" BUILTIN_ACL_DOMAIN, {"ydb.generic.write"}}});
            ModifyTopicACL(ydbDriver.Get(), "/Root/PQ/account1/topic1", {{"topic1@" BUILTIN_ACL_DOMAIN, {"ydb.generic.write"}}});

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
                auto res = persQueueClient->AddReadRule("/Root/account2/topic2",
                    TAddReadRuleSettings().ReadRule(TReadRuleSettings().ConsumerName("user1")));
                res.Wait();
                UNIT_ASSERT(res.GetValue().IsSuccess());
            }

            ModifyTopicACL(ydbDriver.Get(), "/Root/account2/topic2", {{"user1@" BUILTIN_ACL_DOMAIN, {"ydb.generic.read"}}});

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

        void TestReadAtTimestampImpl(ui32 maxMessagesCount, std::function<TString(ui32)> generateMessage) {
            TTestServer server(false);
            server.ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);
            server.StartServer();

            server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::TX_PROXY_SCHEME_CACHE});

            Cerr << ">>>>> Prepare scheme" << Endl;
            PrepareForGrpcNoDC(*server.AnnoyingClient);

            Cerr << ">>>>> Create PersQueue client" << Endl;
            NYdb::TDriverConfig driverCfg;
            driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort)
                .SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG))
                .SetDatabase("/Root");

            auto ydbDriver = MakeHolder<NYdb::TDriver>(driverCfg);
            auto persqueueClient = MakeHolder<NYdb::NPersQueue::TPersQueueClient>(*ydbDriver);

            // Topic was created in PrepareForGrpcNoDC
            const TString topic = "account2/topic2";
            const TString consumerName = "userx";

            {
                Cerr << ">>>>> Create consumer '" << consumerName << "'" << Endl;
                auto res = persqueueClient->AddReadRule("/Root/" + topic,
                                    NYdb::NPersQueue::TAddReadRuleSettings()
                                        .ReadRule(NYdb::NPersQueue::TReadRuleSettings()
                                        .ConsumerName(consumerName)));
                res.Wait();
                UNIT_ASSERT(res.GetValue().IsSuccess());
            }

            Cerr << ">>>>> Create writeSession" << Endl;
            auto writeSessionSettings = NYdb::NPersQueue::TWriteSessionSettings()
                            .ClusterDiscoveryMode(NYdb::NPersQueue::EClusterDiscoveryMode::Off)
                            .Path(topic)
                            .MessageGroupId(topic)
                            .Codec(NYdb::NPersQueue::ECodec::RAW);
            auto writeSession = persqueueClient->CreateWriteSession(writeSessionSettings);

            TMaybe<TContinuationToken> continuationToken = Nothing();
            ui32 messagesAcked = 0;
            auto processEvent = [&](TWriteSessionEvent::TEvent& event) {
                std::visit(TOverloaded {
                        [&](const TWriteSessionEvent::TAcksEvent& event) {
                            //! Acks just confirm that message was received and saved by server successfully.
                            //! Here we just count acked messages to check, that everything written is confirmed.
                            Cerr << "GOT ACK " << TInstant::Now() << "\n";
                            Sleep(TDuration::MilliSeconds(3));
                            for (const auto& ack : event.Acks) {
                                Y_UNUSED(ack);
                                messagesAcked++;
                            }
                        },
                        [&](TWriteSessionEvent::TReadyToAcceptEvent& event) {
                            continuationToken = std::move(event.ContinuationToken);
                        },
                        [&](const TSessionClosedEvent&) {
                            UNIT_ASSERT(false);
                        }
                }, event);
                };

            Cerr << ">>>>> Receiving continuationToken" << Endl;
            for (auto& event: writeSession->GetEvents(true)) {
                processEvent(event);
            }
            UNIT_ASSERT(continuationToken.Defined());

            Cerr << ">>>>> Write messages" << Endl;
            for (ui32 i = 0; i < maxMessagesCount; ++i) {
                TString message = generateMessage(i);
                Cerr << "WRITTEN message " << i << "\n";
                writeSession->Write(std::move(*continuationToken), std::move(message));
                //! Continue token is no longer valid once used.
                continuationToken = Nothing();
                while (messagesAcked <= i || !continuationToken.Defined()) {
                    for (auto& event: writeSession->GetEvents(true)) {
                        processEvent(event);
                    }
                }
                Sleep(TDuration::MilliSeconds(10));
            }

            // Ts and firstOffset and expectingQuantities will be set in first iteration of reading by received messages.
            // Each will contains shifts from the message: before, equals and after.
            // It allow check reading from different shift. First iteration read from zero.
            TVector<TInstant> ts { TInstant::Zero() };
            TVector<ui32> firstOffset { 0 };
            TVector<size_t> expectingQuantities { maxMessagesCount };

            // Start test scenario

            Cerr << ">>>>> Start reading" << Endl << Flush;
            for (size_t i = 0; i < ts.size(); ++i) {
                TInstant curTs = ts[i];
                size_t expectingQuantity = expectingQuantities[i];

                Cerr << ">>>>> Iteration: " << i << " Start reading from " << curTs << ". ExpectingQuantity " << expectingQuantity << Endl << Flush;

                // Accumulate received messages
                //  Key is unique message body
                //  Value is quantity of received messages with it body
                TMap<TString, size_t> map;

                std::shared_ptr<NYdb::NPersQueue::IReadSession> reader;
                auto settings = NYdb::NPersQueue::TReadSessionSettings()
                        .AppendTopics(topic)
                        .ConsumerName(consumerName)
                        .StartingMessageTimestamp(curTs)
                        .ReadOnlyOriginal(true);

                ui32 lastOffset = 0;

                settings.EventHandlers_.SimpleDataHandlers([&](NYdb::NPersQueue::TReadSessionEvent::TDataReceivedEvent& event) mutable {
                        Cerr << ">>>>> Iteration: " << i << " TDataReceivedEvent: " << event.DebugString(false)
                             << " size=" << event.GetMessages().size() << Endl << Flush;
                        for (const auto& msg : event.GetMessages()) {
                            Cerr << ">>>>> Iteration: " << i << " Got message: " << msg.GetData().substr(0, 16)
                                                        << " :: " << msg.DebugString(false) << Endl << Flush;

                            auto count = ++map[msg.GetData()];
                            UNIT_ASSERT_C(count == 1, "Each message must be received once");
                            if (i == 0) {
                                // First iteration. Filling ts and firstOffset vectors from received messages
                                ts.push_back(msg.GetWriteTime() - TDuration::MilliSeconds(1));
                                ts.push_back(msg.GetWriteTime());
                                ts.push_back(msg.GetWriteTime() + TDuration::MilliSeconds(1));

                                firstOffset.push_back(msg.GetOffset());
                                firstOffset.push_back(msg.GetOffset());
                                firstOffset.push_back(msg.GetOffset() + 1);

                                size_t prevQuantity = expectingQuantities.back();
                                expectingQuantities.push_back(prevQuantity);
                                expectingQuantities.push_back(prevQuantity);
                                expectingQuantities.push_back(prevQuantity - 1);

                                Cerr << ">>>>> Iteration: " << i << " GOT MESSAGE TIMESTAMP " << msg.GetWriteTime() << Endl << Flush;
                            } else {
                                if (map.size() == 1) {
                                    auto expectedOffset = firstOffset[i];
                                    UNIT_ASSERT_EQUAL_C(msg.GetOffset(), expectedOffset, "Iteration: " << i
                                                                << " Expected first message offset " << expectedOffset
                                                                << " but got " << msg.GetOffset());
                                } else {
                                    UNIT_ASSERT_C(lastOffset < msg.GetOffset(), "Iteration: " << i
                                                                << " unexpected offset order. Last offset " << lastOffset
                                                                << " Message offset " << msg.GetOffset());
                                }

                                lastOffset = msg.GetOffset();
                            }
                        }
                    }, false);
                reader = CreateReader(*ydbDriver, settings);

                Cerr << ">>>>> Iteration: " << i << " Reader was created" << Endl << Flush;

                Cerr << ">>>>> Iteration: " << i << " Wait receiving all messages" << Endl << Flush;
                Sleep(TDuration::MilliSeconds(10));
                for (size_t k = 0; k < 1000 && map.size() < expectingQuantity; ++k) { // Wait 10 seconds
                  Sleep(TDuration::MilliSeconds(10));
                }

                Cerr << ">>>>> Iteration: " << i << " Closing session. Got " << map.size() << " messages" << Endl << Flush;
                while(!reader->Close(TDuration::Seconds(1))) {};
                Cerr << ">>>>> Iteration: " << i << " Session closed" << Endl << Flush;

                if (i == 0) {
                    for (ui32 j = 1; j < ts.size(); ++j) {
                        Cerr << ">>>>> Planed iteration: " << j
                             << ". Start reading from time: " << ts[j]
                             << ". Expected first message offset: " << firstOffset[j]
                             << ". Expected message quantity: " << expectingQuantities[j] << Endl;
                    }
                }
                UNIT_ASSERT_EQUAL_C(map.size(), expectingQuantity, "Wrong message number. Received: " << map.size() << ". Excpected: " << expectingQuantity);
            }
        }

        Y_UNIT_TEST(TestReadAtTimestamp_3) {
            auto generate = [](ui32 messageId) {
                return TStringBuilder() << "Hello___" << messageId << "___" << CreateGuidAsString() << TString(1_MB, 'a');
            };

            TestReadAtTimestampImpl(3, generate);
        }

        Y_UNIT_TEST(TestReadAtTimestamp_10) {
            auto generate = [](ui32 messageId) {
                return TStringBuilder() << "Hello___" << messageId << "___" << CreateGuidAsString() << TString(1_MB, 'a');
            };

            TestReadAtTimestampImpl(10, generate);
        }

        Y_UNIT_TEST(TestWriteStat1stClass) {
            auto testWriteStat1stClass = [](const TString& consumerName) {
                TTestServer server(false);
                server.ServerSettings.PQConfig.SetTopicsAreFirstClassCitizen(true);
                server.StartServer();
                server.EnableLogs({NKikimrServices::PQ_READ_PROXY, NKikimrServices::TX_PROXY_SCHEME_CACHE});

                const TString topicName{"account2/topic2"};
                const TString fullTopicName{"/Root/account2/topic2"};
                const TString folderId{"somefolder"};
                const TString cloudId{"somecloud"};
                const TString databaseId{"root"};
                UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
                                         server.AnnoyingClient->AlterUserAttributes("/", "Root",
                                                                                    {{"folder_id", folderId},
                                                                                     {"cloud_id", cloudId},
                                                                                     {"database_id", databaseId}}));

                server.AnnoyingClient->SetNoConfigMode();
                server.AnnoyingClient->FullInit();
                server.AnnoyingClient->InitUserRegistry();
                server.AnnoyingClient->MkDir("/Root", "account2");
                server.AnnoyingClient->CreateTopicNoLegacy(fullTopicName, 5);

                NYdb::TDriverConfig driverCfg;

                driverCfg.SetEndpoint(TStringBuilder() << "localhost:" << server.GrpcPort).SetLog(CreateLogBackend("cerr", ELogPriority::TLOG_DEBUG)).SetDatabase("/Root");

                const auto monPort = TPortManager().GetPort();
                auto Counters = server.CleverServer->GetGRpcServerRootCounters();
                NActors::TSyncHttpMon Monitoring({
                    .Port = monPort,
                    .Address = "localhost",
                    .Threads = 3,
                    .Title = "root",
                    .Host = "localhost",
                });
                Monitoring.RegisterCountersPage("counters", "Counters", Counters);
                Monitoring.Start();

                auto ydbDriver = MakeHolder<NYdb::TDriver>(driverCfg);
                auto persQueueClient = MakeHolder<NYdb::NPersQueue::TPersQueueClient>(*ydbDriver);

                {
                    auto res = persQueueClient->AddReadRule(fullTopicName,
                        TAddReadRuleSettings().ReadRule(TReadRuleSettings().ConsumerName(consumerName)));
                    res.Wait();
                    UNIT_ASSERT(res.GetValue().IsSuccess());
                }

                auto checkCounters =
                    [cloudId, folderId, databaseId](auto monPort,
                                                    const std::set<std::string>& canonicalSensorNames,
                                                    const TString& stream, const TString& consumer,
                                                    const TString& host, const TString& shard) {
                        auto counters = GetCounters1stClass(monPort, "datastreams", "%2FRoot", cloudId,
                                                            databaseId, folderId, stream, consumer, host,
                                                            shard);
                        const auto sensors = counters["sensors"].GetArray();
                        std::set<std::string> sensorNames;
                        std::transform(sensors.begin(), sensors.end(),
                                       std::inserter(sensorNames, sensorNames.begin()),
                                       [](auto& el) {
                                           return el["labels"]["name"].GetString();
                                       });
                        auto equal = sensorNames == canonicalSensorNames;
                        UNIT_ASSERT(equal);
                    };

                {
                    NYdb::NScheme::TSchemeClient schemeClient(*ydbDriver);
                    NYdb::NScheme::TPermissions permissions("user@builtin", {"ydb.generic.read", "ydb.generic.write"});

                    auto result = schemeClient.ModifyPermissions("/Root",
                                                                 NYdb::NScheme::TModifyPermissionsSettings().AddGrantPermissions(permissions)).ExtractValueSync();
                    Cerr << result.GetIssues().ToString() << "\n";
                    UNIT_ASSERT(result.IsSuccess());
                }

                {
                    auto newDriverCfg = driverCfg;
                    newDriverCfg.SetAuthToken("user@builtin");

                    ydbDriver = MakeHolder<NYdb::TDriver>(newDriverCfg);

                    auto writer = CreateSimpleWriter(*ydbDriver, fullTopicName, "123", 1);
                    for (int i = 0; i < 4; ++i) {
                        bool res = writer->Write(TString(10, 'a'));
                        UNIT_ASSERT(res);
                    }

                    NYdb::NPersQueue::TReadSessionSettings settings;
                    settings.ConsumerName(consumerName).AppendTopics(topicName);
                    auto reader = CreateReader(*ydbDriver, settings);

                    auto msg = GetNextMessageSkipAssignment(reader);
                    UNIT_ASSERT(msg);

                    checkCounters(monPort,
                                  {
                                      "api.grpc.topic.stream_read.commits",
                                      "api.grpc.topic.stream_read.partition_session.errors",
                                      "api.grpc.topic.stream_read.partition_session.started",
                                      "api.grpc.topic.stream_read.partition_session.stopped",
                                      "api.grpc.topic.stream_read.partition_session.count",
                                      "api.grpc.topic.stream_read.partition_session.starting_count",
                                      "api.grpc.topic.stream_read.partition_session.stopping_count",
                                      "api.grpc.topic.stream_write.errors",
                                      "api.grpc.topic.stream_write.sessions_active_count",
                                      "api.grpc.topic.stream_write.sessions_created",
                                  },
                                  topicName, "", "", ""
                                  );

                    checkCounters(monPort,
                                  {
                                      "api.grpc.topic.stream_read.commits",
                                      "api.grpc.topic.stream_read.partition_session.errors",
                                      "api.grpc.topic.stream_read.partition_session.started",
                                      "api.grpc.topic.stream_read.partition_session.stopped",
                                      "api.grpc.topic.stream_read.partition_session.count",
                                      "api.grpc.topic.stream_read.partition_session.starting_count",
                                      "api.grpc.topic.stream_read.partition_session.stopping_count",

                                  },
                                  topicName, consumerName, "", ""
                                  );

                    checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                                  {
                                      "topic.read.lag_milliseconds",
                                      "topic.write.bytes",
                                      "topic.write.messages",
                                      "topic.write.discarded_bytes",
                                      "topic.write.discarded_messages",
                                      "api.grpc.topic.stream_write.bytes",
                                      "topic.write.partition_throttled_milliseconds",
                                      "topic.write.message_size_bytes",
                                      "api.grpc.topic.stream_write.messages",
                                      "topic.write.lag_milliseconds",
                                      "topic.write.uncompressed_bytes",
                                      "api.grpc.topic.stream_read.bytes",
                                      "api.grpc.topic.stream_read.messages",
                                      "topic.read.bytes",
                                      "topic.read.messages",
                                  },
                                  topicName, "", "", ""
                                  );

                    checkCounters(server.CleverServer->GetRuntime()->GetMonPort(),
                                  {
                                      "topic.read.lag_milliseconds",
                                      "api.grpc.topic.stream_read.bytes",
                                      "api.grpc.topic.stream_read.messages",
                                      "topic.read.bytes",
                                      "topic.read.messages",
                                  },
                                  topicName, consumerName, "", ""
                                  );
                }
            };

            testWriteStat1stClass("user1");
            testWriteStat1stClass("some@random@consumer");
        }
    } // Y_UNIT_TEST_SUITE(TPersQueueNewSchemeCacheTest)


    Y_UNIT_TEST_SUITE(TPersqueueDataPlaneTestSuite) {
        Y_UNIT_TEST(WriteSession) {
            TPersQueueV1TestServer server({.CheckACL=true, .TenantModeEnabled=true});

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
                                                                    .RetryPolicy(NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy()));
                Cerr << "InitSeqNO " << writer->GetInitSeqNo() << "\n";
                writer->Write("somedata", 1);
                writer->Close();
            }
            {
                auto reader = server.PersQueueClient->CreateReadSession(TReadSessionSettings().ConsumerName("non_existing")
                                                                        .AppendTopics(topic).DisableClusterDiscovery(true)
                                                                        .RetryPolicy(NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy()));


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
                                                                        .RetryPolicy(NYdb::NPersQueue::IRetryPolicy::GetNoRetryPolicy()));


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
            TPersQueueV1TestServer server({.TenantModeEnabled=true});

            {
                auto res = server.PersQueueClient->AddReadRule("/Root/acc/topic1", TAddReadRuleSettings().ReadRule(TReadRuleSettings().ConsumerName("user1")));
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
            TPersQueueV1TestServer server({.TenantModeEnabled=true});

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
            TPersQueueV1TestServer server({.TenantModeEnabled=true});
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
                rr->set_important(false);
                rr->set_supported_format(TopicSettings::FORMAT_BASE);
                rr->add_supported_codecs(CODEC_GZIP);
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
                rr->set_important(false);
                rr->set_supported_format(TopicSettings::FORMAT_BASE);
                rr->add_supported_codecs(CODEC_GZIP);
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
