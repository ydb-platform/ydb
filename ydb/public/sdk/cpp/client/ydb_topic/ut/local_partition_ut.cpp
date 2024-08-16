#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/common/trace_lazy.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/trace.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/impl/write_session.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <format>

using namespace NYdb;
using namespace NYdb::NPersQueue::NTests;

namespace NYdb::NTopic::NTests {

    Y_UNIT_TEST_SUITE(LocalPartition) {
        std::shared_ptr<TTopicSdkTestSetup> CreateSetup(const TString& testCaseName, ui32 nodeCount = 1, bool createTopic = true) {
            NKikimr::Tests::TServerSettings settings = TTopicSdkTestSetup::MakeServerSettings();
            settings.SetNodeCount(nodeCount);
            return std::make_shared<TTopicSdkTestSetup>(testCaseName, settings, createTopic);
        }

        TTopicSdkTestSetup CreateSetupForSplitMerge(const TString& testCaseName) {
            NKikimrConfig::TFeatureFlags ff;
            ff.SetEnableTopicSplitMerge(true);
            ff.SetEnablePQConfigTransactionsAtSchemeShard(true);
            auto settings = TTopicSdkTestSetup::MakeServerSettings();
            settings.SetFeatureFlags(ff);
            auto setup = TTopicSdkTestSetup(testCaseName, settings, false);
            setup.GetRuntime().GetAppData().PQConfig.SetTopicsAreFirstClassCitizen(true);
            setup.GetRuntime().GetAppData().PQConfig.SetUseSrcIdMetaMappingInFirstClass(true);
            return setup;
        }

        NYdb::TDriverConfig CreateConfig(const TTopicSdkTestSetup& setup, TString discoveryAddr)
        {
            NYdb::TDriverConfig config = setup.MakeDriverConfig();
            config.SetEndpoint(discoveryAddr);
            return config;
        }

        TWriteSessionSettings CreateWriteSessionSettings()
        {
            return TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .ProducerId(TEST_MESSAGE_GROUP_ID)
                .PartitionId(0)
                .DirectWriteToPartition(true);
        }

        TReadSessionSettings CreateReadSessionSettings()
        {
            return TReadSessionSettings()
                .ConsumerName(TEST_CONSUMER)
                .AppendTopics(TEST_TOPIC);
        }

        void WriteMessage(TTopicClient& client)
        {
            Cerr << "=== Write message" << Endl;

            auto writeSession = client.CreateSimpleBlockingWriteSession(CreateWriteSessionSettings());
            UNIT_ASSERT(writeSession->Write("message"));
            writeSession->Close();
        }

        void ReadMessage(TTopicClient& client, ui64 expectedCommitedOffset = 1)
        {
            Cerr << "=== Read message" << Endl;

            auto readSession = client.CreateReadSession(CreateReadSessionSettings());

            TMaybe<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
            UNIT_ASSERT(event);
            auto startPartitionSession = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(event.Get());
            UNIT_ASSERT_C(startPartitionSession, DebugString(*event));

            startPartitionSession->Confirm();

            event = readSession->GetEvent(true);
            UNIT_ASSERT(event);
            auto dataReceived = std::get_if<TReadSessionEvent::TDataReceivedEvent>(event.Get());
            UNIT_ASSERT_C(dataReceived, DebugString(*event));

            dataReceived->Commit();

            auto& messages = dataReceived->GetMessages();
            UNIT_ASSERT(messages.size() == 1);
            UNIT_ASSERT(messages[0].GetData() == "message");

            event = readSession->GetEvent(true);
            UNIT_ASSERT(event);
            auto commitOffsetAck = std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(event.Get());
            UNIT_ASSERT_C(commitOffsetAck, DebugString(*event));
            UNIT_ASSERT_VALUES_EQUAL(commitOffsetAck->GetCommittedOffset(), expectedCommitedOffset);
        }

        template <class TService>
        std::unique_ptr<grpc::Server> StartGrpcServer(const TString& address, TService& service) {
            grpc::ServerBuilder builder;
            builder.AddListeningPort(address, grpc::InsecureServerCredentials());
            builder.RegisterService(&service);
            return builder.BuildAndStart();
        }

        class TMockDiscoveryService: public Ydb::Discovery::V1::DiscoveryService::Service {
        public:
            TMockDiscoveryService()
            {
                ui16 discoveryPort = TPortManager().GetPort();
                DiscoveryAddr = TStringBuilder() << "0.0.0.0:" << discoveryPort;
                Cerr << "==== TMockDiscovery server started on port " << discoveryPort << Endl;
                Server = ::NYdb::NTopic::NTests::NTestSuiteLocalPartition::StartGrpcServer(DiscoveryAddr, *this);
            }

            void SetGoodEndpoints(TTopicSdkTestSetup& setup)
            {
                std::lock_guard lock(Lock);
                Cerr << "=== TMockDiscovery set good endpoint nodes " << Endl;
                SetEndpointsLocked(setup.GetRuntime().GetNodeId(0), setup.GetRuntime().GetNodeCount(), setup.GetServer().GrpcPort);
            }

            // Call this method only after locking the Lock.
            void SetEndpointsLocked(ui32 firstNodeId, ui32 nodeCount, ui16 port)
            {
                Cerr << "==== TMockDiscovery add endpoints, firstNodeId " << firstNodeId << ", nodeCount " << nodeCount << ", port " << port << Endl;

                MockResults.clear_endpoints();
                if (nodeCount > 0)
                {
                    Ydb::Discovery::EndpointInfo* endpoint = MockResults.add_endpoints();
                    endpoint->set_address(TStringBuilder() << "localhost");
                    endpoint->set_port(port);
                    endpoint->set_node_id(firstNodeId);
                }
                if (nodeCount > 1)
                {
                    Ydb::Discovery::EndpointInfo* endpoint = MockResults.add_endpoints();
                    endpoint->set_address(TStringBuilder() << "ip6-localhost"); // name should be different
                    endpoint->set_port(port);
                    endpoint->set_node_id(firstNodeId + 1);
                }
                if (nodeCount > 2) {
                    UNIT_FAIL("Unsupported count of nodes");
                }
            }

            void SetEndpoints(ui32 firstNodeId, ui32 nodeCount, ui16 port)
            {
                std::lock_guard lock(Lock);
                SetEndpointsLocked(firstNodeId, nodeCount, port);
            }

            grpc::Status ListEndpoints(grpc::ServerContext* context, const Ydb::Discovery::ListEndpointsRequest* request, Ydb::Discovery::ListEndpointsResponse* response) override {
                std::lock_guard lock(Lock);
                UNIT_ASSERT(context);

                if (Delay)
                {
                    Cerr << "==== Delay " << Delay << " before ListEndpoints request" << Endl;
                    TInstant start = TInstant::Now();
                    while (start + Delay < TInstant::Now())
                    {
                        if (context->IsCancelled())
                            return grpc::Status::CANCELLED;
                        Sleep(TDuration::MilliSeconds(100));
                    }
                }

                Cerr << " ==== ListEndpoints request: " << request->ShortDebugString() << Endl;

                auto* op = response->mutable_operation();
                op->set_ready(true);
                op->set_status(Ydb::StatusIds::SUCCESS);
                op->mutable_result()->PackFrom(MockResults);

                Cerr << "==== ListEndpoints response: " << response->ShortDebugString() << Endl;

                return grpc::Status::OK;
            }

            TString GetDiscoveryAddr() const {
                return DiscoveryAddr;
            }

            void SetDelay(TDuration delay) {
                Delay = delay;
            }

        private:
            Ydb::Discovery::ListEndpointsResult MockResults;
            TString DiscoveryAddr = 0;
            std::unique_ptr<grpc::Server> Server;
            TAdaptiveLock Lock;

            TDuration Delay = {};
        };

        auto Start(TString testCaseName, std::shared_ptr<TMockDiscoveryService> mockDiscoveryService = {})
        {
            struct Result {
                std::shared_ptr<TTopicSdkTestSetup> Setup;
                std::shared_ptr<TTopicClient> Client;
                std::shared_ptr<TMockDiscoveryService> MockDiscoveryService;
            };

            auto setup = CreateSetup(testCaseName);

            if (!mockDiscoveryService)
            {
                mockDiscoveryService = std::make_shared<TMockDiscoveryService>();
                mockDiscoveryService->SetGoodEndpoints(*setup);
            }

            TDriver driver(CreateConfig(*setup, mockDiscoveryService->GetDiscoveryAddr()));

            auto client = std::make_shared<TTopicClient>(driver);

            return Result{setup, client, mockDiscoveryService};
        }

        Y_UNIT_TEST(Basic) {
            auto [setup, client, discovery] = Start(TEST_CASE_NAME);

            WriteMessage(*client);
            ReadMessage(*client);
        }

        Y_UNIT_TEST(Restarts) {
            auto [setup, client, discovery] = Start(TEST_CASE_NAME);

            for (size_t i = 1; i <= 10; ++i) {
                Cerr << "=== Restart attempt " << i << Endl;
                setup->GetServer().KillTopicPqTablets(setup->GetTopicPath());
                WriteMessage(*client);
                ReadMessage(*client, i);
            }
        }

        Y_UNIT_TEST(DescribeBadPartition) {
            auto setup = CreateSetup(TEST_CASE_NAME);


            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(*setup);

            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

            // Set non-existing partition
            auto writeSettings = CreateWriteSessionSettings();
            writeSettings.RetryPolicy(retryPolicy);
            writeSettings.PartitionId(1);

            retryPolicy->Initialize();
            retryPolicy->ExpectBreakDown();

            Cerr << "=== Create write session\n";
            TTopicClient client(TDriver(CreateConfig(*setup, discovery.GetDiscoveryAddr())));
            auto writeSession = client.CreateWriteSession(writeSettings);

            Cerr << "=== Wait for retries\n";
            retryPolicy->WaitForRetriesSync(3);

            Cerr << "=== Alter partition count\n";
            TAlterTopicSettings alterSettings;
            alterSettings.AlterPartitioningSettings(2, 2);
            auto alterResult = client.AlterTopic(setup->GetTopicPath(), alterSettings).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(alterResult.GetStatus(), NYdb::EStatus::SUCCESS, alterResult.GetIssues().ToString());

            Cerr << "=== Wait for repair\n";
            retryPolicy->WaitForRepairSync();

            Cerr << "=== Close write session\n";
            writeSession->Close();
        }

        Y_UNIT_TEST(DiscoveryServiceBadPort) {
            auto setup = CreateSetup(TEST_CASE_NAME);

            TMockDiscoveryService discovery;
            discovery.SetEndpoints(9999, 2, 0);

            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

            auto writeSettings = CreateWriteSessionSettings();
            writeSettings.RetryPolicy(retryPolicy);

            retryPolicy->Initialize();
            retryPolicy->ExpectBreakDown();

            Cerr << "=== Create write session\n";
            TTopicClient client(TDriver(CreateConfig(*setup, discovery.GetDiscoveryAddr())));
            auto writeSession = client.CreateWriteSession(writeSettings);

            Cerr << "=== Wait for retries\n";
            retryPolicy->WaitForRetriesSync(3);

            discovery.SetGoodEndpoints(*setup);

            Cerr << "=== Wait for repair\n";
            retryPolicy->WaitForRepairSync();

            Cerr << "=== Close write session\n";
            writeSession->Close();
        }

        Y_UNIT_TEST(DiscoveryServiceBadNodeId) {
            auto setup = CreateSetup(TEST_CASE_NAME);

            TMockDiscoveryService discovery;
            discovery.SetEndpoints(9999, setup->GetRuntime().GetNodeCount(), setup->GetServer().GrpcPort);

            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();

            auto writeSettings = CreateWriteSessionSettings();
            writeSettings.RetryPolicy(retryPolicy);

            retryPolicy->Initialize();
            retryPolicy->ExpectBreakDown();

            Cerr << "=== Create write session\n";
            TTopicClient client(TDriver(CreateConfig(*setup, discovery.GetDiscoveryAddr())));
            auto writeSession = client.CreateWriteSession(writeSettings);

            Cerr << "=== Wait for retries\n";
            retryPolicy->WaitForRetriesSync(3);

            discovery.SetGoodEndpoints(*setup);

            Cerr << "=== Wait for repair\n";
            retryPolicy->WaitForRepairSync();

            Cerr << "=== Close write session\n";
            writeSession->Close();
        }

        Y_UNIT_TEST(DescribeHang) {
            auto setup = CreateSetup(TEST_CASE_NAME);

            TMockDiscoveryService discovery;
            discovery.SetEndpoints(9999, 2, 0);

            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>(TDuration::Days(1));

            auto writeSettings = CreateWriteSessionSettings();
            writeSettings.RetryPolicy(retryPolicy);

            retryPolicy->Initialize();
            retryPolicy->ExpectBreakDown();

            Cerr << "=== Create write session\n";
            TTopicClient client(TDriver(CreateConfig(*setup, discovery.GetDiscoveryAddr())));
            auto writeSession = client.CreateWriteSession(writeSettings);

            Cerr << "=== Close write session\n";
            writeSession->Close();
        }

        Y_UNIT_TEST(DiscoveryHang) {
            auto setup = CreateSetup(TEST_CASE_NAME);

            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(*setup);
            discovery.SetDelay(TDuration::Days(1));

            Cerr << "=== Create write session\n";
            TTopicClient client(TDriver(CreateConfig(*setup, discovery.GetDiscoveryAddr())));
            auto writeSession = client.CreateWriteSession(CreateWriteSessionSettings());

            Cerr << "=== Close write session\n";
            writeSession->Close();
        }

        Y_UNIT_TEST(WithoutPartition) {
            // Direct write without partition: happy way.
            auto setup = CreateSetup(TEST_CASE_NAME);
            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(*setup);
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}));
            TDriver driver(driverConfig);
            TTopicClient client(driver);
            auto sessionSettings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .ProducerId(TEST_MESSAGE_GROUP_ID)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(true);
            auto writeSession = client.CreateSimpleBlockingWriteSession(sessionSettings);
            UNIT_ASSERT(writeSession->Write("message"));
            writeSession->Close();

            auto node0_id = std::to_string(setup->GetRuntime().GetNodeId(0));
            TExpectedTrace expected{
                "InitRequest !partition_id !partition_with_generation",
                "InitResponse partition_id=0 session_id",
                "DescribePartitionRequest partition_id=0",
                std::format("DescribePartitionResponse partition_id=0 pl_generation=1 pl_node_id={}", node0_id),
                std::format("PreferredPartitionLocation Generation=1 NodeId={}", node0_id),
                "InitRequest !partition_id pwg_partition_id=0 pwg_generation=1",
                "InitResponse partition_id=0 session_id",
            };
            auto const events = tracingBackend->GetEvents();
            UNIT_ASSERT(expected.Matches(events));
        }

        Y_UNIT_TEST(WithoutPartitionWithRestart) {
            // Direct write without partition: with tablet restart.
            auto setup = CreateSetup(TEST_CASE_NAME);
            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(*setup);
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}));
            TDriver driver(driverConfig);
            TTopicClient client(driver);

            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
            auto gotAck = NThreading::NewPromise();
            auto gotToken = NThreading::NewPromise();
            TMaybe<TContinuationToken> token;
            auto writerSettings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(true)
                .RetryPolicy(retryPolicy);
            writerSettings.EventHandlers_
                .ReadyToAcceptHandler([&](TWriteSessionEvent::TReadyToAcceptEvent& ev) {
                    token = std::move(ev.ContinuationToken);
                    if (!gotToken.HasValue()) {
                        gotToken.SetValue();
                    }
                })
                .AcksHandler([&](TWriteSessionEvent::TAcksEvent&) {
                    gotAck.SetValue();
                });

            retryPolicy->Initialize();
            retryPolicy->ExpectBreakDown();
            auto writer = client.CreateWriteSession(writerSettings);
            gotToken.GetFuture().Wait();
            writer->Write(std::move(*token), "message");
            gotAck.GetFuture().Wait();
            auto repaired = NThreading::NewPromise();
            retryPolicy->WaitForRepair(repaired);
            setup->GetServer().KillTopicPqTablets(setup->GetTopicPath());
            repaired.GetFuture().Wait();
            writer->Close();

            auto node0_id = std::to_string(setup->GetRuntime().GetNodeId(0));
            TExpectedTrace expected{
                "InitRequest !partition_id !partition_with_generation",
                "InitResponse partition_id=0 session_id",
                "DescribePartitionRequest partition_id=0",
                "DescribePartitionResponse partition_id=0 pl_generation=1 pl_node_id=" + node0_id,
                "PreferredPartitionLocation Generation=1 NodeId=" + node0_id,
                "InitRequest !partition_id pwg_partition_id=0 pwg_generation=1",
                "InitResponse partition_id=0 session_id",
                "Error status=UNAVAILABLE",

                // The tablet has been killed, find out the node the partition tablet ends up on.
                "DescribePartitionRequest partition_id=0",
                "DescribePartitionResponse partition_id=0 pl_generation=2 pl_node_id=" + node0_id,
                "PreferredPartitionLocation Generation=2 NodeId=" + node0_id,
                "InitRequest !partition_id pwg_partition_id=0 pwg_generation=2",
                "InitResponse partition_id=0 session_id",
            };
            auto const events = tracingBackend->GetEvents();
            UNIT_ASSERT(expected.Matches(events));
        }

        Y_UNIT_TEST(WithoutPartitionUnknownEndpoint) {
            // Direct write without partition: with unknown endpoint.
            // Create 2 nodes. Mark the node 1 as down, so the topic partition will be assigned to the node 2.
            // Our write session then gets assigned to the partition which lives on the node 2.
            // But at first we only add node 1 to the discovery service.
            auto setup = CreateSetup(TEST_CASE_NAME, 2, /* createTopic = */ false);
            setup->GetServer().AnnoyingClient->MarkNodeInHive(setup->GetServer().GetRuntime(), 0, false);
            setup->CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1);
            setup->GetServer().AnnoyingClient->MarkNodeInHive(setup->GetServer().GetRuntime(), 0, true);
            TMockDiscoveryService discovery;
            discovery.SetEndpoints(setup->GetRuntime().GetNodeId(0), 1, setup->GetServer().GrpcPort);
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}));
            TDriver driver(driverConfig);
            TTopicClient client(driver);
            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
            auto sessionSettings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(true)
                .RetryPolicy(retryPolicy);
            retryPolicy->Initialize();
            retryPolicy->ExpectBreakDown();
            auto writeSession = client.CreateSimpleBlockingWriteSession(sessionSettings);

            retryPolicy->WaitForRetriesSync(1);
            discovery.SetGoodEndpoints(*setup);
            tracingBackend->WaitForEvent("InitRequest").GetValueSync();
            UNIT_ASSERT(writeSession->Close());

            auto node1_id = std::to_string(setup->GetRuntime().GetNodeId(1));
            TExpectedTrace expected{
                "InitRequest !partition_id !partition_with_generation",
                "InitResponse partition_id=0",
                "DescribePartitionRequest partition_id=0",
                std::format("DescribePartitionResponse partition_id=0 pl_generation=1 pl_node_id={}", node1_id),
                "Error status=UNAVAILABLE",
                "DescribePartitionRequest partition_id=0",
                std::format("DescribePartitionResponse partition_id=0 pl_generation=1 pl_node_id={}", node1_id),
                std::format("PreferredPartitionLocation Generation=1 NodeId={}", node1_id),
                "InitRequest !partition_id pwg_partition_id=0 pwg_generation=1",

                // At this point we get an UNAVAILABLE error, as the InitRequest is made for the node 2, but received and processed by node 1.
                // It's expected, as our test server starts only one gRPC proxy and all requests go through the node 1. Error looks like this:
                // TPartitionWriter <some-id> (partition=0) received TEvClientConnected with wrong NodeId. Expected: 1, received 2
            };
            auto const events((tracingBackend)->GetEvents());
            UNIT_ASSERT(expected.Matches(events));
        }

        Y_UNIT_TEST(WithoutPartitionDeadNode) {
            // This test emulates a situation, when InitResponse directs us to an inaccessible node.
            auto setup = CreateSetup(TEST_CASE_NAME);
            TMockDiscoveryService discovery;
            discovery.SetEndpoints(setup->GetRuntime().GetNodeId(0), 1, 0);
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}));
            TDriver driver(driverConfig);
            TTopicClient client(driver);
            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
            auto sessionSettings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(true)
                .PartitionId(0)
                .RetryPolicy(retryPolicy);
            retryPolicy->Initialize();
            retryPolicy->ExpectBreakDown();
            auto writeSession = client.CreateSimpleBlockingWriteSession(sessionSettings);

            retryPolicy->WaitForRetriesSync(1);
            discovery.SetGoodEndpoints(*setup);
            retryPolicy->WaitForRepairSync();
            UNIT_ASSERT(writeSession->Close());

            auto node0_id = std::to_string(setup->GetRuntime().GetNodeId(0));
            TExpectedTrace expected{
                "DescribePartitionRequest partition_id=0",
                "Error status=TRANSPORT_UNAVAILABLE",
                "DescribePartitionRequest partition_id=0",
                std::format("DescribePartitionResponse partition_id=0 pl_generation=1 pl_node_id={}", node0_id),
                std::format("PreferredPartitionLocation Generation=1 NodeId={}", node0_id),
                "InitRequest !partition_id pwg_partition_id=0 pwg_generation=1",
                "InitResponse partition_id=0",
            };
            auto const events = tracingBackend->GetEvents();
            UNIT_ASSERT(expected.Matches(events));
        }

        Y_UNIT_TEST(WithoutPartitionPartitionRelocation) {
            // This test emulates partition relocation from one node to another.
            auto setup = CreateSetup(TEST_CASE_NAME, 2, /* createTopic = */ false);
            // Make the node 1 unavailable.
            setup->GetServer().AnnoyingClient->MarkNodeInHive(setup->GetServer().GetRuntime(), 0, false);
            setup->CreateTopic(TEST_TOPIC, TEST_CONSUMER, 2);

            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(*setup);
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}));
            TDriver driver(driverConfig);
            TTopicClient client(driver);
            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
            auto sessionSettings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(true)
                .RetryPolicy(retryPolicy);

            retryPolicy->Initialize();
            retryPolicy->ExpectBreakDown();

            // Connect to node 2.
            auto writeSession = client.CreateSimpleBlockingWriteSession(sessionSettings);
            retryPolicy->WaitForRetriesSync(2);
            // Make the node 2 unavailable.
            setup->GetServer().AnnoyingClient->MarkNodeInHive(setup->GetServer().GetRuntime(), 1, false);
            setup->GetServer().AnnoyingClient->KickNodeInHive(setup->GetServer().GetRuntime(), 1);
            // Bring back the node 1.
            setup->GetServer().AnnoyingClient->MarkNodeInHive(setup->GetServer().GetRuntime(), 0, true);
            retryPolicy->WaitForRepairSync();
            writeSession->Close();

            auto node0_id = std::to_string(setup->GetRuntime().GetNodeId(0));
            auto node1_id = std::to_string(setup->GetRuntime().GetNodeId(1));
            TExpectedTrace expected{
                "InitRequest !partition_id !partition_with_generation",
                "InitResponse partition_id=1 session_id",
                "DescribePartitionRequest partition_id=1",
                std::format("DescribePartitionResponse partition_id=1 pl_generation=1 pl_node_id={}", node1_id),
                std::format("PreferredPartitionLocation Generation=1 NodeId={}", node1_id),
                "InitRequest !partition_id pwg_partition_id=1 pwg_generation=1",
                "Error status=UNAVAILABLE",
                "DescribePartitionRequest partition_id=1",
                std::format("DescribePartitionResponse partition_id=1 pl_generation=2 pl_node_id={}", node0_id),
                std::format("PreferredPartitionLocation Generation=2 NodeId={}", node0_id),
                "InitRequest !partition_id pwg_partition_id=1 pwg_generation=2",
                "InitResponse partition_id=1",
            };
            auto const events = tracingBackend->GetEvents();
            UNIT_ASSERT(expected.Matches(events));
        }

        Y_UNIT_TEST(DirectWriteWithoutDescribeResourcesPermission) {
            // The DirectWrite option makes the write session send a DescribePartitionRequest to locate the partition's node.
            // Previously, it required DescribeSchema (DescribeResources) permission. However, this permission is too broad
            // to be granted to anyone who needs the DirectWrite option. The DescribePartitionRequest should work when either
            // UpdateRow (WriteTopic) or DescribeSchema permission is granted.
            //
            // In this test, we don't grant DescribeSchema permission and check that direct write works anyway.

            auto setup = CreateSetup(TEST_CASE_NAME);
            auto authToken = "x-user-x@builtin";

            {
                // Allow UpdateRow only, no DescribeSchema permission.
                NACLib::TDiffACL acl;
                acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, authToken);
                setup->GetServer().AnnoyingClient->ModifyACL("/Root", TEST_TOPIC, acl.SerializeAsString());
            }

            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(*setup);
            auto* tracingBackend = new TTracingBackend();
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr())
                .SetLog(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}))
                .SetAuthToken(authToken);
            TDriver driver(driverConfig);
            TTopicClient client(driver);

            auto sessionSettings = TWriteSessionSettings()
                .Path(TEST_TOPIC)
                .ProducerId(TEST_MESSAGE_GROUP_ID)
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(true);

            auto writeSession = client.CreateSimpleBlockingWriteSession(sessionSettings);
            UNIT_ASSERT(writeSession->Write("message"));
            writeSession->Close();

            TExpectedTrace expected{
                "InitRequest",
                "InitResponse partition_id=0",
                "DescribePartitionRequest partition_id=0",
                "DescribePartitionResponse partition_id=0 pl_generation=1",
                "PreferredPartitionLocation Generation=1",
                "InitRequest pwg_partition_id=0 pwg_generation=1",
                "InitResponse partition_id=0",
            };
            auto const events = tracingBackend->GetEvents();
            UNIT_ASSERT(expected.Matches(events));
        }

        Y_UNIT_TEST(WithoutPartitionWithSplit) {
            auto setup = CreateSetupForSplitMerge(TEST_CASE_NAME);
            setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1, 100);

            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(setup);
            auto driverConfig = CreateConfig(setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}));
            TDriver driver(driverConfig);
            TTopicClient client(driver);
            auto writeSettings = TWriteSessionSettings()
                                .Path(TEST_TOPIC)
                                .ProducerId(TEST_MESSAGE_GROUP_ID)
                                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                                .DirectWriteToPartition(true);
            auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
            TTestReadSession ReadSession("Session-0", client, 2);

            UNIT_ASSERT(writeSession->Write(Msg("message_1.1", 2)));

            ui64 txId = 1006;
            SplitPartition(setup, ++txId, 0, "a");

            UNIT_ASSERT(writeSession->Write(Msg("message_1.2", 3)));

            ReadSession.WaitAllMessages();

            for (const auto& info : ReadSession.Impl->ReceivedMessages) {
                if (info.Data == "message_1.1") {
                    UNIT_ASSERT_EQUAL(0, info.PartitionId);
                    UNIT_ASSERT_EQUAL(2, info.SeqNo);
                } else if (info.Data == "message_1.2") {
                    UNIT_ASSERT(1 == info.PartitionId || 2 == info.PartitionId);
                    UNIT_ASSERT_EQUAL(3, info.SeqNo);
                } else {
                    UNIT_ASSERT_C(false, "Unexpected message: " << info.Data);
                }
            }

            writeSession->Close(TDuration::Seconds(1));

            auto node0_id = std::to_string(setup.GetRuntime().GetNodeId(0));
            TExpectedTrace expected{
                "InitRequest !partition_id !partition_with_generation",
                "InitResponse partition_id=0",
                "DescribePartitionRequest partition_id=0",
                std::format("DescribePartitionResponse partition_id=0 pl_generation=1 pl_node_id={}", node0_id),
                std::format("PreferredPartitionLocation Generation=1 NodeId={}", node0_id),
                "InitRequest !partition_id pwg_partition_id=0 pwg_generation=1",
                "InitResponse partition_id=0",
                "Error status=OVERLOADED",
                "ClearDirectWriteToPartitionId",
                "InitRequest !partition_id !partition_with_generation",
                "InitResponse partition_id=2",
                "DescribePartitionRequest partition_id=2",
                std::format("DescribePartitionResponse partition_id=2 pl_generation=1 pl_node_id={}", node0_id),
                std::format("PreferredPartitionLocation Generation=1 NodeId={}", node0_id),
                "InitRequest !partition_id pwg_partition_id=2 pwg_generation=1",
                "InitResponse partition_id=2",
            };
            auto const events = tracingBackend->GetEvents();
            UNIT_ASSERT(expected.Matches(events));

            ReadSession.Close();
        }
    }
}
