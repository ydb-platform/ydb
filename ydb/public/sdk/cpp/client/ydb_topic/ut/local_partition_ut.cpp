#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>

#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/write_session.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <future>

using namespace NYdb;
using namespace NYdb::NPersQueue::NTests;

namespace NYdb::NTopic::NTests {

    Y_UNIT_TEST_SUITE(LocalPartition) {
        std::shared_ptr<TTopicSdkTestSetup> CreateSetup(const TString& testCaseName, ui32 nodeCount = 1) {
            NKikimr::Tests::TServerSettings settings = TTopicSdkTestSetup::MakeServerSettings();
            settings.SetNodeCount(nodeCount);
            return std::make_shared<TTopicSdkTestSetup>(testCaseName, settings);
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
                Cerr << "=== TMockDiscovery set good endpoint nodes " << Endl;
                SetEndpoints(setup.GetRuntime().GetNodeId(0), setup.GetRuntime().GetNodeCount(), setup.GetServer().GrpcPort);
            }

            void SetEndpoints(ui32 firstNodeId, ui32 nodeCount, ui16 port)
            {
                std::lock_guard lock(Lock);

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

                Cerr << "==== ListEndpoints request: " << request->ShortDebugString() << Endl;

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
    }
}