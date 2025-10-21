#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/ut/ut_utils/ut_utils.h>

#include <ydb/public/sdk/cpp/src/client/topic/common/trace_lazy.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>
#include <ydb/public/sdk/cpp/tests/integration/topic/utils/trace.h>

#include <ydb/public/sdk/cpp/src/client/persqueue_public/persqueue.h>

#include <ydb/public/sdk/cpp/src/client/topic/impl/common.h>
#include <ydb/public/sdk/cpp/src/client/persqueue_public/impl/write_session.h>

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/local_partition.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <format>


using namespace NYdb::NPersQueue::NTests;

namespace NYdb::inline Dev::NTopic::NTests {

    Y_UNIT_TEST_SUITE(LocalPartition) {
        std::shared_ptr<TTopicSdkTestSetup> CreateSetup(const std::string& testCaseName, std::uint32_t nodeCount = 1, bool createTopic = true) {
            NKikimr::Tests::TServerSettings settings = TTopicSdkTestSetup::MakeServerSettings();
            settings.SetNodeCount(nodeCount);
            return std::make_shared<TTopicSdkTestSetup>(testCaseName, settings, createTopic);
        }

        TTopicSdkTestSetup CreateSetupForSplitMerge(const std::string& testCaseName) {
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

        auto Start(const std::string& testCaseName, std::shared_ptr<TMockDiscoveryService> mockDiscoveryService = {})
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

        Y_UNIT_TEST(Restarts) {
            auto [setup, client, discovery] = Start(TEST_CASE_NAME);

            for (size_t i = 1; i <= 10; ++i) {
                std::cerr << "=== Restart attempt " << i << std::endl;
                setup->GetServer().KillTopicPqTablets(setup->GetFullTopicPath());
                WriteMessage(*setup, *client);
                ReadMessage(*setup, *client, i);
            }
        }

        Y_UNIT_TEST(WithoutPartitionWithRestart) {
            // Direct write without partition: with tablet restart.
            auto setup = CreateSetup(TEST_CASE_NAME);
            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(*setup);
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(std::unique_ptr<TLogBackend>(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}).Release()));
            TDriver driver(driverConfig);
            TTopicClient client(driver);
            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
            auto sessionSettings = TWriteSessionSettings()
                .Path(setup->GetTopicPath())
                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                .DirectWriteToPartition(true)
                .RetryPolicy(retryPolicy);

            retryPolicy->Initialize();
            retryPolicy->ExpectBreakDown();
            auto writeSession = client.CreateSimpleBlockingWriteSession(sessionSettings);
            UNIT_ASSERT(writeSession->Write("message"));
            setup->GetServer().KillTopicPqTablets(setup->GetFullTopicPath());
            retryPolicy->WaitForRepairSync();
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
                "Error status=UNAVAILABLE",

                // The tablet has been killed, find out the partition node the tablet ends up.
                "DescribePartitionRequest partition_id=0",
                std::format("DescribePartitionResponse partition_id=0 pl_generation=2 pl_node_id={}", node0_id),
                std::format("PreferredPartitionLocation Generation=2 NodeId={}", node0_id),
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
            setup->CreateTopic(setup->GetTopicPath(), setup->GetConsumerName(), 1);
            setup->GetServer().AnnoyingClient->MarkNodeInHive(setup->GetServer().GetRuntime(), 0, true);
            TMockDiscoveryService discovery;
            discovery.SetEndpoints(setup->GetRuntime().GetNodeId(0), 1, setup->GetServer().GrpcPort);
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(std::unique_ptr<TLogBackend>(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}).Release()));
            TDriver driver(driverConfig);
            TTopicClient client(driver);
            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
            auto sessionSettings = TWriteSessionSettings()
                .Path(setup->GetTopicPath())
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

        Y_UNIT_TEST(WithoutPartitionPartitionRelocation) {
            // This test emulates partition relocation from one node to another.
            auto setup = CreateSetup(TEST_CASE_NAME, 2, /* createTopic = */ false);
            // Make the node 1 unavailable.
            setup->GetServer().AnnoyingClient->MarkNodeInHive(setup->GetServer().GetRuntime(), 0, false);
            setup->CreateTopic(setup->GetTopicPath(), setup->GetConsumerName(), 2);

            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(*setup);
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(std::unique_ptr<TLogBackend>(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}).Release()));
            TDriver driver(driverConfig);
            TTopicClient client(driver);
            auto retryPolicy = std::make_shared<TYdbPqTestRetryPolicy>();
            auto sessionSettings = TWriteSessionSettings()
                .Path(setup->GetTopicPath())
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

            setup->GetServer().AnnoyingClient->GrantConnect(authToken);

            {
                // Allow UpdateRow only, no DescribeSchema permission.
                NACLib::TDiffACL acl;
                acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, authToken);
                setup->GetServer().AnnoyingClient->ModifyACL("/Root", setup->GetTopicPath(), acl.SerializeAsString());
            }

            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(*setup);
            auto* tracingBackend = new TTracingBackend();
            auto driverConfig = CreateConfig(*setup, discovery.GetDiscoveryAddr())
                .SetLog(std::unique_ptr<TLogBackend>(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}).Release()))
                .SetAuthToken(authToken);
            TDriver driver(driverConfig);
            TTopicClient client(driver);

            auto sessionSettings = TWriteSessionSettings()
                .Path(setup->GetTopicPath())
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
            setup.CreateTopic(setup.GetTopicPath(), setup.GetConsumerName(), 1, 100);

            TMockDiscoveryService discovery;
            discovery.SetGoodEndpoints(setup);
            auto driverConfig = CreateConfig(setup, discovery.GetDiscoveryAddr());
            auto* tracingBackend = new TTracingBackend();
            driverConfig.SetLog(std::unique_ptr<TLogBackend>(CreateCompositeLogBackend({new TStreamLogBackend(&Cerr), tracingBackend}).Release()));
            TDriver driver(driverConfig);
            TTopicClient client(driver);
            auto writeSettings = TWriteSessionSettings()
                                .Path(setup.GetTopicPath())
                                .ProducerId(TEST_MESSAGE_GROUP_ID)
                                .MessageGroupId(TEST_MESSAGE_GROUP_ID)
                                .DirectWriteToPartition(true);
            auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
            auto ReadSession = NPQ::NTest::CreateTestReadSession({ .Name="Session-0", .Setup=setup, .Sdk = NPQ::NTest::SdkVersion::Topic, .ExpectedMessagesCount = 2 });


            UNIT_ASSERT(writeSession->Write(NPQ::NTest::Msg("message_1.1", 2)));

            ui64 txId = 1006;
            NPQ::NTest::SplitPartition(setup, ++txId, 0, "a");

            UNIT_ASSERT(writeSession->Write(NPQ::NTest::Msg("message_1.2", 3)));

            ReadSession->WaitAllMessages();

            for (const auto& info : ReadSession->GetReceivedMessages()) {
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

            ReadSession->Close();
        }
    }
}
