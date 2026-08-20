#include <ydb/tests/functional/federation_test/common_functions.h>
#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/grpc/include/grpcpp/grpcpp.h>


using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NFederationTests;

const TString prodDatabasePath  = "/Root/logbroker-federation/prod";
const TString testDatabasePath  = "/Root/logbroker-federation/test";
const TString prodTopicPath = "topic";
const TString testTopicPath = "topic";
const TString gapTopicPath = "gap-topic";
const TString gapTopicCMPath = "/prod/gap-topic";
const TString consumerName  = "consumer";



struct TClusterEndpoints {
    TClusterEndpoints() {
        const TString cmPort = std::getenv("CM_PORT");
        const TString portA = std::getenv("cluster_a_port");
        const TString portB = std::getenv("cluster_b_port");
        UNIT_ASSERT_C(cmPort, "CM_PORT is not set");
        UNIT_ASSERT_C(portA, "cluster_a_port is not set by federation_recipe");
        UNIT_ASSERT_C(portB, "cluster_b_port is not set by federation_recipe");
        EndpointA = TStringBuilder() << "localhost:" << portA;
        EndpointB = TStringBuilder() << "localhost:" << portB;
        EndpointCM = TStringBuilder() << "localhost:" << cmPort;
    }

    TString EndpointA;
    TString EndpointB;
    TString EndpointCM;
};

Y_UNIT_TEST_SUITE(TFederationWriteReadTest) {

    Y_UNIT_TEST(WriteAndReadOnClusterA) {
        TClusterEndpoints env;
        TString writtenMessage = "hello from cluster_a";
        WriteMessages(env.EndpointA, prodDatabasePath, prodTopicPath,
                      "ut-producer-a", {writtenMessage});

        TDriver driver = MakeDriver(env.EndpointA, prodDatabasePath);
        TTopicClient client(driver);

        std::map<ui64, TString> messages;
        {
            TTopicClient client(driver);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(consumerName)
                    .AppendTopics(TTopicReadSettings(prodTopicPath))
            );
            messages = ReadMessages(session, 1);
            session->Close(TDuration::Seconds(5));
        }
        driver.Stop(true);
        UNIT_ASSERT_EQUAL(writtenMessage, messages[0]);
    }

    Y_UNIT_TEST(SimpleRemoteMirrorRuleWorks) {
        TClusterEndpoints env;

        auto channel = grpc::CreateChannel(
            env.EndpointCM,
            grpc::InsecureChannelCredentials()
        );
        auto stub = NLogBroker::NAdmin::ConfigurationManagerAdminService::NewStub(channel);

        NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
        req.set_comment("unittest: create remote mirror rule");

        auto* action = req.add_actions();
        auto* mirror = action->mutable_create_remote_mirror_rule();

        mirror->mutable_remote_mirror_rule()->mutable_topic()->set_path("test/topic");
        mirror->mutable_remote_mirror_rule()->mutable_cluster()->set_cluster("cluster_b");

        auto* props = mirror->mutable_properties();
        props->mutable_src_cluster_endpoint()->set_user_defined(env.EndpointA);
        props->mutable_src_database()->set_user_defined("/Root/logbroker-federation/test");
        props->mutable_src_topic()->set_user_defined("topic");
        props->mutable_src_consumer()->set_user_defined("consumer");
        props->mutable_credentials()->set_oauth_token("root@builtin");

        Cerr << TInstant::Now() << " Executing command" << Endl;

        NLogBroker::ExecuteModifyCommandsResponse resp;
        {
            grpc::ClientContext ctx;
            auto status = stub->ExecuteModifyCommands(&ctx, req, &resp);
            UNIT_ASSERT_C(status.ok(), status.error_message());
        }
        Cerr << TInstant::Now() << " waiting for cm operation" << Endl;
        auto op = WaitOperation(*stub, resp.operation());
        UNIT_ASSERT_C(op.ready(), "operation never became ready");
        UNIT_ASSERT_VALUES_EQUAL_C((int)op.status(), (int)NLogBroker::StatusIds::SUCCESS, "CM returned non-success status");

        const std::vector<TString> written = {"mirror-msg-0", "mirror-msg-1", "mirror-msg-2"};
        WriteMessages(env.EndpointA, testDatabasePath, testTopicPath, "mirror-producer", written);

        TDriver driverB = MakeDriver(env.EndpointB, testDatabasePath);
        std::map<ui64, TString> received;
        {
            TTopicClient client(driverB);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(consumerName)
                    .AppendTopics(TTopicReadSettings(testTopicPath))
            );
            received = ReadMessages(session, written.size(), TDuration::Seconds(60));
            session->Close(TDuration::Seconds(5));
        }
        driverB.Stop(true);

        UNIT_ASSERT_VALUES_EQUAL_C(received.size(), written.size(),
            "Expected " + std::to_string(written.size()) + " mirrored messages on cluster_b, got " + std::to_string(received.size()));

        for (size_t i = 0; i < written.size(); i++) {
            UNIT_ASSERT_EQUAL_C(written[i], received[i], TStringBuilder() << "written=" << written[i] << ", received=" << received[i]);
        }
    }

    Y_UNIT_TEST(CreateRemoteMirrorRuleWithGaps) {
        TClusterEndpoints env;

        const TString kSrcTopicCmPath = "prod/gaps-src-topic";
        const TString kSrcTopicYdbPath = "gaps-src-topic";

        const TString kDstTopicCmPath = "prod/gaps-dst-topic";
        const TString kDstTopicYdbPath = "gaps-dst-topic";

        auto channel = grpc::CreateChannel(
            env.EndpointCM,
            grpc::InsecureChannelCredentials()
        );
        auto stub = NLogBroker::NAdmin::ConfigurationManagerAdminService::NewStub(channel);

        CmCreateTopic(*stub, kSrcTopicCmPath, "unittest: create gaps-src-topic");
        CmCreateTopic(*stub, kDstTopicCmPath, "unittest: create gaps-dst-topic");

        {
            NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
            req.set_comment("unittest: set retention=1s on gaps-src-topic");
            req.set_token("root@builtin");

            auto* action = req.add_actions();
            action->mutable_update_topic()->mutable_path()->set_path(kSrcTopicCmPath);
            action->mutable_update_topic()->mutable_properties()->mutable_retention_period_sec()->set_user_defined(1);
            action->mutable_update_topic()->mutable_admin_properties()->mutable_max_partition_write_speed()->set_user_defined(40_MB);

            ExecCmRequest(*stub, req, "set retention on src topic");
        }

        {
            NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
            req.set_comment("unittest: set write quota=10KB/s on gaps-dst-topic");
            req.set_token("root@builtin");

            auto* action = req.add_actions();
            action->mutable_update_topic()->mutable_path()->set_path(kDstTopicCmPath);
            action->mutable_update_topic()->mutable_admin_properties()->mutable_max_partition_write_speed()->set_user_defined(2_MB);

            ExecCmRequest(*stub, req, "set write quota on dst topic");
        }

        {
            NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
            req.set_comment("unittest: create mirror rule for gap test");

            auto* action = req.add_actions();
            auto* rule = action->mutable_create_remote_mirror_rule();
            rule->mutable_remote_mirror_rule()->mutable_topic()->set_path(kDstTopicCmPath);
            rule->mutable_remote_mirror_rule()->mutable_cluster()->set_cluster("cluster_b");

            rule->mutable_properties()->mutable_src_cluster_endpoint()->set_user_defined(env.EndpointA);
            rule->mutable_properties()->mutable_src_database()->set_user_defined(prodDatabasePath);
            rule->mutable_properties()->mutable_src_topic()->set_user_defined(kSrcTopicYdbPath);
            rule->mutable_properties()->mutable_src_consumer()->set_user_defined(consumerName);
            rule->mutable_properties()->mutable_credentials()->set_oauth_token("root@builtin");

            ExecCmRequest(*stub, req, "create mirror rule");
        }

        Sleep(TDuration::Seconds(30));

        std::vector<TString> writtenPayloads = WriteLoadMessages(env.EndpointA, prodDatabasePath, kSrcTopicYdbPath, "gaps-producer", 100);
        UNIT_ASSERT_EQUAL(writtenPayloads.size(), 100);
        Sleep(TDuration::Seconds(5));

        {
            TDriver driverB = MakeDriver(env.EndpointB, prodDatabasePath);
            TTopicClient client(driverB);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(consumerName)
                    .AppendTopics(TTopicReadSettings(kDstTopicYdbPath))
            );

            std::map<ui64, TString> mirroredMessages = ReadMessages(session, 1000, TDuration::Seconds(10));
            Cerr << TInstant::Now() << " WrittenMessages.size()=" << writtenPayloads.size() << ", mirroredMessages.size()=" << mirroredMessages.size() << Endl;
            UNIT_ASSERT(mirroredMessages.size() < writtenPayloads.size());
            for (auto& [offset, data] : mirroredMessages) {
                Cerr << "Offset=" << offset << " mirrored: " << data.substr(0, 10) << ", size=" << data.size() << Endl;
                Cerr << "Offset=" << offset << " written: " << writtenPayloads[offset].substr(0, 10) << ", size=" << writtenPayloads[offset].size() << Endl;
                Cerr << "~~~~~~~~~~~~~" << Endl;
                UNIT_ASSERT_EQUAL(writtenPayloads[offset], data);
            }
        }
    }

        Y_UNIT_TEST(CreateRemoteMirrorRuleWithGapsAutosplitTopic) {
            TClusterEndpoints env;
            const TString srcTopicCmPath = "prod/ap-src-topic";
            const TString srcTopicYdbPath = "ap-src-topic";
            const TString dstTopicCmPath = "prod/ap-dst-topic";
            const TString dstTopicYdbPath = "ap-dst-topic";
            Cerr << TInstant::Now() << " Starting test" << Endl;

            auto channel = grpc::CreateChannel(
            env.EndpointCM,
            grpc::InsecureChannelCredentials()
            );
            auto stub = NLogBroker::NAdmin::ConfigurationManagerAdminService::NewStub(channel);

            CmCreateTopic(*stub, srcTopicCmPath, "unittest: create ap-src-topic", true);
            CmCreateTopic(*stub, dstTopicCmPath, "unittest: create ap-dst-topic", true);

            {
            NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
            req.set_comment("unittest: create mirror rule for autopartitioning test");

            auto* action = req.add_actions();
            auto* rule = action->mutable_create_remote_mirror_rule();
            rule->mutable_remote_mirror_rule()->mutable_topic()->set_path(dstTopicCmPath);
            rule->mutable_remote_mirror_rule()->mutable_cluster()->set_cluster("cluster_b");

            rule->mutable_properties()->mutable_src_cluster_endpoint()->set_user_defined(env.EndpointA);
            rule->mutable_properties()->mutable_src_database()->set_user_defined(prodDatabasePath);
            rule->mutable_properties()->mutable_src_topic()->set_user_defined(srcTopicYdbPath);
            rule->mutable_properties()->mutable_src_consumer()->set_user_defined(consumerName);
            rule->mutable_properties()->mutable_credentials()->set_oauth_token("root@builtin");

            ExecCmRequest(*stub, req, "create autopartitioning mirror rule");

            Sleep(TDuration::Seconds(15));

            const size_t messageCount = 60;
            const size_t messageSize = 500 * 1024; // 500 KB
            std::vector<TString> allWritten = WriteLoadMessages(
                env.EndpointA, prodDatabasePath, srcTopicYdbPath,
                "ap-producer", messageCount, messageSize, messageSize);

            GetActivePartitionCount(env.EndpointA, prodDatabasePath, srcTopicYdbPath);

            size_t srcActivePartitions = 1;
            TInstant splitDeadline = TInstant::Now() + TDuration::Seconds(120);
            while (TInstant::Now() < splitDeadline && srcActivePartitions < 2) {
                Sleep(TDuration::Seconds(3));
                srcActivePartitions = GetActivePartitionCount(env.EndpointA, prodDatabasePath, srcTopicYdbPath);
                Cerr << TInstant::Now() << " src active partitions: " << srcActivePartitions << Endl;
            }
            UNIT_ASSERT_C(srcActivePartitions >= 2, "src topic did not split after writing " + std::to_string(allWritten.size()) + " messages");

            size_t dstActivePartitions = 0;
            TInstant dstSplitDeadline = TInstant::Now() + TDuration::Seconds(120);
            while (TInstant::Now() < dstSplitDeadline) {
                dstActivePartitions = GetActivePartitionCount(env.EndpointB, prodDatabasePath, dstTopicYdbPath);
                Cerr << TInstant::Now() << " dst active partitions: " << dstActivePartitions << Endl;
                if (dstActivePartitions >= srcActivePartitions) {
                    break;
                }
                Sleep(TDuration::Seconds(5));
            }

            UNIT_ASSERT_VALUES_EQUAL_C(dstActivePartitions, srcActivePartitions,
                "dst topic did not split to match src partition count");

            Cerr << TInstant::Now() << "Src and dst topic have splitted" << Endl;

            std::unordered_set<TString> writtenSet(allWritten.begin(), allWritten.end());

            std::map<std::pair<ui64, ui64>, TString> dstMessages;
            {
                TDriver driverB = MakeDriver(env.EndpointB, prodDatabasePath);
                {
                    TTopicClient client(driverB);
                    auto session = client.CreateReadSession(
                        TReadSessionSettings()
                            .ConsumerName(consumerName)
                            .AppendTopics(TTopicReadSettings(dstTopicYdbPath))
                    );
                    dstMessages = ReadAutoscaledTopicMessages(session, allWritten.size(), TDuration::Seconds(20));
                    session->Close(TDuration::Seconds(5));
                }
                driverB.Stop(true);
            }

            Cerr << TInstant::Now() << "Read dst topic. Comparing" << Endl;

            for (auto& [key, data] : dstMessages) {
                UNIT_ASSERT_C(writtenSet.count(data),
                    "dst message at partition=" + std::to_string(key.first) +
                    " offset=" + std::to_string(key.second) + " not found in written set: " + data.substr(0, 20));
            }
        }
    }
}
