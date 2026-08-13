#include <ydb/tests/functional/federation_test/common_functions.h>
#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/grpc/include/grpcpp/grpcpp.h>

const std::string kDatabase = "/Root/logbroker-federation/prod";
const std::string kTopicYdb = "write-disabled-topic";
const std::string kTopicCM  = "prod/write-disabled-topic";
const std::string kConsumer = "consumer";

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NFederationTests;

using AdminStub = NLogBroker::NAdmin::ConfigurationManagerAdminService::Stub;

Y_UNIT_TEST_SUITE(TWriteDisabledTest) {

    Y_UNIT_TEST(DisableWriteOnClusterA) {
        const char* portA  = std::getenv("cluster_a_port");
        const char* cmPort = std::getenv("CM_PORT");
        UNIT_ASSERT_C(portA, "cluster_a_port is not set by federation_recipe");
        UNIT_ASSERT_C(cmPort, "CM_PORT is not set by federation_recipe");

        const std::string endpointA = std::string("localhost:") + portA;

        auto channel = grpc::CreateChannel(
            std::string("localhost:") + cmPort,
            grpc::InsecureChannelCredentials()
        );
        auto stub = NLogBroker::NAdmin::ConfigurationManagerAdminService::NewStub(channel);

        {
            NLogBroker::NAdmin::ExecuteModifyCommandsRequest req;
            req.set_comment("create write-disabled-topic for test");
            auto* action = req.add_actions();
            action->mutable_create_topic()->mutable_path()->set_path(kTopicCM);
            action->mutable_create_topic()->set_parent_template("default");
            action->mutable_create_topic()->mutable_properties()->mutable_partitions_count()->set_user_defined(1);
            ExecCmRequest(*stub, req, "create topic");
        }

        const std::vector<std::string> initialMsgs = {
            "msg-0", "msg-1", "msg-2", "msg-3", "msg-4",
            "msg-5", "msg-6", "msg-7", "msg-8", "msg-9",
        };
        WriteMessages(endpointA, kDatabase, kTopicYdb, "producer-initial", initialMsgs);

        {
            TDriver driverA = MakeDriver(endpointA, kDatabase);
            TTopicClient client(driverA);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(kConsumer)
                    .AppendTopics(TTopicReadSettings(kTopicYdb))
            );
            auto got = ReadMessages(session, initialMsgs.size());
            UNIT_ASSERT_VALUES_EQUAL_C(got.size(), initialMsgs.size(), "Expected 10 initial messages, got " + std::to_string(got.size()));
            for (size_t i = 0; i < initialMsgs.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(got[i], initialMsgs[i]);
            }
        }

        SetClusterWriteEnabled(*stub, "cluster_a", false);
        Sleep(TDuration::Seconds(5));

        {
            TDriver driver = MakeDriver(endpointA, kDatabase);
            TTopicClient client(driver);
            auto session = client.CreateSimpleBlockingWriteSession(
                TWriteSessionSettings()
                    .Path(kTopicYdb)
                    .MessageGroupId("producer-on-closed")
                    .RetryPolicy(NYdb::Dev::NTopic::IRetryPolicy::GetExponentialBackoffPolicy(
                    /*minDelay=*/TDuration::MilliSeconds(10),
                    /*minLongRetryDelay=*/TDuration::MilliSeconds(200),
                    /*maxDelay=*/TDuration::Seconds(5),
                    /*maxRetries=*/10,
                    /*maxTime=*/TDuration::Seconds(20)))
            );
            bool writeOk = session->Write("test");
            session->Close(TDuration::Seconds(2));
            driver.Stop(true);

            UNIT_ASSERT_C(!writeOk, "Write to cluster_a must be rejected when write_enabled=false in CM");
        }


        {
            TDriver driverA = MakeDriver(endpointA, kDatabase);
            TTopicClient client(driverA);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(kConsumer)
                    .AppendTopics(TTopicReadSettings(kTopicYdb))
            );
            auto noNewMsgs = ReadMessages(session, 1, TDuration::Seconds(5));
            UNIT_ASSERT_C(noNewMsgs.empty(),
                "No new messages should appear on cluster_a while write-disabled");
        }

        SetClusterWriteEnabled(*stub, "cluster_a", true);
        Sleep(TDuration::Seconds(5));

        const std::string msgAfterReEnable = "msg-after-reenable";
        WriteMessages(endpointA, kDatabase, kTopicYdb, "producer-reenable", {msgAfterReEnable});

        {
            TDriver driverA = MakeDriver(endpointA, kDatabase);
            TTopicClient client(driverA);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(kConsumer)
                    .AppendTopics(TTopicReadSettings(kTopicYdb))
            );
            auto afterReEnabled = ReadMessages(session, 1);
            UNIT_ASSERT_C(!afterReEnabled.empty(), "Expected a message from cluster_a after re-enabling writes");
            UNIT_ASSERT_VALUES_EQUAL(afterReEnabled[10], msgAfterReEnable);
        }
    }

}
