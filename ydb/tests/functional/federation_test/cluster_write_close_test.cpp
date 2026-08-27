#include <ydb/tests/functional/federation_test/common_functions.h>
#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/grpc/include/grpcpp/grpcpp.h>

using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NFederationTests;

Y_UNIT_TEST_SUITE(TWriteDisabledTest) {

    Y_UNIT_TEST(DisableWriteOnClusterA) {
        NFederationTests::TClusterEndpoints env;

        const TString prodDatabasePath = "/logbroker-federation/prod";
        const TString fullProdDatabasePath = "/Root/logbroker-federation/prod";
        const TString topicPath = "write-disabled-topic";
        const TString fullTopicPath  = "prod/write-disabled-topic";
        const TString kConsumer = "consumer";

        {
            TDriver driver = MakeDriver(env.EndpointCM, prodDatabasePath);
            TTopicClient client(driver);
            auto result = client.CreateTopic(
                topicPath,
                TCreateTopicSettings()
                    .PartitioningSettings(1, 1)
            ).GetValueSync();
            driver.Stop(true);
            UNIT_ASSERT_C(result.IsSuccess(),
                TStringBuilder() << "CreateTopic(" << topicPath << ") failed: " << result.GetIssues().ToString());
        }

        Sleep(TDuration::Seconds(2));


        const std::vector<TString> initialMsgs = {
            "msg-0", "msg-1", "msg-2", "msg-3", "msg-4",
            "msg-5", "msg-6", "msg-7", "msg-8", "msg-9",
        };
        WriteMessages(env.EndpointA, fullProdDatabasePath, topicPath, "producer-initial", initialMsgs);

        {
            TDriver driverA = MakeDriver(env.EndpointA, fullProdDatabasePath);
            TTopicClient client(driverA);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(kConsumer)
                    .AppendTopics(TTopicReadSettings(topicPath))
            );
            auto got = ReadMessages(session, initialMsgs.size());
            UNIT_ASSERT_VALUES_EQUAL_C(got.size(), initialMsgs.size(), "Expected 10 initial messages, got " + std::to_string(got.size()));
            for (size_t i = 0; i < initialMsgs.size(); ++i) {
                UNIT_ASSERT_VALUES_EQUAL(got[i], initialMsgs[i]);
            }
        }


        SetClusterWriteEnabledYql(env.EndpointA, "cluster_a", false);

        Sleep(TDuration::Seconds(30));

        {
            TDriver driver = MakeDriver(env.EndpointA, fullProdDatabasePath);
            TTopicClient client(driver);
            auto session = client.CreateSimpleBlockingWriteSession(
                TWriteSessionSettings()
                    .Path(topicPath)
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
            TDriver driverA = MakeDriver(env.EndpointA, fullProdDatabasePath);
            TTopicClient client(driverA);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(kConsumer)
                    .AppendTopics(TTopicReadSettings(topicPath))
            );
            auto noNewMsgs = ReadMessages(session, 1, TDuration::Seconds(5));
            UNIT_ASSERT_C(noNewMsgs.empty(),
                "No new messages should appear on cluster_a while write-disabled");
        }

        SetClusterWriteEnabledYql(env.EndpointA, "cluster_a", true);

        Sleep(TDuration::Seconds(30));

        const TString msgAfterReEnable = "msg-after-reenable";
        WriteMessages(env.EndpointA, fullProdDatabasePath, topicPath, "producer-reenable", {msgAfterReEnable});

        {
            TDriver driverA = MakeDriver(env.EndpointA, fullProdDatabasePath);
            TTopicClient client(driverA);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(kConsumer)
                    .AppendTopics(TTopicReadSettings(topicPath))
            );
            auto afterReEnabled = ReadMessages(session, 1);
            UNIT_ASSERT_C(!afterReEnabled.empty(), "Expected a message from cluster_a after re-enabling writes");
            UNIT_ASSERT_VALUES_EQUAL(afterReEnabled[10], msgAfterReEnable);
        }
    }

}
