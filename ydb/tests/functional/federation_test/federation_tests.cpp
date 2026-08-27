#include <ydb/tests/functional/federation_test/common_functions.h>
#include <library/cpp/testing/unittest/registar.h>
#include <contrib/libs/grpc/include/grpcpp/grpcpp.h>


using namespace NYdb;
using namespace NYdb::NTopic;
using namespace NFederationTests;

const TString prodDatabasePath  = "/logbroker-federation/prod";
const TString fullProdDatabasePath  = "Root/logbroker-federation/prod";
const TString testDatabasePath  = "/logbroker-federation/test";
const TString fullTestDatabasePath  = "/Root/logbroker-federation/test";
const TString prodTopicPath = "topic";
const TString testTopicPath = "topic";
const TString gapTopicPath = "gap-topic";
const TString gapTopicCMPath = "/prod/gap-topic";
const TString consumerName  = "consumer";


Y_UNIT_TEST_SUITE(TFederationWriteReadTest) {

    // Y_UNIT_TEST(WriteAndReadOnClusterA) {
    //     TClusterEndpoints env;
    //     TString writtenMessage = "hello from cluster_a";
    //     WriteMessages(env.EndpointA, prodDatabasePath, prodTopicPath,
    //                   "ut-producer-a", {writtenMessage});

    //     TDriver driver = MakeDriver(env.EndpointA, prodDatabasePath);
    //     TTopicClient client(driver);

    //     std::map<ui64, TString> messages;
    //     {
    //         TTopicClient client(driver);
    //         auto session = client.CreateReadSession(
    //             TReadSessionSettings()
    //                 .ConsumerName(consumerName)
    //                 .AppendTopics(TTopicReadSettings(prodTopicPath))
    //         );
    //         messages = ReadMessages(session, 1);
    //         session->Close(TDuration::Seconds(5));
    //     }
    //     driver.Stop(true);
    //     UNIT_ASSERT_EQUAL(writtenMessage, messages[0]);
    // }

    Y_UNIT_TEST(SimpleRemoteMirrorRuleWorks) {
        NFederationTests::TClusterEndpoints env;

        const TString mirrorConsumerName = "mirror-consumer";
        const TString fullMirrorConsumerName = "/logbroker-federation/test/mirror-consumer";
        const TString fullTestTopicPath = "/test/topic";
        const TString mirroredTopicPath = testTopicPath + "-mirrored-from-cluster_a";
        const TString fullMirroredTopicPath = "/test/" + mirroredTopicPath;

        {
            TDriver driver = MakeDriver(env.EndpointCM, testDatabasePath);
            TTopicClient client(driver);
            auto result = client.AlterTopic(
                testTopicPath,
                TAlterTopicSettings()
                    .BeginAddConsumer(fullMirrorConsumerName)
                        .AddAttribute("_lb_read_rule", R"({"type":"mirror-to","cluster":"cluster_b"})")
                    .EndAddConsumer()
            ).GetValueSync();
            driver.Stop(true);
            UNIT_ASSERT_C(result.IsSuccess(),
                TStringBuilder() << "AlterTopic (add mirror-to consumer) failed: " << result.GetIssues().ToString());
        }

        const std::vector<TString> written = {"mirror-msg-0", "mirror-msg-1", "mirror-msg-2"};
        WriteMessages(env.EndpointA, fullTestDatabasePath, testTopicPath, "mirror-producer", written);

        TDriver driverB = MakeDriver(env.EndpointB, fullTestDatabasePath);
        std::map<ui64, TString> received;
        {
            TTopicClient client(driverB);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(consumerName)
                    .AppendTopics(TTopicReadSettings(mirroredTopicPath))
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
        NFederationTests::TClusterEndpoints env;

        const TString srcTopicCmPath = "prod/gaps-topic";
        const TString srcTopicYdbPath = "gaps-topic";

        const TString mirroredTopicPath = "/prod/gaps-topic-mirrored-from-cluster_a";
        const TString shortMirroredTopicPath = "gaps-topic-mirrored-from-cluster_a";
        const TString prodConsumerName = "/logbroker-federation/prod/consumer-mirror";
        const TString shortProdConsumerName = "consumer-mirror";

        {
            TDriver driver = MakeDriver(env.EndpointCM, prodDatabasePath);
            TTopicClient client(driver);
            auto result = client.CreateTopic(
                srcTopicYdbPath,
                TCreateTopicSettings()
                    .RetentionPeriod(TDuration::Seconds(1))
                    .PartitionWriteSpeedBytesPerSecond(40_MB)
                    .PartitioningSettings(1, 1)
            ).GetValueSync();
            driver.Stop(true);
            UNIT_ASSERT_C(result.IsSuccess(),
                TStringBuilder() << "CreateTopic(" << srcTopicYdbPath << ") failed: "
                                << result.GetIssues().ToString());
        }

        {
            TDriver driver = MakeDriver(env.EndpointCM, prodDatabasePath);
            TTopicClient client(driver);
            auto result = client.AlterTopic(
                srcTopicYdbPath,
                TAlterTopicSettings()
                    .BeginAddConsumer(prodConsumerName)
                        .AddAttribute("_lb_read_rule", R"({"type":"mirror-to","cluster":"cluster_b"})")
                    .EndAddConsumer()
            ).GetValueSync();
            driver.Stop(true);
            UNIT_ASSERT_C(result.IsSuccess(),
                TStringBuilder() << "AlterTopic (add mirror-to consumer) failed: "
                                << result.GetIssues().ToString());
        }

        Sleep(TDuration::Seconds(40));
        {
            TDriver driverB = MakeDriver(env.EndpointB, fullProdDatabasePath);
            TTopicClient client(driverB);
            auto result = client.AlterTopic(
                shortMirroredTopicPath,
                TAlterTopicSettings()
                    .SetPartitionWriteSpeedBytesPerSecond(2_MB)
            ).GetValueSync();
            driverB.Stop(true);
            UNIT_ASSERT_C(result.IsSuccess(),
                TStringBuilder() << "AlterTopic (throttle mirrored topic) failed: "
                                << result.GetIssues().ToString());
        }

        Sleep(TDuration::Seconds(5));

        std::vector<TString> writtenPayloads = WriteLoadMessages(env.EndpointA, fullProdDatabasePath, srcTopicYdbPath, "gaps-producer", 100);
        UNIT_ASSERT_EQUAL(writtenPayloads.size(), 100);
        Sleep(TDuration::Seconds(5));

        {
            TDriver driverB = MakeDriver(env.EndpointB, fullProdDatabasePath);
            TTopicClient client(driverB);
            auto session = client.CreateReadSession(
                TReadSessionSettings()
                    .ConsumerName(shortProdConsumerName)
                    .AppendTopics(TTopicReadSettings(shortMirroredTopicPath))
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
        NFederationTests::TClusterEndpoints env;
        const TString srcTopicYdbPath = "topic-ap";
        const TString fullSrtTopicPath = "/prod/topic-ap";
        const TString mirroredTopicYdbPath = srcTopicYdbPath + "-mirrored-from-cluster_a";
        const TString prodConsumerName = "/logbroker-federation/prod/consumer-mirror";
        const TString shortProdConsumerName = "consumer-mirror";

        Cerr << TInstant::Now() << " Starting test" << Endl;

        // Create src topic with autopartitioning via SDK on CM endpoint
        {
            TDriver driver = MakeDriver(env.EndpointCM, prodDatabasePath);
            TTopicClient client(driver);
            auto result = client.CreateTopic(
                srcTopicYdbPath,
                TCreateTopicSettings()
                    .PartitioningSettings(TPartitioningSettings(
                        /*minActivePartitions=*/1,
                        /*maxActivePartitions=*/4,
                        TAutoPartitioningSettings(
                            EAutoPartitioningStrategy::ScaleUp,
                            TDuration::Seconds(10),
                            /*downUtilizationPercent=*/0,
                            /*upUtilizationPercent=*/1
                        )
                    ))
            ).GetValueSync();
            driver.Stop(true);
            UNIT_ASSERT_C(result.IsSuccess(),
                TStringBuilder() << "CreateTopic(" << srcTopicYdbPath << ") failed: " << result.GetIssues().ToString());
        }

        Cerr << TInstant::Now() << " Src topic created, adding mirror-to consumer" << Endl;

        TDriver driver = MakeDriver(env.EndpointCM, prodDatabasePath);
        TTopicClient client(driver);
        auto result = client.AlterTopic(
            srcTopicYdbPath,
            TAlterTopicSettings()
                .BeginAddConsumer(prodConsumerName)
                    .AddAttribute("_lb_read_rule", R"({"type":"mirror-to","cluster":"cluster_b"})")
                .EndAddConsumer()
        ).GetValueSync();
        driver.Stop(true);
        UNIT_ASSERT_C(result.IsSuccess(),
            TStringBuilder() << "AlterTopic (add mirror-to consumer) failed: " << result.GetIssues().ToString());


        Sleep(TDuration::Seconds(15));

        const size_t messageCount = 60;
        const size_t messageSize = 500 * 1024; // 500 KB
        std::vector<TString> allWritten = WriteLoadMessages(env.EndpointA, fullProdDatabasePath, srcTopicYdbPath,
                                                            "ap-producer", messageCount, messageSize, messageSize);

        GetActivePartitionCount(env.EndpointA, prodDatabasePath, srcTopicYdbPath);

        size_t srcActivePartitions = 1;
        TInstant splitDeadline = TInstant::Now() + TDuration::Seconds(120);
        while (TInstant::Now() < splitDeadline && srcActivePartitions < 2) {
            Sleep(TDuration::Seconds(3));
            srcActivePartitions = GetActivePartitionCount(env.EndpointA, fullProdDatabasePath, srcTopicYdbPath);
            Cerr << TInstant::Now() << " src active partitions: " << srcActivePartitions << Endl;
        }
        UNIT_ASSERT_C(srcActivePartitions >= 2, "src topic did not split after writing " + std::to_string(allWritten.size()) + " messages");

        size_t dstActivePartitions = 0;
        TInstant dstSplitDeadline = TInstant::Now() + TDuration::Seconds(120);
        while (TInstant::Now() < dstSplitDeadline) {
            dstActivePartitions = GetActivePartitionCount(env.EndpointB, fullProdDatabasePath, mirroredTopicYdbPath);
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
            TDriver driverB = MakeDriver(env.EndpointB, fullProdDatabasePath);
            {
                TTopicClient client(driverB);
                auto session = client.CreateReadSession(
                    TReadSessionSettings()
                        .ConsumerName(consumerName)
                        .AppendTopics(TTopicReadSettings(mirroredTopicYdbPath))
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
