#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/datetime/base.h>
#include <util/stream/output.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(Balancing) {

    void Simple(SdkVersion sdk) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 10);

        auto readSession0 = CreateTestReadSession({ .Name="Session-0", .Setup=setup, .Sdk = sdk });
        {
            readSession0->WaitAndAssertPartitions({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "Single reading session must read all partitions");
            readSession0->Run();
        }

        auto readSession1 = CreateTestReadSession({ .Name="Session-1", .Setup=setup, .Sdk = sdk });
        {
            readSession1->Run();

            Sleep(TDuration::Seconds(1));

            auto p0 = readSession0->GetPartitions();
            auto p1 = readSession1->GetPartitions();

            UNIT_ASSERT_VALUES_EQUAL_C(5, p0.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p0, " << p0 << ")");
            UNIT_ASSERT_VALUES_EQUAL_C(5, p1.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p1, " << p1 << ")");
            p0.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(10, p0.size(), "Must read all partitions but " << p0);
        }

        auto readSession2 = CreateTestReadSession({ .Name="Session-2", .Setup=setup, .Sdk = sdk, .Partitions = {0, 1} });
        {
            readSession2->WaitAndAssertPartitions({0, 1}, "The reading session should read partitions 0 and 1 because it clearly required them to be read.");
            readSession2->Run();

            Sleep(TDuration::Seconds(1));

            auto p0 = readSession0->GetPartitions();
            auto p1 = readSession1->GetPartitions();
            auto pa = p0;
            pa.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(4, p0.size(), "There should be an even distribution of partitions " << p0);
            UNIT_ASSERT_VALUES_EQUAL_C(4, p1.size(), "There should be an even distribution of partitions " << p1);
            UNIT_ASSERT_VALUES_EQUAL_C(8, pa.size(), "Must read all partitions but " << pa);
        }

        auto readSession3 = CreateTestReadSession({ .Name="Session-3", .Setup=setup, .Sdk = sdk, .Partitions = {0} });
        {
            readSession3->WaitAndAssertPartitions({0}, "The reading session should read partitions 0 and 1 because it clearly required them to be read.");
            readSession2->WaitAndAssertPartitions({1}, "The reading session should read partitions 0 and 1 because it clearly required them to be read.");

            auto p0 = readSession0->GetPartitions();
            auto p1 = readSession1->GetPartitions();
            p0.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(8, p0.size(), "Must read all partitions but " << p0);
        }

        {
            readSession3->Run();
            readSession3->Close();

            readSession2->WaitAndAssertPartitions({0, 1}, "The reading session should read partitions 0 and 1 because it clearly required them to be read. (after release Session-3)");
            readSession2->Run();
        }

        {
            readSession2->Run();
            readSession2->Close();

            Sleep(TDuration::Seconds(1));

            auto p0 = readSession0->GetPartitions();
            auto p1 = readSession1->GetPartitions();

            UNIT_ASSERT_VALUES_EQUAL_C(5, p0.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p0, " << p0 << ")");
            UNIT_ASSERT_VALUES_EQUAL_C(5, p1.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p1, " << p1 << ")");
            p0.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(10, p0.size(), "Must read all partitions but " << p0);
        }

        {
            readSession1->Run();
            readSession1->Close();

            readSession0->WaitAndAssertPartitions({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "Single reading session must read all partitions");
            readSession0->Run();
        }


        readSession0->Close();
    }

    Y_UNIT_TEST(Balancing_OneTopic_TopicApi) {
        Simple(SdkVersion::Topic);
    }

    Y_UNIT_TEST(Balancing_OneTopic_PQv1) {
        Simple(SdkVersion::PQv1);
    }


    void ManyTopics(SdkVersion sdk) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 10);
        setup.CreateTopic("other-test-topic", TEST_CONSUMER, 10);

        TTopicClient client = setup.MakeClient();

        auto readSession0 = CreateTestReadSession({ .Name="Session-0", .Setup=setup, .Sdk = sdk, .Topics = {TEST_TOPIC, "other-test-topic"} });
        Sleep(TDuration::Seconds(1));

        {
            auto p = readSession0->GetPartitionsA();
            UNIT_ASSERT_VALUES_EQUAL(10, p[TEST_TOPIC].size());
            UNIT_ASSERT_VALUES_EQUAL(10, p["other-test-topic"].size());
        }

        auto readSession1 = CreateTestReadSession({ .Name="Session-1", .Setup=setup, .Sdk = sdk, .Topics = {TEST_TOPIC, "other-test-topic"} });
        Sleep(TDuration::Seconds(1));

        {
            auto p = readSession0->GetPartitionsA();
            UNIT_ASSERT_VALUES_EQUAL(5, p[TEST_TOPIC].size());
            UNIT_ASSERT_VALUES_EQUAL(5, p["other-test-topic"].size());
        }
        {
            auto p = readSession1->GetPartitionsA();
            UNIT_ASSERT_VALUES_EQUAL(5, p[TEST_TOPIC].size());
            UNIT_ASSERT_VALUES_EQUAL(5, p["other-test-topic"].size());
        }
    }

    Y_UNIT_TEST(Balancing_ManyTopics_TopicApi) {
        ManyTopics(SdkVersion::Topic);
    }

    Y_UNIT_TEST(Balancing_ManyTopics_PQv1) {
        ManyTopics(SdkVersion::PQv1);
    }

    //
    // PQv1
    //
    Y_UNIT_TEST(PQv1_Simple) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 10);

        auto client = NYdb::NPersQueue::TPersQueueClient(*(setup.GetServer().AnnoyingClient->GetDriver()));

        class TestReader {
        public:
            TestReader(const TString& name, NYdb::NPersQueue::TPersQueueClient& client) {
                Partitions = std::make_shared<std::unordered_set<ui32>>();
                Lock = std::make_shared<TMutex>();

                NYdb::NPersQueue::TReadSessionSettings settings;
                settings
                    .ConsumerName(TEST_CONSUMER)
                    .AppendTopics(TEST_TOPIC);

                settings.EventHandlers_.CreatePartitionStreamHandler([lock=Lock, partitions=Partitions, name=name](NYdb::NPersQueue::TReadSessionEvent::TCreatePartitionStreamEvent& e) {
                    Cerr << ">>>>> " << name << " Received TCreatePartitionStreamEvent " << e.DebugString() << Endl<< Flush;
                    e.Confirm();
                    with_lock (*lock) {
                        partitions->insert(e.GetPartitionStream()->GetPartitionId());
                    }

                   // UNIT_ASSERT_C(!Partitions.insert(e.GetPartitionStream()->GetPartitionId()).second, "Partition already is readed");
                });
                settings.EventHandlers_.DestroyPartitionStreamHandler([lock=Lock, partitions=Partitions, name=name](NYdb::NPersQueue::TReadSessionEvent::TDestroyPartitionStreamEvent& e) {
                    Cerr << ">>>>> " << name << " Received TDestroyPartitionStreamEvent " << e.DebugString() << Endl<< Flush;
                    e.Confirm();
                    with_lock (*lock) {
                        partitions->erase(e.GetPartitionStream()->GetPartitionId());
                    }

                    //UNIT_ASSERT_C(!Partitions.erase(e.GetPartitionStream()->GetPartitionId()), "Partition is not readed");
                });

                ReadSession = client.CreateReadSession(settings);
            }



            std::shared_ptr<TMutex> Lock;
            std::shared_ptr<std::unordered_set<ui32>> Partitions;
            std::shared_ptr<NYdb::NPersQueue::IReadSession> ReadSession;
        };

        TestReader session1("Session-1", client);
        {
            Sleep(TDuration::Seconds(1));
            with_lock (*session1.Lock) {
                UNIT_ASSERT_VALUES_EQUAL(10, session1.Partitions->size());
            }
        }

        TestReader session2("Session-2", client);
        {
            Sleep(TDuration::Seconds(1));
            with_lock (*session1.Lock) {
                UNIT_ASSERT_VALUES_EQUAL(5, session1.Partitions->size());
            }
            with_lock (*session2.Lock) {
                UNIT_ASSERT_VALUES_EQUAL(5, session2.Partitions->size());
            }
        }

        session1.ReadSession->Close();
        session2.ReadSession->Close();
    }
 }

} // namespace NKikimr
