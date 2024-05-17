#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/client/ydb_topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/datetime/base.h>
#include <util/stream/output.h>


static inline IOutputStream& operator<<(IOutputStream& o, std::set<size_t> t) {
    o << "[" << JoinRange(", ", t.begin(), t.end()) << "]";
    return o;
}

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;

Y_UNIT_TEST_SUITE(Balancing) {

    Y_UNIT_TEST(Simple) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 10);

        TTopicClient client = setup.MakeClient();

        TTestReadSession readSession0("Session-0", client);
        {
            readSession0.WaitAndAssertPartitions({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "Single reading session must read all partitions");
            readSession0.Run();
        }

        TTestReadSession readSession1("Session-1", client);
        {
            readSession1.Run();

            Sleep(TDuration::Seconds(1));

            auto p0 = readSession0.GetPartitions();
            auto p1 = readSession1.GetPartitions();

            UNIT_ASSERT_VALUES_EQUAL_C(5, p0.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p0, " << p0 << ")");
            UNIT_ASSERT_VALUES_EQUAL_C(5, p1.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p1, " << p1 << ")");
            p0.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(10, p0.size(), "Must read all partitions but " << p0);
        }

        TTestReadSession readSession2("Session-2", client, Max<size_t>(), true, {0, 1});
        {
            readSession2.WaitAndAssertPartitions({0, 1}, "The reading session should read partitions 0 and 1 because it clearly required them to be read.");
            readSession2.Run();

            Sleep(TDuration::Seconds(1));

            auto p0 = readSession0.GetPartitions();
            auto p1 = readSession1.GetPartitions();
            auto pa = p0;
            pa.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(4, p0.size(), "There should be an even distribution of partitions " << p0);
            UNIT_ASSERT_VALUES_EQUAL_C(4, p1.size(), "There should be an even distribution of partitions " << p1);
            UNIT_ASSERT_VALUES_EQUAL_C(8, pa.size(), "Must read all partitions but " << pa);
        }

        TTestReadSession readSession3("Session-3", client, Max<size_t>(), true, {0});
        {
            readSession3.WaitAndAssertPartitions({0}, "The reading session should read partitions 0 and 1 because it clearly required them to be read.");
            readSession2.WaitAndAssertPartitions({1}, "The reading session should read partitions 0 and 1 because it clearly required them to be read.");

            auto p0 = readSession0.Impl->Partitions;
            p0.insert(readSession1.Impl->Partitions.begin(), readSession1.Impl->Partitions.end());
            UNIT_ASSERT_VALUES_EQUAL_C(8, p0.size(), "Must read all partitions but " << p0);
        }

        {
            readSession3.Run();
            readSession3.Close();

            readSession2.WaitAndAssertPartitions({0, 1}, "The reading session should read partitions 0 and 1 because it clearly required them to be read. (after release Session-3)");
            readSession2.Run();
        }

        {
            readSession2.Run();
            readSession2.Close();

            Sleep(TDuration::Seconds(1));

            auto p0 = readSession0.GetPartitions();
            auto p1 = readSession1.GetPartitions();

            UNIT_ASSERT_VALUES_EQUAL_C(5, p0.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p0, " << p0 << ")");
            UNIT_ASSERT_VALUES_EQUAL_C(5, p1.size(), "After the appearance of the second reading session, the partitions should be distributed evenly among them (p1, " << p1 << ")");
            p0.insert(p1.begin(), p1.end());
            UNIT_ASSERT_VALUES_EQUAL_C(10, p0.size(), "Must read all partitions but " << p0);
        }

        {
            readSession1.Run();
            readSession1.Close();

            readSession0.WaitAndAssertPartitions({0, 1, 2, 3, 4, 5, 6, 7, 8, 9}, "Single reading session must read all partitions");
            readSession0.Run();
        }


        readSession0.Close();
    }

/*
    Y_UNIT_TEST(BalanceManySession) {
        TTopicSdkTestSetup setup = CreateSetup();
        setup.CreateTopic(TEST_TOPIC, TEST_CONSUMER, 1000);

        TTopicClient client = setup.MakeClient();

        auto CreateClient = [&](size_t i) {
            auto readSettings = TReadSessionSettings()
                .ConsumerName(TEST_CONSUMER)
                .AppendTopics(TEST_TOPIC);
            readSettings.Topics_[0].AppendPartitionIds(i % 1000);

            return client.CreateReadSession(readSettings);
        };

        Cerr << ">>>>> " << TInstant::Now() << " Begin create sessions" << Endl << Flush;

        std::deque<std::shared_ptr<IReadSession>> sessions;
        for (int i = 0; i < 2000; ++i) {
            sessions.push_back(CreateClient(i));
        }

        for (int i = 0 ; i < 1000 ; ++i) {
            Cerr << ">>>>> " << TInstant::Now() << " Close session " << i << Endl << Flush;

            auto s = sessions.front();
            s->Close();
            sessions.pop_front();

            Sleep(TDuration::MilliSeconds(50));

            sessions.push_back(CreateClient(i * 7));
        }

        Cerr << ">>>>> " << TInstant::Now() << " Finished" << Endl << Flush;
        Sleep(TDuration::Seconds(10));
    }
*/
 }

} // namespace NKikimr
