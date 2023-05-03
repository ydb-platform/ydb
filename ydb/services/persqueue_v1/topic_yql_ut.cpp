#include <ydb/services/persqueue_v1/ut/test_utils.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/test_server.h>

namespace NKikimr::NPersQueueTests {

namespace {
    const static TString DEFAULT_TOPIC_NAME = "rt3.dc1--topic1";
}

Y_UNIT_TEST_SUITE(TTopicYqlTest) {
    Y_UNIT_TEST(DropTopicYql) {
        NPersQueue::TTestServer server;
  //      Server->EnableLogs({NKikimrServices::FLAT_TX_SCHEMESHARD
        auto CheckPQChildrenSize = [&](const TString prefix) {
            auto children = server.AnnoyingClient->Ls("/Root/PQ/")->Record.GetPathDescription().ChildrenSize();
            Cerr << prefix << ", children in PQ:" << children << Endl;
            return children;
        };
        server.AnnoyingClient->CreateTopic(DEFAULT_TOPIC_NAME, 10);
        auto before = CheckPQChildrenSize("before drop");
        server.AnnoyingClient->RunYqlSchemeQuery(TStringBuilder() << "DROP TOPIC `/Root/PQ/" << DEFAULT_TOPIC_NAME << "`;");
        auto after = CheckPQChildrenSize("after drop");
        UNIT_ASSERT_VALUES_EQUAL(after + 1, before);
    }
    Y_UNIT_TEST(CreateAndAlterTopicYql) {
        NPersQueue::TTestServer server;
        auto CheckPQChildrenSize = [&](const TString prefix) {
            auto children = server.AnnoyingClient->Ls("/Root/PQ/")->Record.GetPathDescription().ChildrenSize();
            Cerr << prefix << ", children in PQ:" << children << Endl;
            return children;
        };
        auto before = CheckPQChildrenSize("before create");
        const char *query = R"__(
            CREATE TOPIC `/Root/PQ/rt3.dc1--legacy--topic1` (
                CONSUMER c1,
                CONSUMER c2 WITH (important = true, read_from = 100, supported_codecs = 'RAW, LZOP, GZIP')
            ) WITH (min_active_partitions = 2, partition_count_limit = 5, retention_period = Interval('PT1H'),
                    retention_storage_mb = 15,
                    supported_codecs = 'RAW, GZIP', partition_write_speed_bytes_per_second = 9000,
                    partition_write_burst_bytes = 100500
            );
        )__";
        server.AnnoyingClient->RunYqlSchemeQuery(query);
        auto after = CheckPQChildrenSize("after create");
        UNIT_ASSERT_VALUES_EQUAL(after, before + 1);
        auto pqGroup = server.AnnoyingClient->Ls("/Root/PQ/rt3.dc1--legacy--topic1")->Record.GetPathDescription()
                                                                                            .GetPersQueueGroup();
        const auto& descr = pqGroup.GetPQTabletConfig();
        Cerr <<"=== PATH DESCRIPTION: \n" << pqGroup.DebugString();
        UNIT_ASSERT_VALUES_EQUAL(descr.GetPartitionConfig().GetLifetimeSeconds(), 3600);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetPartitionConfig().GetBurstSize(), 100500);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetPartitionConfig().GetWriteSpeedInBytesPerSecond(), 9000);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetPartitionConfig().ImportantClientIdSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetPartitionConfig().GetImportantClientId(0), "c2");

        auto& codecs = descr.GetConsumerCodecs(1);
        UNIT_ASSERT_VALUES_EQUAL(codecs.IdsSize(), 3);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetCodecs().IdsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(pqGroup.GetTotalGroupCount(), 2);
        UNIT_ASSERT_VALUES_EQUAL(descr.GetReadFromTimestampsMs(1), 100 * 1000);

        auto descrCopy = descr;
        {
            auto partCfg = descrCopy.MutablePartitionConfig();
            partCfg->SetLifetimeSeconds(7200);
            partCfg->SetBurstSize(100501);
            partCfg->SetWriteSpeedInBytesPerSecond(9001);
            auto* rtfs = descrCopy.MutableReadFromTimestampsMs();
            rtfs->Set(1, 1609462861000);
        }
        const char *query2 = R"__(
        ALTER TOPIC `/Root/PQ/rt3.dc1--legacy--topic1`
            ALTER CONSUMER c2 SET (read_from = Timestamp('2021-01-01T01:01:01Z')),
            SET (min_active_partitions = 3, retention_period = Interval('PT2H'), retention_storage_mb = 10,
                 partition_count_limit = 5,  
                 partition_write_burst_bytes = 100501, partition_write_speed_bytes_per_second = 9001
            );
        )__";
        server.AnnoyingClient->RunYqlSchemeQuery(query2);
        auto pqGroup2 = server.AnnoyingClient->Ls("/Root/PQ/rt3.dc1--legacy--topic1")->Record.GetPathDescription()
                                                                                             .GetPersQueueGroup();
        const auto& descr2 = pqGroup2.GetPQTabletConfig();
        UNIT_ASSERT_VALUES_EQUAL(descrCopy.DebugString(), descr2.DebugString());
        
        const char *query3 = R"__(
        ALTER TOPIC `/Root/PQ/rt3.dc1--legacy--topic1`
            DROP CONSUMER c1,
            ALTER CONSUMER c2 SET (important = false, read_from = Datetime('2021-01-01T01:01:01Z'), supported_codecs = 'RAW, GZIP'),
            ADD CONSUMER c3 WITH (important = true),
            SET (supported_codecs = 'RAW');
        )__";
        Cerr << "\nRun query: \n" << query2 << Endl;
        server.AnnoyingClient->RunYqlSchemeQuery(query3);

        pqGroup2 = server.AnnoyingClient->Ls("/Root/PQ/rt3.dc1--legacy--topic1")->Record.GetPathDescription()
                                                                                        .GetPersQueueGroup();
        const auto& descr3 = pqGroup2.GetPQTabletConfig();
        Cerr <<"=== PATH DESCRIPTION: \n" << pqGroup.DebugString();
        UNIT_ASSERT_VALUES_EQUAL(descr3.GetPartitionConfig().GetLifetimeSeconds(), 7200);
        UNIT_ASSERT_VALUES_EQUAL(descr3.GetPartitionConfig().GetBurstSize(), 100501);
        UNIT_ASSERT_VALUES_EQUAL(descr3.GetPartitionConfig().GetWriteSpeedInBytesPerSecond(), 9001);
        UNIT_ASSERT_VALUES_EQUAL(descr3.GetPartitionConfig().ImportantClientIdSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(descr3.GetPartitionConfig().GetImportantClientId(0), "c3");

        UNIT_ASSERT_VALUES_EQUAL(descr3.GetConsumerCodecs(0).IdsSize(), 2);
        UNIT_ASSERT_VALUES_EQUAL(descr3.GetCodecs().IdsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(descr3.GetReadFromTimestampsMs(0), 1609462861000);
        UNIT_ASSERT_VALUES_EQUAL(pqGroup2.GetTotalGroupCount(), 3);

        //1609462861
    }

    Y_UNIT_TEST(BadRequests) {
        NPersQueue::TTestServer server;
        {
            const char *query = R"__(
                CREATE TOPIC `/Root/PQ/rt3.dc1--legacy--topic1` (CONSUMER c1, CONSUMER c2);
            )__";
            server.AnnoyingClient->RunYqlSchemeQuery(query);
        }{
            const char *query = R"__(
                CREATE TOPIC `/Root/PQ/rt3.dc1--legacy--topic2` (CONSUMER c1 with (read_from = today));
            )__";
            server.AnnoyingClient->RunYqlSchemeQuery(query, false);
        }
        {
            const char *query = R"__(
                ALTER TOPIC `/Root/PQ/rt3.dc1--legacy--topic1`
                    ADD CONSUMER c3,
                    ALTER CONSUMER c3 SET (important = true);
            )__";
            server.AnnoyingClient->RunYqlSchemeQuery(query, false);
        }
        {
            const char *query = R"__(
                ALTER TOPIC `/Root/PQ/rt3.dc1--legacy--topic1`
                    ALTER CONSUMER c2 SET (important = true),
                    DROP CONSUMER c2;
            )__";
            server.AnnoyingClient->RunYqlSchemeQuery(query, false);
        }
        {
            const char *query = R"__(
                ALTER TOPIC `/Root/PQ/rt3.dc1--legacy--topic1`
                    DROP CONSUMER c4,
                    ADD CONSUMER c4 WITH (important = true);
            )__";
            server.AnnoyingClient->RunYqlSchemeQuery(query, false);
        }

    }
};

} //namespace NKikimr::NPersQueueTests
