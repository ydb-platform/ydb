#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/pqrb/partition_scale_manager.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

namespace NKikimr {

using namespace NYdb::NTopic;
using namespace NYdb::NTopic::NTests;
using namespace NSchemeShardUT_Private;
using namespace NKikimr::NPQ::NTest;

Y_UNIT_TEST_SUITE(SlowTopicAutopartitioning) {

    void ExecuteQuery(NYdb::NTable::TSession& session, const TString& query ) {
        const auto result = session.ExecuteSchemeQuery(query).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
    }

    void ExecuteDataQuery(NYdb::NTable::TSession& session, const TString& query) {
        TString error;
        for (size_t i = 0; i < 20; ++i) {
            const auto result = session.ExecuteDataQuery(query, NYdb::NTable::TTxControl::BeginTx().CommitTx()).GetValueSync();
            if (NYdb::EStatus::SUCCESS != result.GetStatus()) {
                error = result.GetIssues().ToString();
                Sleep(TDuration::MilliSeconds(500));
                continue;
            }
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());
            return;
        }

        UNIT_ASSERT_C(false, "ExecuteDataQuery error: " << error);
    }

    ui64 GetBalancerTabletId(TTopicSdkTestSetup& setup, const TString& topicPath) {
        auto pathDescr = setup.GetServer().AnnoyingClient->Describe(&setup.GetRuntime(), topicPath).GetPathDescription().GetPersQueueGroup();
        auto balancerTabletId = pathDescr.GetBalancerTabletID();
        Cerr << ">>>>> BalancerTabletID=" << balancerTabletId << Endl << Flush;
        UNIT_ASSERT(balancerTabletId);
        return balancerTabletId;
    }

    void SplitPartitionRB(TTopicSdkTestSetup& setup, const TString& topicPath, ui32 partitionId) {
        auto balancerTabletId = GetBalancerTabletId(setup, topicPath);
        auto edge = setup.GetRuntime().AllocateEdgeActor();
        setup.GetRuntime().SendToPipe(balancerTabletId, edge, new TEvPQ::TEvPartitionScaleStatusChanged(partitionId, NKikimrPQ::EScaleStatus::NEED_SPLIT));
    }

    void AssertPartitionCount(TTopicSdkTestSetup& setup, const TString& topicPath, size_t expectedCount) {
        auto client = setup.MakeClient();
        auto describe = client.DescribeTopic(topicPath).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL(describe.GetTopicDescription().GetPartitions().size(), expectedCount);
    }

    void WaitAndAssertPartitionCount(TTopicSdkTestSetup& setup, const TString& topicPath, size_t expectedCount) {
        auto client = setup.MakeClient();
        size_t partitionCount = 0;
        for (size_t i = 0; i < 10; ++i) {
            Sleep(TDuration::Seconds(1));
            auto describe = client.DescribeTopic(topicPath).GetValueSync();
            partitionCount = describe.GetTopicDescription().GetPartitions().size();
            if (partitionCount == expectedCount) {
                break;
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(partitionCount, expectedCount);
    }

    void AssertMessageCountInTopic(TTopicClient client, const TString& topicPath, size_t expected, TDuration timeout = TDuration::Seconds(300)) {
        TInstant deadlineTime = TInstant::Now() + timeout;

        size_t count = 0;

        std::set<ui32> partitions;

        auto reader = client.CreateReadSession(
            TReadSessionSettings()
                .AutoPartitioningSupport(true)
                .AppendTopics(TTopicReadSettings(topicPath))
                .ConsumerName("consumer-1"));
        while(deadlineTime > TInstant::Now()) {
            for (auto event : reader->GetEvents(false)) {
                if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TDataReceivedEvent>(&event)) {
                    count += x->GetMessages().size();
                } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TStartPartitionSessionEvent>(&event)) {
                    partitions.insert(x->GetPartitionSession()->GetPartitionId());
                    x->Confirm();
                    Cerr << ">>>>> " << x->DebugString() << Endl << Flush;
                } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(&event)) {
                    Cerr << ">>>>> " << x->DebugString() << Endl << Flush;
                } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionStatusEvent>(&event)) {
                    Cerr << ">>>>> " << x->DebugString() << Endl << Flush;
                } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TStopPartitionSessionEvent>(&event)) {
                    x->Confirm();
                    Cerr << ">>>>> " << x->DebugString() << Endl << Flush;
                } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TPartitionSessionClosedEvent>(&event)) {
                    Cerr << ">>>>> " << x->DebugString() << Endl << Flush;
                } else if (auto* x = std::get_if<NYdb::NTopic::TReadSessionEvent::TEndPartitionSessionEvent>(&event)) {
                    x->Confirm();
                    Cerr << ">>>>> " << x->DebugString() << Endl << Flush;
                } else if (auto* sessionClosedEvent = std::get_if<NYdb::NTopic::TSessionClosedEvent>(&event)) {
                    x->Confirm();
                    Cerr << ">>>>> " << x->DebugString() << Endl << Flush;
                }

                if (count == expected) {
                    return;
                }
            }
            Sleep(TDuration::MilliSeconds(250));
        }

        UNIT_ASSERT_VALUES_EQUAL(expected, count);
        UNIT_ASSERT_C(partitions.size() > 1, "Split must be happened");
        Cerr << ">>>>> Partition count: " << partitions.size() << Endl << Flush;
    }

    Y_UNIT_TEST(CDC_Write) {
        TTopicSdkTestSetup setup = CreateSetup();
        auto client = setup.MakeClient();
        auto tableClient = setup.MakeTableClient();
        auto session = tableClient.CreateSession().GetValueSync().GetSession();

        ExecuteQuery(session, R"(
            --!syntax_v1
            CREATE TABLE `/Root/origin` (
                id UInt64,
                order UInt64,
                value Text,
                PRIMARY KEY (id, order)
            ) WITH (
                AUTO_PARTITIONING_BY_SIZE = ENABLED,
                AUTO_PARTITIONING_PARTITION_SIZE_MB = 5,
                AUTO_PARTITIONING_BY_LOAD = ENABLED,
                AUTO_PARTITIONING_MIN_PARTITIONS_COUNT = 1,
                AUTO_PARTITIONING_MAX_PARTITIONS_COUNT = 64,
                UNIFORM_PARTITIONS = 1
            );
        )");

        ExecuteQuery(session, R"(
            --!syntax_v1
            ALTER TABLE `/Root/origin`
                ADD CHANGEFEED `feed` WITH (
                    MODE = 'UPDATES',
                    FORMAT = 'JSON',
                    TOPIC_AUTO_PARTITIONING = 'ENABLED',
                    TOPIC_MIN_ACTIVE_PARTITIONS = 1,
                    TOPIC_MAX_ACTIVE_PARTITIONS = 100
                );
        )");

        {
            TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAlterPartitioningSettings()
                    .MinActivePartitions(1)
                    .MaxActivePartitions(100)
                    .BeginAlterAutoPartitioningSettings()
                        .Strategy(EAutoPartitioningStrategy::ScaleUp)
                        .StabilizationWindow(TDuration::Seconds(15))
                        .DownUtilizationPercent(1)
                        .UpUtilizationPercent(2)
                    .EndAlterAutoPartitioningSettings()
                .EndAlterTopicPartitioningSettings()
                .BeginAddConsumer()
                    .ConsumerName("consumer-1")
                .EndAddConsumer();
            auto f = client.AlterTopic("/Root/origin/feed", alterSettings);
            f.Wait();

            auto v = f.GetValueSync();
            UNIT_ASSERT_C(v.IsSuccess(),  "Error: " << v);
        }

        Cerr << ">>>>> " << TInstant::Now() << " Start table insert" << Endl << Flush;
        for (size_t i = 0; i < 50; ++i) {
            ExecuteDataQuery(session, R"(
                --!syntax_v1
                $sample = AsList(
                    AsStruct(ListFromRange(0, 5000) AS v)
                );

                UPSERT INTO `/Root/origin` (id, order, value)
                SELECT
                    RandomNumber(v) AS id,
                    CAST(v AS Uint64) AS order,
                    CAST('0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF0123456789ABCDEF' AS Utf8?)  AS value
                FROM as_table($sample)
                    FLATTEN BY (v);
            )");
        }

        Cerr << ">>>>> " << TInstant::Now() << " Start read topic" << Endl << Flush;
        AssertMessageCountInTopic(client, "/Root/origin/feed/streamImpl", 250000);
        Cerr << ">>>>> " << TInstant::Now() << " End" << Endl << Flush;
    }
}

} // namespace NKikimr
