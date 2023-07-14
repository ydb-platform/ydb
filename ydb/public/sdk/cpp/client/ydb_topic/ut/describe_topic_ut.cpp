#include <ydb/library/persqueue/topic_parser_public/topic_parser.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/ut/managed_executor.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/impl/common.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_core/ut/ut_utils/ut_utils.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

#include <future>

namespace NYdb::NTopic::NTests {

    Y_UNIT_TEST_SUITE(DescribeTopic) {
        Y_UNIT_TEST(Basic) {
            auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
            TTopicClient client(setup->GetDriver());

            auto result = client.DescribeTopic(setup->GetTestTopicPath()).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

            const auto& description = result.GetTopicDescription();

            auto& partitions = description.GetPartitions();
            UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);

            auto& partition = partitions[0];
            UNIT_ASSERT(partition.GetActive());
            UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), 0);
        }

        Y_UNIT_TEST(Statistics) {
            auto setup = std::make_shared<NPersQueue::NTests::TPersQueueYdbSdkTestSetup>(TEST_CASE_NAME);
            TTopicClient client(setup->GetDriver());

            TDescribeTopicSettings settings;
            settings.IncludeStats(true);

            // Get empty topic description
            {
                auto result = client.DescribeTopic(setup->GetTestTopicPath(), settings).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                auto& description = result.GetTopicDescription();
                UNIT_ASSERT_VALUES_EQUAL(description.GetPartitions().size(), 1);

                auto& stats = description.GetTopicStats();
                UNIT_ASSERT_VALUES_EQUAL(stats.GetStoreSizeBytes(), 0);
            }

            // Write a message
            {
                auto writeSettings = TWriteSessionSettings().Path(setup->GetTestTopic()).MessageGroupId(::NPersQueue::SDKTestSetup::GetTestMessageGroupId());
                auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
                std::string message(10_KB, 'x');
                UNIT_ASSERT(writeSession->Write(message));
                writeSession->Close();
            }

            // Get non-empty topic description
            {
                auto result = client.DescribeTopic(setup->GetTestTopicPath(), settings).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                auto& description = result.GetTopicDescription();

                auto& stats = description.GetTopicStats();
                UNIT_ASSERT_GT(stats.GetStoreSizeBytes(), 0);
                UNIT_ASSERT_GT(stats.GetBytesWrittenPerMinute(), 0);
                UNIT_ASSERT_GT(stats.GetBytesWrittenPerHour(), 0);
                UNIT_ASSERT_GT(stats.GetBytesWrittenPerDay(), 0);
                UNIT_ASSERT_GT(stats.GetMaxWriteTimeLag(), TDuration::Zero());
                UNIT_ASSERT_GT(stats.GetMinLastWriteTime(), TInstant::Zero());

                auto& partitions = description.GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);

                auto& partition = partitions[0];
                UNIT_ASSERT(partition.GetActive());
                UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), 0);

                auto& partitionStats = *partition.GetPartitionStats();
                UNIT_ASSERT_VALUES_EQUAL(partitionStats.GetStartOffset(), 0);
                UNIT_ASSERT_GT(partitionStats.GetEndOffset(), 0);
                UNIT_ASSERT_GT(partitionStats.GetStoreSizeBytes(), 0);
                UNIT_ASSERT_GT(partitionStats.GetBytesWrittenPerMinute(), 0);
                UNIT_ASSERT_GT(partitionStats.GetBytesWrittenPerHour(), 0);
                UNIT_ASSERT_GT(partitionStats.GetBytesWrittenPerDay(), 0);
                UNIT_ASSERT_GT(partitionStats.GetMaxWriteTimeLag(), TDuration::Zero());
                UNIT_ASSERT_GT(partitionStats.GetLastWriteTime(), TInstant::Zero());
            }
        }
    }
}
