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

    Y_UNIT_TEST_SUITE(Describe) {

        void DescribeTopic(NPersQueue::NTests::TPersQueueYdbSdkTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation, bool killTablets)
        {
            TDescribeTopicSettings settings;
            settings.IncludeStats(requireStats);
            settings.IncludeLocation(requireLocation);

            {
                auto result = client.DescribeTopic(setup.GetTestTopicPath(), settings).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                const auto& description = result.GetTopicDescription();

                const auto& partitions = description.GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);

                const auto& partition = partitions[0];
                UNIT_ASSERT(partition.GetActive());
                UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), 0);

                if (requireStats)
                {
                    const auto& stats = description.GetTopicStats();

                    if (requireNonEmptyStats)
                    {
                        UNIT_ASSERT_GT(stats.GetStoreSizeBytes(), 0);
                        UNIT_ASSERT_GT(stats.GetBytesWrittenPerMinute(), 0);
                        UNIT_ASSERT_GT(stats.GetBytesWrittenPerHour(), 0);
                        UNIT_ASSERT_GT(stats.GetBytesWrittenPerDay(), 0);
                        UNIT_ASSERT_GT(stats.GetMaxWriteTimeLag(), TDuration::Zero());
                        UNIT_ASSERT_GT(stats.GetMinLastWriteTime(), TInstant::Zero());
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(stats.GetStoreSizeBytes(), 0);
                    }
                }

                if (requireLocation)
                {
                    UNIT_ASSERT(partition.GetPartitionLocation());
                    const auto& partitionLocation = *partition.GetPartitionLocation();
                    UNIT_ASSERT_GT(partitionLocation.GetNodeId(), 0);
                    UNIT_ASSERT_GE(partitionLocation.GetGeneration(), 0); // greater-or-equal 0
                }
            }

            if (killTablets)
            {
                setup.KillTopicTablets(setup.GetTestTopicPath());

                auto result = client.DescribeTopic(setup.GetTestTopicPath(), settings).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                const auto& description = result.GetTopicDescription();

                const auto& partitions = description.GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);

                const auto& partition = partitions[0];
                UNIT_ASSERT(partition.GetActive());
                UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), 0);

                if (requireStats)
                {
                    const auto& stats = description.GetTopicStats();

                    if (requireNonEmptyStats)
                    {
                        UNIT_ASSERT_GT(stats.GetStoreSizeBytes(), 0);
                        UNIT_ASSERT_GT(stats.GetBytesWrittenPerMinute(), 0);
                        UNIT_ASSERT_GT(stats.GetBytesWrittenPerHour(), 0);
                        UNIT_ASSERT_GT(stats.GetBytesWrittenPerDay(), 0);
                        UNIT_ASSERT_GT(stats.GetMaxWriteTimeLag(), TDuration::Zero());
                        UNIT_ASSERT_GT(stats.GetMinLastWriteTime(), TInstant::Zero());
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(stats.GetStoreSizeBytes(), 0);
                    }
                }

                if (requireLocation)
                {
                    UNIT_ASSERT(partition.GetPartitionLocation());
                    const auto& partitionLocation = *partition.GetPartitionLocation();
                    UNIT_ASSERT_GT(partitionLocation.GetNodeId(), 0);
                    UNIT_ASSERT_GT(partitionLocation.GetGeneration(), 0); // greater-then 0 after tablet restart
                }
            }
        }

        void DescribeConsumer(NPersQueue::NTests::TPersQueueYdbSdkTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation, bool killTablets)
        {
            TDescribeConsumerSettings settings;
            settings.IncludeStats(requireStats);
            settings.IncludeLocation(requireLocation);

            {
                auto result = client.DescribeConsumer(setup.GetTestTopicPath(), ::NPersQueue::SDKTestSetup::GetTestConsumer(), settings).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                const auto& description = result.GetConsumerDescription();

                const auto& partitions = description.GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);

                const auto& partition = partitions[0];
                UNIT_ASSERT(partition.GetActive());
                UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), 0);

                if (requireStats)
                {
                    const auto& stats = partition.GetPartitionStats();
                    const auto& consumerStats = partition.GetPartitionConsumerStats();
                    UNIT_ASSERT(stats);
                    UNIT_ASSERT(consumerStats);

                    if (requireNonEmptyStats)
                    {
                        UNIT_ASSERT_GE(stats->GetStartOffset(), 0);
                        UNIT_ASSERT_GE(stats->GetEndOffset(), 0);
                        UNIT_ASSERT_GT(stats->GetStoreSizeBytes(), 0);
                        UNIT_ASSERT_GT(stats->GetLastWriteTime(), TInstant::Zero());
                        UNIT_ASSERT_GT(stats->GetMaxWriteTimeLag(), TDuration::Zero());
                        UNIT_ASSERT_GT(stats->GetBytesWrittenPerMinute(), 0);
                        UNIT_ASSERT_GT(stats->GetBytesWrittenPerHour(), 0);
                        UNIT_ASSERT_GT(stats->GetBytesWrittenPerDay(), 0);

                        UNIT_ASSERT_GT(consumerStats->GetLastReadOffset(), 0);
                        UNIT_ASSERT_GT(consumerStats->GetCommittedOffset(), 0);
                        UNIT_ASSERT_GE(consumerStats->GetReadSessionId(), 0);
                        UNIT_ASSERT_VALUES_EQUAL(consumerStats->GetReaderName(), "");
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(stats->GetStartOffset(), 0);
                        UNIT_ASSERT_VALUES_EQUAL(consumerStats->GetLastReadOffset(), 0);
                    }
                }

                if (requireLocation)
                {
                    UNIT_ASSERT(partition.GetPartitionLocation());
                    const auto& partitionLocation = *partition.GetPartitionLocation();
                    UNIT_ASSERT_GT(partitionLocation.GetNodeId(), 0);
                    UNIT_ASSERT_GE(partitionLocation.GetGeneration(), 0); // greater-or-equal 0
                }
            }

            if (killTablets)
            {
                setup.KillTopicTablets(setup.GetTestTopicPath());

                auto result = client.DescribeConsumer(setup.GetTestTopicPath(), ::NPersQueue::SDKTestSetup::GetTestConsumer(), settings).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                const auto& description = result.GetConsumerDescription();

                const auto& partitions = description.GetPartitions();
                UNIT_ASSERT_VALUES_EQUAL(partitions.size(), 1);

                const auto& partition = partitions[0];
                UNIT_ASSERT(partition.GetActive());
                UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), 0);

                if (requireLocation)
                {
                    UNIT_ASSERT(partition.GetPartitionLocation());
                    const auto& partitionLocation = *partition.GetPartitionLocation();
                    UNIT_ASSERT_GT(partitionLocation.GetNodeId(), 0);
                    UNIT_ASSERT_GT(partitionLocation.GetGeneration(), 0); // greater-then 0 after tablet restart
                }
            }
        }

        void DescribePartition(NPersQueue::NTests::TPersQueueYdbSdkTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation, bool killTablets)
        {
            TDescribePartitionSettings settings;
            settings.IncludeStats(requireStats);
            settings.IncludeLocation(requireLocation);

            i64 testPartitionId = 0;

            {
                auto result = client.DescribePartition(setup.GetTestTopicPath(), testPartitionId, settings).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                const auto& description = result.GetPartitionDescription();

                const auto& partition = description.GetPartition();
                UNIT_ASSERT(partition.GetActive());
                UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), testPartitionId);

                if (requireStats)
                {
                    const auto& stats = partition.GetPartitionStats();
                    UNIT_ASSERT(stats);

                    if (requireNonEmptyStats)
                    {
                        UNIT_ASSERT_GE(stats->GetStartOffset(), 0);
                        UNIT_ASSERT_GE(stats->GetEndOffset(), 0);
                        UNIT_ASSERT_GT(stats->GetStoreSizeBytes(), 0);
                        UNIT_ASSERT_GT(stats->GetLastWriteTime(), TInstant::Zero());
                        UNIT_ASSERT_GT(stats->GetMaxWriteTimeLag(), TDuration::Zero());
                        UNIT_ASSERT_GT(stats->GetBytesWrittenPerMinute(), 0);
                        UNIT_ASSERT_GT(stats->GetBytesWrittenPerHour(), 0);
                        UNIT_ASSERT_GT(stats->GetBytesWrittenPerDay(), 0);
                    } else {
                        UNIT_ASSERT_VALUES_EQUAL(stats->GetStoreSizeBytes(), 0);
                    }
                }

                if (requireLocation)
                {
                    UNIT_ASSERT(partition.GetPartitionLocation());
                    const auto& partitionLocation = *partition.GetPartitionLocation();
                    UNIT_ASSERT_GT(partitionLocation.GetNodeId(), 0);
                    UNIT_ASSERT_GE(partitionLocation.GetGeneration(), 0); // greater-or-equal 0
                }
            }

            if (killTablets)
            {
                setup.KillTopicTablets(setup.GetTestTopicPath());

                auto result = client.DescribePartition(setup.GetTestTopicPath(), testPartitionId, settings).GetValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToString());

                const auto& description = result.GetPartitionDescription();

                const auto& partition = description.GetPartition();
                UNIT_ASSERT(partition.GetActive());
                UNIT_ASSERT_VALUES_EQUAL(partition.GetPartitionId(), testPartitionId);

                if (requireLocation)
                {
                    UNIT_ASSERT(partition.GetPartitionLocation());
                    const auto& partitionLocation = *partition.GetPartitionLocation();
                    UNIT_ASSERT_GT(partitionLocation.GetNodeId(), 0);
                    UNIT_ASSERT_GT(partitionLocation.GetGeneration(), 0); // greater-then 0 after tablet restart
                }
            }
        }

        Y_UNIT_TEST(Basic) {
            NPersQueue::NTests::TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
            TTopicClient client(setup.GetDriver());

            DescribeTopic(setup, client, false, false, false, false);
            DescribeConsumer(setup, client, false, false, false, false);
            DescribePartition(setup, client, false, false, false, false);
        }

        Y_UNIT_TEST(Statistics) {
            NPersQueue::NTests::TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
            TTopicClient client(setup.GetDriver());

            // Get empty description
            DescribeTopic(setup, client, true, false, false, false);
            DescribeConsumer(setup, client, true, false, false, false);
            DescribePartition(setup, client, true, false, false, false);

            // Write a message
            {
                auto writeSettings = TWriteSessionSettings().Path(setup.GetTestTopicPath()).MessageGroupId(::NPersQueue::SDKTestSetup::GetTestMessageGroupId());
                auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
                std::string message(10_KB, 'x');
                UNIT_ASSERT(writeSession->Write(message));
                writeSession->Close();
            }

            // Read a message
            {
                auto readSettings = TReadSessionSettings()
                                        .ConsumerName(setup.GetTestConsumer())
                                        .AppendTopics(setup.GetTestTopicPath());
                auto readSession = client.CreateReadSession(readSettings);

                auto event = readSession->GetEvent(true);
                UNIT_ASSERT(event.Defined());

                auto& startPartitionSession = std::get<TReadSessionEvent::TStartPartitionSessionEvent>(*event);
                startPartitionSession.Confirm();

                event = readSession->GetEvent(true);
                UNIT_ASSERT(event.Defined());

                auto& dataReceived = std::get<TReadSessionEvent::TDataReceivedEvent>(*event);
                dataReceived.Commit();
            }

            // Get non-empty description
            DescribeTopic(setup, client, true, true, false, false);
            DescribeConsumer(setup, client, true, true, false, false);
            DescribePartition(setup, client, true, true, false, false);
        }

        Y_UNIT_TEST(Location) {
            NPersQueue::NTests::TPersQueueYdbSdkTestSetup setup(TEST_CASE_NAME);
            TTopicClient client(setup.GetDriver());

            DescribeTopic(setup, client, false, false, true, false);
            DescribeConsumer(setup, client, false, false, true, false);
            DescribePartition(setup, client, false, false, true, false);

            // Describe with KillTablets
            DescribeTopic(setup, client, false, false, true, true);
            DescribeConsumer(setup, client, false, false, true, true);
            DescribePartition(setup, client, false, false, true, true);
        }
    }
}
