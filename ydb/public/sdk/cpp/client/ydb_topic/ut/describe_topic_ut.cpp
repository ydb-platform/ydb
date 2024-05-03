#include "ut_utils/topic_sdk_test_setup.h"

#include <format>
#include <ydb/library/persqueue/topic_parser_public/topic_parser.h>
#include <ydb/public/api/protos/persqueue_error_codes_v1.pb.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/topic.h>
#include <ydb/public/sdk/cpp/client/ydb_persqueue_public/persqueue.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/common/log_lazy.h>
#include <ydb/public/sdk/cpp/client/ydb_topic/impl/common.h>

#include <library/cpp/testing/unittest/registar.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <library/cpp/threading/future/future.h>
#include <library/cpp/threading/future/async.h>

namespace NYdb::NTopic::NTests {

    Y_UNIT_TEST_SUITE(Describe) {

        void DescribeTopic(TTopicSdkTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation, bool killTablets)
        {
            TDescribeTopicSettings settings;
            settings.IncludeStats(requireStats);
            settings.IncludeLocation(requireLocation);

            {
                auto result = client.DescribeTopic(TEST_TOPIC, settings).GetValueSync();
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
                setup.GetServer().KillTopicPqTablets(setup.GetTopicPath());

                auto result = client.DescribeTopic(TEST_TOPIC, settings).GetValueSync();
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

        void DescribeConsumer(TTopicSdkTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation, bool killTablets)
        {
            TDescribeConsumerSettings settings;
            settings.IncludeStats(requireStats);
            settings.IncludeLocation(requireLocation);

            {
                auto result = client.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER, settings).GetValueSync();
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
                setup.GetServer().KillTopicPqTablets(setup.GetTopicPath());

                auto result = client.DescribeConsumer(TEST_TOPIC, TEST_CONSUMER, settings).GetValueSync();
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

        void DescribePartition(TTopicSdkTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation, bool killTablets)
        {
            TDescribePartitionSettings settings;
            settings.IncludeStats(requireStats);
            settings.IncludeLocation(requireLocation);

            i64 testPartitionId = 0;

            {
                auto result = client.DescribePartition(TEST_TOPIC, testPartitionId, settings).GetValueSync();
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
                setup.GetServer().KillTopicPqTablets(setup.GetTopicPath());

                auto result = client.DescribePartition(TEST_TOPIC, testPartitionId, settings).GetValueSync();
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
            TTopicSdkTestSetup setup(TEST_CASE_NAME);
            TTopicClient client = setup.MakeClient();

            DescribeTopic(setup, client, false, false, false, false);
            DescribeConsumer(setup, client, false, false, false, false);
            DescribePartition(setup, client, false, false, false, false);
        }

        Y_UNIT_TEST(Statistics) {
            TTopicSdkTestSetup setup(TEST_CASE_NAME);
            TTopicClient client = setup.MakeClient();

            // Get empty description
            DescribeTopic(setup, client, true, false, false, false);
            DescribeConsumer(setup, client, true, false, false, false);
            DescribePartition(setup, client, true, false, false, false);

            const size_t messagesCount = 1;

            // Write a message
            {
                auto writeSettings = TWriteSessionSettings().Path(TEST_TOPIC).MessageGroupId(TEST_MESSAGE_GROUP_ID).Codec(ECodec::RAW);
                auto writeSession = client.CreateSimpleBlockingWriteSession(writeSettings);
                std::string message(32_MB, 'x');

                for(size_t i = 0; i < messagesCount; ++i) {
                    UNIT_ASSERT(writeSession->Write(message));
                }
                writeSession->Close();
            }

            // Read a message
            {
                auto readSettings = TReadSessionSettings().ConsumerName(TEST_CONSUMER).AppendTopics(TEST_TOPIC);
                auto readSession = client.CreateReadSession(readSettings);

                // Event 1: start partition session
                {
                    TMaybe<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
                    UNIT_ASSERT(event);
                    auto startPartitionSession = std::get_if<TReadSessionEvent::TStartPartitionSessionEvent>(event.Get());
                    UNIT_ASSERT_C(startPartitionSession, DebugString(*event));

                    startPartitionSession->Confirm();
                }

                // Event 2: data received
                {
                    TMaybe<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
                    UNIT_ASSERT(event);
                    auto dataReceived = std::get_if<TReadSessionEvent::TDataReceivedEvent>(event.Get());
                    UNIT_ASSERT_C(dataReceived, DebugString(*event));

                    dataReceived->Commit();
                }

                // Event 3: commit acknowledgement
                {
                    TMaybe<TReadSessionEvent::TEvent> event = readSession->GetEvent(true);
                    UNIT_ASSERT(event);
                    auto commitOffsetAck = std::get_if<TReadSessionEvent::TCommitOffsetAcknowledgementEvent>(event.Get());

                    UNIT_ASSERT_C(commitOffsetAck, DebugString(*event));

                    UNIT_ASSERT_VALUES_EQUAL(commitOffsetAck->GetCommittedOffset(), messagesCount);
                }
            }

            // Get non-empty description
            DescribeTopic(setup, client, true, true, false, false);
            DescribeConsumer(setup, client, true, true, false, false);
            DescribePartition(setup, client, true, true, false, false);
        }

        Y_UNIT_TEST(Location) {
            TTopicSdkTestSetup setup(TEST_CASE_NAME);
            TTopicClient client = setup.MakeClient();

            DescribeTopic(setup, client, false, false, true, false);
            DescribeConsumer(setup, client, false, false, true, false);
            DescribePartition(setup, client, false, false, true, false);

            // Describe with KillTablets
            DescribeTopic(setup, client, false, false, true, true);
            DescribeConsumer(setup, client, false, false, true, true);
            DescribePartition(setup, client, false, false, true, true);
        }

        TDescribePartitionResult RunPermissionTest(TTopicSdkTestSetup& setup, int userId, bool existingTopic, bool allowUpdateRow, bool allowDescribeSchema) {
            TString authToken = TStringBuilder() << "x-user-" << userId << "@builtin";
            Cerr << std::format("=== existingTopic={} allowUpdateRow={} allowDescribeSchema={} authToken={}\n",
                                existingTopic, allowUpdateRow, allowDescribeSchema, std::string(authToken));

            auto driverConfig = setup.MakeDriverConfig().SetAuthToken(authToken);
            auto client = TTopicClient(TDriver(driverConfig));
            auto settings = TDescribePartitionSettings().IncludeLocation(true);
            i64 testPartitionId = 0;

            NACLib::TDiffACL acl;
            if (allowDescribeSchema) {
                acl.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, authToken);
            }
            if (allowUpdateRow) {
                acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, authToken);
            }
            setup.GetServer().AnnoyingClient->ModifyACL("/Root", TEST_TOPIC, acl.SerializeAsString());

            return client.DescribePartition(existingTopic ? TEST_TOPIC : "bad-topic", testPartitionId, settings).GetValueSync();
        }

        Y_UNIT_TEST(DescribePartitionPermissions) {
            TTopicSdkTestSetup setup(TEST_CASE_NAME);
            setup.GetServer().EnableLogs({NKikimrServices::TX_PROXY_SCHEME_CACHE, NKikimrServices::SCHEME_BOARD_SUBSCRIBER}, NActors::NLog::PRI_TRACE);

            int userId = 0;

            struct Expectation {
                bool existingTopic;
                bool allowUpdateRow;
                bool allowDescribeSchema;
                EStatus status;
                NYql::TIssueCode issueCode;
            };

            std::vector<Expectation> expectations{
                {0, 0, 0, EStatus::SCHEME_ERROR, Ydb::PersQueue::ErrorCode::ACCESS_DENIED},
                {0, 0, 1, EStatus::SCHEME_ERROR, Ydb::PersQueue::ErrorCode::ACCESS_DENIED},
                {0, 1, 0, EStatus::SCHEME_ERROR, Ydb::PersQueue::ErrorCode::ACCESS_DENIED},
                {0, 1, 1, EStatus::SCHEME_ERROR, Ydb::PersQueue::ErrorCode::ACCESS_DENIED},
                {1, 0, 0, EStatus::SCHEME_ERROR, Ydb::PersQueue::ErrorCode::ACCESS_DENIED},
                {1, 0, 1, EStatus::SUCCESS, 0},
                {1, 1, 0, EStatus::SUCCESS, 0},
                {1, 1, 1, EStatus::SUCCESS, 0},
            };

            for (auto [existing, update, describe, status, issue] : expectations) {
                auto result = RunPermissionTest(setup, userId++, existing, update, describe);
                auto resultStatus = result.GetStatus();
                auto line = TStringBuilder() << "=== status=" << resultStatus;
                NYql::TIssueCode resultIssue = 0;
                if (!result.GetIssues().Empty()) {
                    resultIssue = result.GetIssues().begin()->GetCode();
                    line << " issueCode=" << resultIssue;
                }
                Cerr << (line << " issues=" << result.GetIssues().ToOneLineString() << Endl);

                UNIT_ASSERT_EQUAL(resultStatus, status);
                UNIT_ASSERT_EQUAL(resultIssue, issue);
                if (resultStatus == EStatus::SUCCESS) {
                    auto& p = result.GetPartitionDescription().GetPartition();
                    UNIT_ASSERT(p.GetActive());
                    UNIT_ASSERT(p.GetPartitionLocation().Defined());
                }
            }
        }
    }
}
