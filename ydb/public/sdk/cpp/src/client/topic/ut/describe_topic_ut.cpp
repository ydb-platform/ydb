#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <library/cpp/testing/unittest/registar.h>

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
                        UNIT_ASSERT_GE(TString{consumerStats->GetReadSessionId()}, 0);
                        UNIT_ASSERT_VALUES_EQUAL(consumerStats->GetReaderName(), "");
                        UNIT_ASSERT_GE(consumerStats->GetMaxWriteTimeLag(), TDuration::Seconds(100));
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

        Y_UNIT_TEST(LocationWithKillTablets) {
            TTopicSdkTestSetup setup(TEST_CASE_NAME);
            TTopicClient client = setup.MakeClient();

            // Describe with KillTablets
            DescribeTopic(setup, client, false, false, true, true);
            DescribeConsumer(setup, client, false, false, true, true);
            DescribePartition(setup, client, false, false, true, true);
        }

        TDescribePartitionResult RunPermissionTest(TTopicSdkTestSetup& setup, int userId, bool existingTopic, bool allowUpdateRow, bool allowDescribeSchema) {
            TString authToken = TStringBuilder() << "x-user-" << userId << "@builtin";
            Cerr << std::format("=== existingTopic={} allowUpdateRow={} allowDescribeSchema={} authToken={}\n",
                                existingTopic, allowUpdateRow, allowDescribeSchema, std::string(authToken));

            setup.GetServer().AnnoyingClient->GrantConnect(authToken);

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
            setup.GetServer().AnnoyingClient->ModifyACL("/Root", TString{TEST_TOPIC}, acl.SerializeAsString());

            while (true) { 
                TDescribePartitionResult result = client.DescribePartition(existingTopic ? TEST_TOPIC : "bad-topic", testPartitionId, settings).GetValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS || result.GetStatus() == EStatus::SCHEME_ERROR || result.GetStatus() == EStatus::UNAUTHORIZED, result.GetIssues());
                // Connect access may appear later
                if (result.GetStatus() != EStatus::UNAUTHORIZED)
                    return result;
                Sleep(TDuration::Seconds(1));
            }
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
                NYdb::NIssue::TIssueCode issueCode;
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
                NYdb::NIssue::TIssueCode resultIssue = 0;
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
                    UNIT_ASSERT(p.GetPartitionLocation().has_value());
                }
            }
        }
    }
}
