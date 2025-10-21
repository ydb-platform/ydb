#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/client.h>

#include <ydb/public/sdk/cpp/tests/integration/topic/utils/describe.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NYdb::inline Dev::NTopic::NTests {

    Y_UNIT_TEST_SUITE(Describe) {

        void DescribeTopicWithKillTablets(TTopicSdkTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation)
        {
            TDescribeTopicSettings settings;
            settings.IncludeStats(requireStats);
            settings.IncludeLocation(requireLocation);

            DescribeTopicTest(setup, client, requireStats, requireNonEmptyStats, requireLocation);

            setup.GetServer().KillTopicPqTablets(setup.GetFullTopicPath());

            auto result = client.DescribeTopic(setup.GetTopicPath(), settings).GetValueSync();
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

        void DescribeConsumerWithKillTablets(TTopicSdkTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation)
        {
            TDescribeConsumerSettings settings;
            settings.IncludeStats(requireStats);
            settings.IncludeLocation(requireLocation);

            DescribeConsumerTest(setup, client, requireStats, requireNonEmptyStats, requireLocation);

            setup.GetServer().KillTopicPqTablets(setup.GetFullTopicPath());

            auto result = client.DescribeConsumer(setup.GetTopicPath(), setup.GetConsumerName(), settings).GetValueSync();
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

        void DescribePartitionWithKillTablets(TTopicSdkTestSetup& setup, TTopicClient& client, bool requireStats, bool requireNonEmptyStats, bool requireLocation)
        {
            TDescribePartitionSettings settings;
            settings.IncludeStats(requireStats);
            settings.IncludeLocation(requireLocation);

            std::int64_t testPartitionId = 0;

            DescribePartitionTest(setup, client, requireStats, requireNonEmptyStats, requireLocation);

            setup.GetServer().KillTopicPqTablets(setup.GetFullTopicPath());

            auto result = client.DescribePartition(setup.GetTopicPath(), testPartitionId, settings).GetValueSync();
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

        Y_UNIT_TEST(LocationWithKillTablets) {
            TTopicSdkTestSetup setup(TEST_CASE_NAME);
            TTopicClient client = setup.MakeClient();

            // Describe with KillTablets
            DescribeTopicWithKillTablets(setup, client, false, false, true);
            DescribeConsumerWithKillTablets(setup, client, false, false, true);
            DescribePartitionWithKillTablets(setup, client, false, false, true);
        }

        TDescribePartitionResult RunPermissionTest(TTopicSdkTestSetup& setup, int userId, bool existingTopic, bool allowUpdateRow, bool allowDescribeSchema) {
            std::string authToken = TStringBuilder() << "x-user-" << userId << "@builtin";
            std::cerr << std::format("=== existingTopic={} allowUpdateRow={} allowDescribeSchema={} authToken={}\n",
                                existingTopic, allowUpdateRow, allowDescribeSchema, std::string(authToken));

            setup.GetServer().AnnoyingClient->GrantConnect(TString(authToken));

            auto driverConfig = setup.MakeDriverConfig().SetAuthToken(authToken);
            auto client = TTopicClient(TDriver(driverConfig));
            auto settings = TDescribePartitionSettings().IncludeLocation(true);
            std::int64_t testPartitionId = 0;

            NACLib::TDiffACL acl;
            if (allowDescribeSchema) {
                acl.AddAccess(NACLib::EAccessType::Allow, NACLib::DescribeSchema, NACLib::TSID(authToken));
            }
            if (allowUpdateRow) {
                acl.AddAccess(NACLib::EAccessType::Allow, NACLib::UpdateRow, NACLib::TSID(authToken));
            }
            setup.GetServer().AnnoyingClient->ModifyACL("/Root", setup.GetTopicPath(), acl.SerializeAsString());

            while (true) { 
                TDescribePartitionResult result = client.DescribePartition(existingTopic ? setup.GetTopicPath() : "bad-topic", testPartitionId, settings).GetValueSync();
                UNIT_ASSERT_C(result.GetStatus() == EStatus::SUCCESS || result.GetStatus() == EStatus::SCHEME_ERROR || result.GetStatus() == EStatus::UNAUTHORIZED, result.GetIssues());
                // Connect access may appear later
                if (result.GetStatus() != EStatus::UNAUTHORIZED) {
                    return result;
                }
                std::this_thread::sleep_for(std::chrono::seconds(1));
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
                std::cerr << line << " issues=" << result.GetIssues().ToOneLineString() << std::endl;

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
