#include <ydb/core/persqueue/ut/common/autoscaling_ut_common.h>

#include <ydb/public/sdk/cpp/src/client/topic/ut/ut_utils/topic_sdk_test_setup.h>

#include <library/cpp/testing/unittest/registar.h>
#include <ydb/core/persqueue/public/partition_key_range/partition_key_range.h>
#include <ydb/core/persqueue/pqrb/partition_scale_manager.h>
#include <ydb/core/tx/schemeshard/ut_helpers/helpers.h>
#include <ydb/core/tx/schemeshard/ut_helpers/test_env.h>

#include <util/stream/output.h>

namespace NKikimr {

    using namespace NYdb::NTopic;
    using namespace NYdb::NTopic::NTests;
    using namespace NSchemeShardUT_Private;
    using namespace NKikimr::NPQ::NTest;

    Y_UNIT_TEST_SUITE(TopicDescribe) {

        struct TMessageGroup {
            ui32 Count;
            ui32 Size;
        };

        static size_t WriteGeneratedMessagesToTestTopic(TTopicClient& client, const std::span<const TMessageGroup> groups) {
            auto writeSession = CreateWriteSession(client, "producer-1", 0, TString{TEST_TOPIC}, false);
            size_t totalCount = 0;
            for (const auto& g : groups) {
                for (ui32 i = 0; i < g.Count; ++i) {
                    UNIT_ASSERT(writeSession->Write(TWriteMessage(TString("@") * g.Size)));
                }
                totalCount += g.Count;
            }
            UNIT_ASSERT(writeSession->Close());
            return totalCount;
        }

        void StartOffsetTest(std::span<const TMessageGroup> groups) {
            TTopicSdkTestSetup setup = CreateSetup();
            TTopicClient client = setup.MakeClient();
            TCreateTopicSettings createSettings = TCreateTopicSettings().PartitioningSettings(1, 1);
            client.CreateTopic(TEST_TOPIC, createSettings).GetValueSync();
            const size_t totalCount = WriteGeneratedMessagesToTestTopic(client, groups);
            Sleep(TDuration::Seconds(3)); // wait for compaction iteration

            const auto describe = client.DescribeTopic(TEST_TOPIC, TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), NYdb::EStatus::SUCCESS, describe.GetIssues().ToString());
            const TTopicDescription& description = describe.GetTopicDescription();
            UNIT_ASSERT_VALUES_EQUAL(description.GetPartitions().size(), 1);
            const TPartitionInfo& partition0Info = description.GetPartitions().at(0);
            UNIT_ASSERT(partition0Info.GetPartitionStats().has_value());
            const NYdb::NTopic::TPartitionStats& partition0Stats = partition0Info.GetPartitionStats().value();
            UNIT_ASSERT_VALUES_EQUAL(partition0Stats.GetEndOffset(), totalCount);
            UNIT_ASSERT_VALUES_EQUAL(partition0Stats.GetStartOffset(), 0);
        }

        Y_UNIT_TEST(BasicStartOffset) {
            constexpr TMessageGroup messages[]{
                {30, 15},
            };
            StartOffsetTest(messages);
        }

        Y_UNIT_TEST(CompactionPreserveStartOffset) {
            constexpr TMessageGroup messages[]{
                {500, 1},
                {2, 8_MB},
                {500, 1},
            };
            StartOffsetTest(messages);
        }

        Y_UNIT_TEST(RetentionChangesStartOffset) {
            constexpr TMessageGroup groups[]{
                {789, 1},
                {4, 8_MB},
                {567, 1},
            };
            TTopicSdkTestSetup setup = CreateSetup();
            TTopicClient client = setup.MakeClient();
            TCreateTopicSettings createSettings = TCreateTopicSettings().PartitioningSettings(1, 1).RetentionStorageMb(4);
            client.CreateTopic(TEST_TOPIC, createSettings).GetValueSync();
            const size_t totalCount = WriteGeneratedMessagesToTestTopic(client, groups);

            const auto describe = client.DescribeTopic(TEST_TOPIC, TDescribeTopicSettings().IncludeStats(true)).GetValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(describe.GetStatus(), NYdb::EStatus::SUCCESS, describe.GetIssues().ToString());
            const TTopicDescription& description = describe.GetTopicDescription();
            UNIT_ASSERT_VALUES_EQUAL(description.GetPartitions().size(), 1);
            const TPartitionInfo& partition0Info = description.GetPartitions().at(0);
            UNIT_ASSERT(partition0Info.GetPartitionStats().has_value());
            const NYdb::NTopic::TPartitionStats& partition0Stats = partition0Info.GetPartitionStats().value();
            const TString textComment = TStringBuilder() << LabeledOutput(partition0Stats.GetStartOffset(), partition0Stats.GetEndOffset(), totalCount);
            UNIT_ASSERT_VALUES_EQUAL_C(partition0Stats.GetEndOffset(), totalCount, textComment);
            UNIT_ASSERT_LT_C(partition0Stats.GetStartOffset(), totalCount - 567, textComment);
            UNIT_ASSERT_GE_C(partition0Stats.GetStartOffset(), 789, textComment);
        }

    } // Y_UNIT_TEST_SUITE(TopicDescribe)
} // namespace NKikimr
