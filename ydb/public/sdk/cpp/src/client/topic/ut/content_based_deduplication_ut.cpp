#include "ut_utils/topic_sdk_test_setup.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/topic/control_plane.h>

namespace NYdb::inline Dev::NTopic::NTests {

Y_UNIT_TEST_SUITE(ContentBasedDeduplicationTests) {

    Y_UNIT_TEST(CreateTopicWithContentBasedDeduplication) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        auto client = setup.MakeClient();
        auto path = setup.GetFullTopicPath();

        // Create topic with content-based deduplication enabled
        TCreateTopicSettings settings;
        settings
            .ContentBasedDeduplication(true)
            .BeginAddConsumer()
                .ConsumerName(TEST_CONSUMER)
            .EndAddConsumer();

        auto createStatus = client.CreateTopic(path, settings).GetValueSync();
        UNIT_ASSERT(createStatus.IsSuccess());

        // Describe the topic and verify content-based deduplication is enabled
        auto describeResult = client.DescribeTopic(path).GetValueSync();
        UNIT_ASSERT(describeResult.IsSuccess());

        const auto& description = describeResult.GetTopicDescription();
        UNIT_ASSERT(description.GetContentBasedDeduplication());
    }

    Y_UNIT_TEST(AlterTopicEnableContentBasedDeduplication) {
        TTopicSdkTestSetup setup{TEST_CASE_NAME, TTopicSdkTestSetup::MakeServerSettings(), false};
        auto client = setup.MakeClient();
        auto path = setup.GetFullTopicPath();

        // Create topic without content-based deduplication
        TCreateTopicSettings createSettings;
        createSettings
            .BeginAddConsumer()
                .ConsumerName(TEST_CONSUMER)
            .EndAddConsumer();

        auto createStatus = client.CreateTopic(path, createSettings).GetValueSync();
        UNIT_ASSERT(createStatus.IsSuccess());

        // Verify initial state
        auto describeResult1 = client.DescribeTopic(path).GetValueSync();
        UNIT_ASSERT(describeResult1.IsSuccess());
        UNIT_ASSERT(!describeResult1.GetTopicDescription().GetContentBasedDeduplication());

        // Alter topic to enable content-based deduplication
        TAlterTopicSettings alterSettings;
        alterSettings.SetContentBasedDeduplication(true);
        auto alterStatus = client.AlterTopic(path, alterSettings).GetValueSync();
        UNIT_ASSERT(alterStatus.IsSuccess());

        // Verify content-based deduplication is now enabled
        auto describeResult2 = client.DescribeTopic(path).GetValueSync();
        UNIT_ASSERT(describeResult2.IsSuccess());
        UNIT_ASSERT(describeResult2.GetTopicDescription().GetContentBasedDeduplication());

        // Alter topic to disable content-based deduplication
        alterSettings.SetContentBasedDeduplication(false);
        alterStatus = client.AlterTopic(path, alterSettings).GetValueSync();
        UNIT_ASSERT(alterStatus.IsSuccess());

        // Verify content-based deduplication is now disabled
        auto describeResult3 = client.DescribeTopic(path).GetValueSync();
        UNIT_ASSERT(describeResult3.IsSuccess());
        UNIT_ASSERT(!describeResult3.GetTopicDescription().GetContentBasedDeduplication());
    }

} // Y_UNIT_TEST_SUITE(ContentBasedDeduplicationTests)

} // namespace NYdb::inline Dev::NTopic::NTests
