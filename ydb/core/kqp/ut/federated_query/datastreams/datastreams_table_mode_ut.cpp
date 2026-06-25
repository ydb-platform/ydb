#include "common.h"

#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NTestUtils;
using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpFederatedQueryDatastreamsTableMode) {

    Y_UNIT_TEST_F(DisableTopicsSqlIoOperations, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(false);
        config.MutableFeatureFlags()->SetEnableTopicsPredicatePushdown(false);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        constexpr char topic[] = "disableEnableTopicsSqlIoOperations";

        ui32 partitionCount = 2;
        CreateTopic(topic, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount), /* local */ true);
        ExecQuery(fmt::format(R"(SELECT * FROM `{topic}`)", "topic"_a = topic), EStatus::SCHEME_ERROR, "Failed to load metadata");
    }

    Y_UNIT_TEST_F(ReadEmptyTopic, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutableFeatureFlags()->SetEnableTopicsPredicatePushdown(false);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        constexpr char topic[] = "readEmptyTopic";

        ui32 partitionCount = 2;
        CreateTopic(topic, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount), /* local */ true);
        auto results = ExecQuery(fmt::format(R"(SELECT * FROM `{topic}`)", "topic"_a = topic));
        CheckScriptResult(results[0], 1, 0, {});
    }

    Y_UNIT_TEST_F(ReadTopicWithAutoPartitioning, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutableFeatureFlags()->SetEnableTopicsPredicatePushdown(false);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        constexpr char topic[] = "readTopicWithAutoPartitioning";

        ui32 partitionCount = 10;
        auto autoPartSettings = NTopic::TAutoPartitioningSettings(NTopic::EAutoPartitioningStrategy::ScaleUp, TDuration::Seconds(1), 50, 50);
        CreateTopic(topic, NTopic::TCreateTopicSettings().PartitioningSettings(1, partitionCount, autoPartSettings), /* local */ true);
        for (size_t i = 0; i < 10; ++i) {
            WriteTopicMessage(topic, "data", 0, /* local */ true);
        }
        auto results = ExecQuery(fmt::format(R"(SELECT * FROM `{topic}`)", "topic"_a = topic));
        CheckScriptResult(results[0], 1, 10, [](TResultSetParser& result) {
            UNIT_ASSERT_VALUES_EQUAL(result.ColumnParser(0).GetString(), "data");
        });
    }

    Y_UNIT_TEST_F(InsertSelect, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutableFeatureFlags()->SetEnableTopicsPredicatePushdown(false);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);
        constexpr char topic_input[] = "InsertSelect_input";
        constexpr char topic_output[] = "InsertSelect_output";

        ui32 partitionCount = 2;
        CreateTopic(topic_input, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount), /* local */ true);
        CreateTopic(topic_output, NTopic::TCreateTopicSettings().PartitioningSettings(1, 1), /* local */ true);

        WriteTopicMessage(topic_input, "data", 0, /* local */ true);
        WriteTopicMessage(topic_input, "data", 1, /* local */ true);

        ExecQuery(fmt::format(R"(INSERT INTO `{topic_output}` SELECT * FROM `{topic_input}`)",
            "topic_input"_a = topic_input,
            "topic_output"_a = topic_output));
        ReadTopicMessages(topic_output, {"data", "data"}, TInstant::Now() - TDuration::Seconds(100), false, true);
    }
}

} // namespace NKikimr::NKqp
