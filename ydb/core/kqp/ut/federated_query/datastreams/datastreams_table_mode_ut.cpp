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

    Y_UNIT_TEST_F(TableModeWithWriteTimePredicate, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutableFeatureFlags()->SetEnableTopicsPredicatePushdown(true);
        config.MutablePQConfig()->SetRequireCredentialsInNewProtocol(true);

        const auto runTest = [&](bool local) {
            const std::string suffix = local ? "_local" : "_nonlocal";
            const std::string topicName = std::string("tableModeWriteTime") + suffix;
            const std::string sourceName = std::string("tableModeWriteTimeSource") + suffix;

            ui32 partitionCount = 1;
            CreateTopic(topicName, NTopic::TCreateTopicSettings().PartitioningSettings(partitionCount, partitionCount), local);

            std::string topicRef;
            if (local) {
                topicRef = fmt::format("`{}`", topicName);
            } else {
                CreatePqSource(sourceName);
                topicRef = fmt::format("`{}`.`{}`", sourceName, topicName);
            }

            auto test = [&](const TString& filter, ui64 rowCount, std::function<void(TResultSetParser&)> validator) {
                TString text = fmt::format(R"(
                    SELECT
                        __ydb_partition_id as partition_id,
                        __ydb_write_time as offset,
                        key as data
                    FROM {topic}
                    WITH (FORMAT = "json_each_row", SCHEMA = (key String NOT NULL))
                    WHERE {filter})",
                    "topic"_a = topicRef,
                    "filter"_a = filter
                );
                auto result = ExecQuery(text);
                CheckScriptResult(result[0], 3, rowCount, validator);
            };

            // Empty topic: equality predicate (any value) — must return 0 rows
            test("__ydb_write_time = Timestamp(\"2020-01-01T00:00:00Z\")", 0, [&](TResultSetParser& /*resultSet*/) {});
            // Empty topic: greater-than predicate — must return 0 rows
            test("__ydb_write_time > Timestamp(\"2020-01-01T00:00:00Z\")", 0, [&](TResultSetParser& /*resultSet*/) {});

            WriteTopicMessage(topicName, "data", 0, local);                   // wrong schema
            WriteTopicMessage(topicName, "{\"key\": \"data1\"}", 0, local);
            WriteTopicMessage(topicName, "{\"key\": \"data2\"}", 0, local);
            Sleep(TDuration::Seconds(5));
            WriteTopicMessage(topicName, "data3", 0, local);                   // wrong schema

            auto received = ReadTopicMessages(topicName, {"1", "2", "3", "4"}, TInstant{}, false, local, false);
            UNIT_ASSERT_VALUES_EQUAL(received.size(), 4);

            test("__ydb_write_time = Timestamp(\"2020-01-01T00:00:00Z\")", 0, [&](TResultSetParser& /*resultSet*/) {});
            test("__ydb_write_time < Timestamp(\"2020-01-01T00:00:00Z\")", 0, [&](TResultSetParser& /*resultSet*/) {});
            test("__ydb_write_time = Timestamp(\"2020-01-01T00:00:00Z\") AND __ydb_write_time > Timestamp(\"2021-01-01T00:00:00Z\")", 0, [&](TResultSetParser& /*resultSet*/) {});
            test("__ydb_write_time = Timestamp(\"" + received[1].second.ToString() + "\")", 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data1");
            });
            test("__ydb_write_time >= Timestamp(\"" + received[1].second.ToString() + "\") \
                AND __ydb_write_time <= Timestamp(\"" + received[2].second.ToString() + "\")", 2, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(2).GetString() == "data1" || resultSet.ColumnParser(2).GetString() == "data2");
            });

            auto test_raw = [&](const TString& filter, ui64 rowCount, std::function<void(TResultSetParser&)> validator) {
                TString text = fmt::format(R"(
                    SELECT __ydb_write_time as offset, Data FROM {topic} WHERE {filter})",
                    "topic"_a = topicRef,
                    "filter"_a = filter
                );
                auto result = ExecQuery(text);
                CheckScriptResult(result[0], 2, rowCount, validator);
            };

            test_raw("__ydb_write_time > CurrentUtcTimestamp(1) - Interval('P1D') AND Data LIKE '%data3%'", 1, [&](TResultSetParser& resultSet) {
                UNIT_ASSERT(resultSet.ColumnParser(1).GetString() == "data3");
            });
            WriteTopicMessage(topicName, "{\"key\": \"data3\"}", 0, local);
            Sleep(TDuration::Seconds(2));
            test("__ydb_write_time > CurrentUtcTimestamp(1) - Interval('PT1S')", 0, [&](TResultSetParser& /*resultSet*/) {});
        };

        runTest(/* local */ false);
        runTest(/* local */ true);
    }
}

} // namespace NKikimr::NKqp
