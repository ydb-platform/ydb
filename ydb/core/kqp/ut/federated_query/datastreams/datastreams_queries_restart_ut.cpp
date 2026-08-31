#include "common.h"

#include <ydb/core/kqp/ut/federated_query/common/common.h>
#include <ydb/library/testlib/s3_recipe_helper/s3_recipe_helper.h>
#include <ydb/library/testlib/solomon_helpers/solomon_emulator_helpers.h>
#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace fmt::literals;
using namespace NTestUtils;
using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NFederatedQueryTest;

Y_UNIT_TEST_SUITE(KqpFederatedQueryDatastreamsQueriesRestart) {

    Y_UNIT_TEST_F(RestartQueryAfterPartitionIncrease, TStreamingTestFixture) {
        InternalInitFederatedQuerySetupFactory = true;
        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsSqlIoOperations(true);
        config.MutableFeatureFlags()->SetEnableUpdatingPartitionsOnStreamingQueryRestart(true);

        const auto runTest = [&](bool local) {
            const std::string suffix = local ? "_local" : "_nonlocal";
            const std::string inputTopicName  = std::string("restartAfterPartIncInputTopic")  + suffix;
            const std::string outputTopicName = std::string("restartAfterPartIncOutputTopic") + suffix;
            const std::string sourceName      = std::string("restartAfterPartIncSource")      + suffix;
            const std::string queryName       = std::string("restartAfterPartIncQuery")       + suffix;

            CreateTopic(inputTopicName, NYdb::NTopic::TCreateTopicSettings()
                .PartitioningSettings(/* minActivePartitions */ 1, /* maxActivePartitions */ 1), local);
            CreateTopic(outputTopicName, std::nullopt, local);

            std::string inputRef, outputRef;
            if (local) {
                inputRef  = fmt::format("`{}`", inputTopicName);
                outputRef = fmt::format("`{}`", outputTopicName);
            } else {
                CreatePqSource(sourceName);
                inputRef  = fmt::format("`{}`.`{}`", sourceName, inputTopicName);
                outputRef = fmt::format("`{}`.`{}`", sourceName, outputTopicName);
            }

            ExecQuery(fmt::format(R"(
                CREATE STREAMING QUERY `{query_name}` AS
                DO BEGIN
                    $in = SELECT value FROM {input_ref} WITH (
                        FORMAT = "json_each_row",
                        SCHEMA = (value String NOT NULL)
                    )
                    WHERE value LIKE "%data%";
                    INSERT INTO {output_ref} SELECT value FROM $in;
                END DO;)",
                "query_name"_a = queryName,
                "input_ref"_a  = inputRef,
                "output_ref"_a = outputRef
            ));

            WriteTopicMessage(inputTopicName, R"({"value": "my_data_0"})", 0, local);
            ReadTopicMessages(outputTopicName, {"my_data_0"},
                TInstant::Now() - TDuration::Seconds(100),
                /* sort */ true, local);

            Sleep(TDuration::Seconds(2));

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
                "query_name"_a = queryName
            ));
            Sleep(TDuration::MilliSeconds(500));

            {
                auto alterSettings = NYdb::NTopic::TAlterTopicSettings();
                alterSettings
                    .BeginAlterPartitioningSettings()
                        .MinActivePartitions(20)
                        .MaxActivePartitions(20)
                    .EndAlterTopicPartitioningSettings();
                const auto result = GetTopicClient(local)->AlterTopic(inputTopicName, alterSettings).ExtractValueSync();
                UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), NYdb::EStatus::SUCCESS, result.GetIssues().ToOneLineString());
            }

            ExecQuery(fmt::format(R"(
                ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);)",
                "query_name"_a = queryName
            ));

            Sleep(TDuration::Seconds(2));

            constexpr ui32 messageCount = 20;
            for (ui32 i = 1; i < messageCount; ++i) {
                WriteTopicMessage(inputTopicName, fmt::format(R"({{"value": "my_data_{}"}})", i), i, local);
            }
            WriteTopicMessage(inputTopicName, R"({"value": "my_data_0"})", 0, local);

            std::vector<std::string> expectedMessages = {"my_data_0"}; // initial message written before restart
            for (ui32 i = 0; i < messageCount; ++i) {
                expectedMessages.push_back(fmt::format("my_data_{}", i));
            }
            ReadTopicMessages(outputTopicName, expectedMessages,
                TInstant::Now() - TDuration::Seconds(100),
                /* sort */ true, local);

            ExecQuery(fmt::format(R"(
                DROP STREAMING QUERY `{query_name}`;)",
                "query_name"_a = queryName
            ));
        };

        runTest(/* local */ false);
        runTest(/* local */ true);
    }

    Y_UNIT_TEST_F(PartitionPredicatePreservedAfterPartitionIncrease, TStreamingTestFixture) {
        constexpr char inputTopicName[]  = "partPredicateAfterIncInputTopic";
        constexpr char outputTopicName[] = "partPredicateAfterIncOutputTopic";
        constexpr char sourceName[]      = "partPredicateAfterIncSource";
        constexpr char queryName[]       = "partPredicateAfterIncQuery";

        auto& config = SetupAppConfig();
        config.MutableFeatureFlags()->SetEnableTopicsPredicatePushdown(true);
        config.MutableFeatureFlags()->SetEnableUpdatingPartitionsOnStreamingQueryRestart(true);

        const ui32 initialPartitionCount = 4;
        CreateTopic(inputTopicName, NYdb::NTopic::TCreateTopicSettings()
            .PartitioningSettings(initialPartitionCount, initialPartitionCount));
        CreateTopic(outputTopicName);
        CreatePqSource(sourceName);

        ExecQuery(fmt::format(R"(
            CREATE STREAMING QUERY `{query_name}` AS
            DO BEGIN
                $in = SELECT value FROM `{source}`.`{input_topic}` WITH (
                    FORMAT = "json_each_row",
                    SCHEMA = (value String NOT NULL)
                )
                WHERE __ydb_partition_id < 2;
                INSERT INTO `{source}`.`{output_topic}` SELECT value FROM $in;
            END DO;)",
            "query_name"_a = queryName,
            "source"_a    = sourceName,
            "input_topic"_a = inputTopicName,
            "output_topic"_a = outputTopicName
        ));

        for (ui32 i = 2; i < initialPartitionCount; ++i) {
            WriteTopicMessage(inputTopicName,
                fmt::format("not_valid_json_p{}", i), i);
        }
        // Write valid JSON to partitions inside the predicate (0, 1).
        for (ui32 i = 0; i < 2; ++i) {
            WriteTopicMessage(inputTopicName,
                fmt::format(R"({{"value": "before_data_p{}"}})", i), i);
        }

        ReadTopicMessages(outputTopicName,
            {"before_data_p0", "before_data_p1"},
            TInstant::Now() - TDuration::Seconds(100),
            /* sort */ true);

        Sleep(TDuration::Seconds(2));

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = FALSE);)",
            "query_name"_a = queryName
        ));
        Sleep(TDuration::MilliSeconds(500));

        {
            NYdb::NTopic::TAlterTopicSettings alterSettings;
            alterSettings
                .BeginAlterPartitioningSettings()
                    .MinActivePartitions(20)
                    .MaxActivePartitions(20)
                .EndAlterTopicPartitioningSettings();
            const auto alterResult = GetTopicClient()
                ->AlterTopic(inputTopicName, alterSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(
                alterResult.GetStatus(), NYdb::EStatus::SUCCESS,
                alterResult.GetIssues().ToOneLineString());
        }

        ExecQuery(fmt::format(R"(
            ALTER STREAMING QUERY `{query_name}` SET (RUN = TRUE);)",
            "query_name"_a = queryName
        ));

        Sleep(TDuration::Seconds(2));

        const TInstant afterRestart = TInstant::Now();

        const ui32 newPartitionCount = 20;
        for (ui32 i = 4; i < newPartitionCount; ++i) {
            WriteTopicMessage(inputTopicName,
                fmt::format("not_valid_json_p{}", i), i);
        }

        WriteTopicMessage(inputTopicName, R"({"value": "after_data_p0"})", 0);
        WriteTopicMessage(inputTopicName, R"({"value": "after_data_p1"})", 1);

        ReadTopicMessages(outputTopicName,
            {"after_data_p0", "after_data_p1"},
            afterRestart,
            /* sort */ true);

        ExecQuery(fmt::format(R"(
            DROP STREAMING QUERY `{query_name}`;)",
            "query_name"_a = queryName
        ));
    }
}

} // namespace NKikimr::NKqp
