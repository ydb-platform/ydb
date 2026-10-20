#include "common.h"

#include <fmt/format.h>

namespace NKikimr::NKqp {

using namespace fmt::literals;

namespace {

class TJsonParsingFixture: public TStreamingTestFixture {
public:
    void Init() {
        auto& flags = *SetupAppConfig().MutableFeatureFlags();
        flags.SetEnableSharedReadingInStreamingQueries(true);
        flags.SetEnableSharedReadingStructuredJsonParsing(true);

        CreateTopic(Topic, std::nullopt, /* local */ true);
        ExecQuery("GRANT ALL ON `/Root` TO `" BUILTIN_ACL_ROOT "`");
        ExecQuery(fmt::format(R"(
            CREATE EXTERNAL DATA SOURCE jsonSource WITH (
                SOURCE_TYPE = "Ydb",
                LOCATION = "{endpoint}",
                DATABASE_NAME = "{database}",
                AUTH_METHOD = "NONE",
                SHARED_READING = "true"
            );
        )", "endpoint"_a = GetKikimrRunner()->GetEndpoint(), "database"_a = TEST_DATABASE));
    }

    NYdb::TOperation::TOperationId StartReading(ui64 rows, bool skipJsonErrors = false) {
        return ExecScript(fmt::format(R"(
            SELECT value FROM jsonSource.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                `skip.json.errors` = "{skip_errors}",
                SCHEMA = (value String NOT NULL)
            ) LIMIT {rows};
        )", "topic"_a = Topic, "skip_errors"_a = skipJsonErrors ? "true" : "false", "rows"_a = rows));
    }

    void CheckMessages(const std::vector<std::string>& messages, const std::vector<std::string>& expected, bool skipJsonErrors = false) {
        Init();
        const auto operation = StartReading(expected.size(), skipJsonErrors);
        WriteTopicMessages(Topic, messages, 0, /* local */ true);
        std::vector<std::string> actual;
        CheckScriptResult(operation, 1, expected.size(), [&](NYdb::TResultSetParser& row) {
            actual.emplace_back(row.ColumnParser("value").GetString());
        });
        UNIT_ASSERT_VALUES_EQUAL(actual, expected);
    }

    static constexpr char Topic[] = "jsonParsing";
};

}   // namespace

Y_UNIT_TEST_SUITE(KqpFederatedQueryDatastreamsJsonParsing) {
    Y_UNIT_TEST_F(NewlineSeparatedObjectsInEachMessage, TJsonParsingFixture) {
        CheckMessages({
            "{\"value\":\"first\"}\n{\"value\":\"second\"}",
            "{\"value\":\"third\"}\n{\"value\":\"fourth\"}",
        }, { "first", "second", "third", "fourth" });
    }

    Y_UNIT_TEST_F(ArrayOfObjectsInEachMessage, TJsonParsingFixture) {
        CheckMessages({
            R"([{"value":"first"},{"value":"second"}])",
            R"([{"value":"third"},{"value":"fourth"}])",
        }, { "first", "second", "third", "fourth" });
    }

    Y_UNIT_TEST_F(JsonCannotCrossTopicMessageBoundary, TJsonParsingFixture) {
        Init();
        const auto operation = StartReading(1);
        // Concatenation is valid JSON, but each topic message is malformed.
        WriteTopicMessages(Topic, { R"({"value":)", R"("split"})" }, 0, /* local */ true);
        const auto result = WaitScriptExecution(operation, NYdb::NQuery::EExecStatus::Failed);
        UNIT_ASSERT(!result.Status().IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.Status().GetIssues().ToString(), "Failed to parse json message");
    }

    Y_UNIT_TEST_F(SkipMalformedMessageBetweenValidMessages, TJsonParsingFixture) {
        CheckMessages({
            "{\"value\":\"first\"}\n{\"value\":\"second\"}",
            R"([{"value":"third"},{"value":"fourth"}])",
            "{",
            R"([{"value":"fifth"},{"value":"sixth"}])",
        }, { "first", "second", "third", "fourth", "fifth", "sixth" }, /* skipJsonErrors */ true);
    }
}

}   // namespace NKikimr::NKqp
