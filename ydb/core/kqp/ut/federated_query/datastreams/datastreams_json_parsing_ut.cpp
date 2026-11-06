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

    NYdb::NQuery::TAsyncExecuteQueryResult StartReading(ui64 rows, bool skipJsonErrors = false) {
        return GetQueryClient()->ExecuteQuery(fmt::format(R"(
            SELECT value FROM jsonSource.`{topic}` WITH (
                STREAMING = "TRUE",
                FORMAT = "json_each_row",
                `skip.json.errors` = "{skip_errors}",
                SCHEMA = (value String NOT NULL)
            ) LIMIT {rows};
        )", "topic"_a = Topic, "skip_errors"_a = skipJsonErrors ? "true" : "false", "rows"_a = rows), NYdb::NQuery::TTxControl::NoTx(),
            NYdb::NQuery::TExecuteQuerySettings()
                .ClientTimeout(TDuration::Seconds(30))
                .RetrySettings(NYdb::NRetry::TRetryOperationSettings().MaxRetries(0)));
    }

    void CheckMessages(const std::vector<std::string>& messages, const std::vector<std::string>& expected, bool skipJsonErrors = false) {
        Init();
        auto asyncResult = StartReading(expected.size(), skipJsonErrors);
        Sleep(TDuration::Seconds(1));
        WriteTopicMessages(Topic, messages, 0, /* local */ true);
        const auto result = asyncResult.ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSets().size(), 1);
        std::vector<std::string> actual;
        CheckScriptResult(result.GetResultSet(0), 1, expected.size(), [&](NYdb::TResultSetParser& row) {
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
                          "{\"value\":\"first\"}\n{\"value\":\"second\"}", "{\"value\":\"third\"}\n{\"value\":\"fourth\"}",
                      },
            { "first", "second", "third", "fourth" });
    }

    Y_UNIT_TEST_F(ArrayOfObjectsInEachMessage, TJsonParsingFixture) {
        CheckMessages({
                          R"([{"value":"first"},{"value":"second"}])", R"([{"value":"third"},{"value":"fourth"}])",
                      },
            { "first", "second", "third", "fourth" });
    }

    Y_UNIT_TEST_F(JsonCannotCrossTopicMessageBoundary, TJsonParsingFixture) {
        Init();
        auto asyncResult = StartReading(1);
        Sleep(TDuration::Seconds(1));
        // Concatenation is valid JSON, but each topic message is malformed.
        WriteTopicMessages(Topic, { R"({"value":)", R"("split"})" }, 0, /* local */ true);
        const auto result = asyncResult.ExtractValueSync();
        UNIT_ASSERT(!result.IsSuccess());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Failed to parse json message");
    }

    Y_UNIT_TEST_F(SkipMalformedMessageBetweenValidMessages, TJsonParsingFixture) {
        CheckMessages({
                          "{\"value\":\"first\"}\n{\"value\":\"second\"}", R"([{"value":"third"},{"value":"fourth"}])", "{",
                          R"([{"value":"fifth"},{"value":"sixth"}])",
                      },
            { "first", "second", "third", "fourth", "fifth", "sixth" }, /* skipJsonErrors */ true);
    }
}

}   // namespace NKikimr::NKqp
