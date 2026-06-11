#include "kqp_indexes_json_ut_common.h"

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

const std::string trueSuffix = std::string("\0\1", 2);
const std::string falseSuffix = std::string("\0\0", 2);
const std::string nullSuffix = std::string("\0\2", 2);

const std::string kFirstLongSqlInValue = "abcdefghijklmnopq";
const std::string kSecondLongSqlInValue = "abcdefghijklmnopqrstuvwxyz";

TKikimrRunner Kikimr(bool enableJsonIndex, bool enableJsonIndexAutoSelect) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(enableJsonIndex);
    featureFlags.SetEnableJsonIndexAutoSelect(enableJsonIndexAutoSelect);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    return TKikimrRunner(settings);
}

void CreateTestTable(TQueryClient& db, const std::string& type, bool withIndex) {
    const auto query = std::format(R"(
        CREATE TABLE TestTable (
            Key Uint64,
            Text {0},
            Data Utf8,
            PRIMARY KEY (Key)
            {1}
        );
    )", type, withIndex ? ", INDEX `json_idx` GLOBAL USING json ON (Text)" : "");
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

TResultSet ReadIndex(TQueryClient& db, const char* table) {
    const auto query = std::format(R"(
        SELECT * FROM `TestTable/json_idx/{}`;
    )", table);
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetResultSet(0);
}

void TestAddJsonIndex(const std::string& type, bool nullable) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

    auto columnType = type + (nullable ? "" : " not null");
    CreateTestTable(db, columnType);

    {
        std::string castStart, castEnd;
        if (type == "JsonDocument") {
            castStart = !nullable ? "unwrap(cast(" : "cast(";
            castEnd = !nullable ? " as JsonDocument))" : " as JsonDocument)";
        }

        std::string query = Sprintf(R"(
            UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                (10, %1$s"\"literal string\""%2$s, "d1"),
                (11, %1$s"0.123"%2$s, "data 2"),
                (12, %1$s"true"%2$s, "very long unit test data 3"),
                (13, %1$s"false"%2$s, "data 4"),
                (14, %1$s"null"%2$s, "data 5"),
                (15, %1$s"[false,\"item 1\",45]"%2$s, "array data 6"),
                (16, %1$s"{\"id\":42042,\"brand\":\"bricks\",\"part_count\":1401,\"price\":null,\"parts\":
                    [{\"id\":32526,\"count\":7,\"name\":\"3x5\"},{\"id\":32523,\"count\":17,\"name\":\"1x3\"}]}"%2$s, "object data 7")
        )", castStart.c_str(), castEnd.c_str());
        if (nullable) {
            query += ", (17, NULL, \"null data 8\")";
        }

        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    {
        std::string query = R"(
            ALTER TABLE `/Root/TestTable` ADD INDEX json_idx
                GLOBAL USING json ON (Text)
        )";

        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    CompareYson(R"([
        [[10u];""];
        [[11u];""];
        [[12u];""];
        [[13u];""];
        [[14u];""];
        [[15u];""];
        [[16u];""];
        [[13u];"\0\0"];
        [[15u];"\0\0"];
        [[12u];"\0\1"];
        [[14u];"\0\2"];
        [[15u];"\0\3item 1"];
        [[10u];"\0\3literal string"];
        [[15u];"\0\4\0\0\0\0\0\200F@"];
        [[11u];"\0\4\xB0rh\x91\xED|\xBF?"];
        [[16u];"\3id"];
        [[16u];"\3id\0\4\0\0\0\0@\x87\xE4@"];
        [[16u];"\6brand"];
        [[16u];"\6brand\0\3bricks"];
        [[16u];"\6parts"];
        [[16u];"\6parts\3id"];
        [[16u];"\6parts\3id\0\4\0\0\0\0\x80\xC3\xDF@"];
        [[16u];"\6parts\3id\0\4\0\0\0\0\xC0\xC2\xDF@"];
        [[16u];"\6parts\5name"];
        [[16u];"\6parts\5name\0\0031x3"];
        [[16u];"\6parts\5name\0\0033x5"];
        [[16u];"\6parts\6count"];
        [[16u];"\6parts\6count\0\4\0\0\0\0\0\0\x1C@"];
        [[16u];"\6parts\6count\0\4\0\0\0\0\0\0001@"];
        [[16u];"\6price"];
        [[16u];"\6price\0\2"];
        [[16u];"\x0bpart_count"];
        [[16u];"\x0bpart_count\0\4\0\0\0\0\0\xE4\x95@"]
    ])", FormatResultSetYson(ReadIndex(db)));
}

void FillTestTable(TQueryClient& db, const std::string& tableName, const std::string& jsonType) {
    const std::vector<std::string> values = {
        R"(('null'))",
        R"(('1'))",
        R"(('true'))",
        R"(('false'))",
        R"(('"1"'))",
        R"(('[]'))",
        R"(('{}'))",
        R"(('{"k1": null}'))",
        R"(('{"k1": 1}'))",
        R"(('{"k1": true}'))",
        R"(('{"k1": false}'))",
        R"(('{"k1": "1"}'))",
        R"(('{"k1": []}'))",
        R"(('{"k1": {}}'))",
        R"(('{"k1": [1, 2, 3]}'))",
        R"(('{"k1": "1", "k2": "22"}'))",
        R"(('[{"k1": "1", "k2": "22"}, {"k1": "1", "k2": "22"}]'))",
        R"(('{"k1": {"k2": {"k3": {"k4": "1"}}}}'))",
        R"(('{"k1": 0, "k2": -1.5, "k3": "text", "k4": true, "k5": null, "k6": [1, "1", false], "k7": {"k1": "v"}}'))",
        R"(('{"k1": [{"k1": 10}, {"k1": 20}], "k2": {"k1": 2, "k2": true}}'))",
        R"(('{"": null}'))",
        R"(('{"": 1}'))",
        R"(('{"": true}'))",
        R"(('{"": false}'))",
        R"(('{"": "1"}'))",
        R"(('{"": []}'))",
        R"(('{"": {}}'))",
        R"(('{"": [1, 2, 3]}'))",
        R"(('{"": {"": {"": {"": ["", "1", null, 1, {"": ""}]}}}}'))",
        R"(('[{"": ""}, {"": ""}, {"": ""}, {"": ""}]'))",
        R"(('{"k1": [[{"k2": 0}, {"k2": 1}], []]}'))",
        R"(('[1, [2, [3, [4, []]]]]'))",
        R"(('["1", {"k1": 1}, [2, 3], 4, null, false]'))",
        R"(('[[{"k1": 1}], [{"k2": [{"k3": 2}]}]]'))",
        R"(('{"k1": {"k2": {"k3": [{"k1": "a"}, {"k2": "b"}], "k4": [0, 1.5, -2, null]}}}'))",
        "NULL",
        "NULL",
        "NULL"
    };

    std::string query = std::format(R"(
        UPSERT INTO {} (Key, Text) VALUES
    )", tableName);

    for (size_t i = 0; i < values.size(); ++i) {
        query += std::format("({}, {}),", i + 1, (values[i] == "NULL" ? "" : jsonType) + values[i]);
    }

    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void ValidatePredicate(TQueryClient& db, const std::string& predicate, TParams params, const std::string& suffix) {
    static constexpr const char* table = "TestTable";
    static constexpr const char* indexTable = "json_idx";

    auto query = [&](const std::string& indexPart, const std::string& pred) {
        return std::format(R"(
            SELECT Key, Text FROM {} VIEW {} WHERE {} {};
        )", table, indexPart, pred, suffix);
    };

    auto mainResult = db.ExecuteQuery(query("PRIMARY KEY", predicate), TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_C(mainResult.IsSuccess(), mainResult.GetIssues().ToString());

    auto indexResult = db.ExecuteQuery(query(indexTable, predicate), TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_C(indexResult.IsSuccess(), indexResult.GetIssues().ToString());

    // Cerr << "MAIN: " << Endl << FormatResultSetYson(mainResult.GetResultSet(0)) << Endl;
    // Cerr << "INDEX: " << Endl << FormatResultSetYson(indexResult.GetResultSet(0)) << Endl;

    Cerr << predicate << ", main size: " << mainResult.GetResultSet(0).RowsCount() << ", index size: " << indexResult.GetResultSet(0).RowsCount() << Endl;
    CompareYson(FormatResultSetYson(mainResult.GetResultSet(0)), FormatResultSetYson(indexResult.GetResultSet(0)));
}

void ValidateError(TQueryClient& db, const std::string& predicate, const std::string& errorMessage) {
    static constexpr const char* table = "TestTable";
    static constexpr const char* indexTable = "json_idx";

    auto query = [&](const std::string& indexPart, const std::string& pred) {
        return std::format(R"(
            SELECT * FROM {} VIEW {} WHERE {} ORDER BY Key;
        )", table, indexPart, pred);
    };

    auto result = db.ExecuteQuery(query(indexTable, predicate), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), "Predicate: " + predicate + ", issues: " + result.GetIssues().ToString());
    UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), errorMessage, "for predicate = " << predicate);
}

void ValidateError(TQueryClient& db, const std::string& predicate, TParams params, const std::string& errorMessage) {
    static constexpr const char* table = "TestTable";
    static constexpr const char* indexTable = "json_idx";

    auto query = [&](const std::string& indexPart, const std::string& pred) {
        return std::format(R"(
            SELECT * FROM {} VIEW {} WHERE {} ORDER BY Key;
        )", table, indexPart, pred);
    };

    auto result = db.ExecuteQuery(query(indexTable, predicate), TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), "Predicate: " + predicate + ", issues: " + result.GetIssues().ToString());
    UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), errorMessage, "for predicate = " << predicate);
}

void TestSelectJsonWithIndex(const std::string& jsonType, const std::optional<bool>& jsonExistsStrict,
    const std::function<void(TQueryClient&, const std::function<std::string(const std::string&)>&)>& body,
    bool enableJsonIndexAutoSelect)
{
    auto kikimr = Kikimr(/* enableJsonIndex */ true, enableJsonIndexAutoSelect);
    auto db = kikimr.GetQueryClient();

    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

    std::function<std::string(const std::string&)> jsonExists;
    if (jsonExistsStrict.has_value()) {
        const bool isStrict = jsonExistsStrict.value();
        jsonExists = [isStrict](const std::string& predicate) {
            return std::format("JSON_EXISTS(Text, '{}')", (isStrict ? "strict " : "lax ") + predicate);
        };
    } else {
        jsonExists = [](const std::string&) { return std::string{}; };
    }

    CreateTestTable(db, jsonType, /* withIndex */ false);
    FillTestTable(db, "TestTable", jsonType);

    {
        auto query = R"(
            ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text)
        )";
        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    body(db, jsonExists);
}

void FillDataColumn(TQueryClient& db) {
    auto result = db.ExecuteQuery(
        R"(UPDATE TestTable SET Data = "row_"u || CAST(Key AS Utf8) WHERE Key > 0;)",
        TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void ValidateTokens(TQueryClient& db, const std::string& predicate,
    std::vector<NJsonIndex::TToken> expected, TParams params,
    const std::string& defaultOperator)
{
    auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    auto query = std::format(R"(
        SELECT * FROM TestTable VIEW json_idx WHERE {};
    )", predicate);

    auto result = db.ExecuteQuery(query, TTxControl::NoTx(), params, settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Predicate: " + predicate + ", error: " + result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetStats(), "Stats are empty");

    auto plan = result.GetStats()->GetPlan();
    UNIT_ASSERT_C(plan, "Plan is empty");

    NJson::TJsonValue planJson;
    auto success = NJson::ReadJsonTree(*plan, &planJson, true);
    UNIT_ASSERT_C(success, "Failed to read plan as JSON");

    auto op = planJson["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Operators"][0]["DefaultOperator"].GetString();
    UNIT_ASSERT_VALUES_EQUAL_C(op, '"' + defaultOperator + '"', "for predicate = " << predicate);

    const auto& tokensJson = planJson["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Operators"][0]["Tokens"];
    UNIT_ASSERT_C(tokensJson.IsArray(), "Tokens field is not a JSON array, for predicate = " << predicate);
    UNIT_ASSERT_VALUES_EQUAL_C(tokensJson.GetArray().size(), expected.size(), "for predicate = " << predicate);

    std::vector<std::string> actual;
    for (const auto& t : tokensJson.GetArray()) {
        actual.push_back(t.GetString());
    }

    std::vector<std::string> expectedFormatted;
    for (const auto& [path, paramName] : expected) {
        expectedFormatted.push_back(TString(NJsonIndex::FormatJsonIndexToken(TString(path), TString(paramName))));
    }

    std::sort(actual.begin(), actual.end());
    std::sort(expectedFormatted.begin(), expectedFormatted.end());

    UNIT_ASSERT_VALUES_EQUAL_C(actual, expectedFormatted, "for predicate = " << predicate);
}

void ValidateTokens(TQueryClient& db, const std::string& predicate, std::vector<std::string> expected,
    const std::string& defaultOperator)
{
    std::vector<NJsonIndex::TToken> withoutParams;
    withoutParams.reserve(expected.size());
    for (const auto& e : expected) {
        withoutParams.push_back(NJsonIndex::TToken{TString(e), TString("")});
    }
    ValidateTokens(db, predicate, std::move(withoutParams), TParamsBuilder().Build(), defaultOperator);
}

TExecuteQueryResult WriteJsonIndexWithKeys(TQueryClient& db, const std::string& stmt, const std::string& tableName,
    const std::string& jsonType, const std::vector<std::pair<ui64, ui64>>& values, bool withReturning)
{
    TStringBuilder query;
    query << stmt << " INTO " << tableName << " (Key, Text, Data) VALUES\n";

    for (size_t i = 0; i < values.size(); ++i) {
        const auto [key, value] = values[i];
        query << "(" << key << ", " << jsonType << "('{\"k" << value << "\": [\"v" << value << "\", " << value << ", " << (value % 2 == 0 ? "true" : "false") << "]}'), \"data " << value << "\")";
        if (i + 1 < values.size()) {
            query << ", ";
        } else {
            query << "\n";
        }
    }

    if (withReturning) {
        query << "RETURNING *";
    }

    return db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
}

void ValidateAutoSelect(TQueryClient& db, const std::string& predicate, const TString& indexName, const std::string& tableName) {
    const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    const auto query = std::format("SELECT * FROM {} WHERE {};", tableName, predicate);

    const auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Predicate: " + predicate + ", error: " + result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetStats(), "Stats are empty for: " + predicate);

    const auto plan = result.GetStats()->GetPlan();
    UNIT_ASSERT_C(plan, "Plan is empty for: " + predicate);

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*plan, &planJson, true), "Failed to parse plan JSON");
    UNIT_ASSERT_C(CountPlanNodesByKv(planJson, "Index", indexName) == 1, indexName + " was not autoselected for: " + predicate);
}

void ValidateNoAutoSelect(TQueryClient& db, const std::string& predicate, const TString& indexName, const std::string& tableName) {
    const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    const auto query = std::format("SELECT * FROM {} WHERE {};", tableName, predicate);
    const auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Predicate: " + predicate + ", error: " + result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetStats(), "Stats are empty");

    const auto plan = result.GetStats()->GetPlan();
    UNIT_ASSERT_C(plan, "Plan is empty");

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*plan, &planJson, true), "Failed to parse plan JSON");
    UNIT_ASSERT_C(CountPlanNodesByKv(planJson, "Index", indexName) == 0, indexName + " was autoselected for: " + predicate);
}

void ValidateAutoSelectWithDecl(TQueryClient& db, const std::string& declares, const std::string& predicate,
    const TString& indexName, const std::string& tableName)
{
    const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    const auto query = declares + "\nSELECT * FROM " + tableName + " WHERE " + predicate + ";";

    const auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Predicate: " + predicate + ", error: " + result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetStats(), "Stats are empty for: " + predicate);

    const auto plan = result.GetStats()->GetPlan();
    UNIT_ASSERT_C(plan, "Plan is empty for: " + predicate);

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*plan, &planJson, true), "Failed to parse plan JSON");
    UNIT_ASSERT_C(CountPlanNodesByKv(planJson, "Index", indexName) == 1, indexName + " was not autoselected for: " + predicate);
}

void ValidateNoAutoSelectWithDecl(TQueryClient& db, const std::string& declares, const std::string& predicate,
    const TString& indexName, const std::string& tableName)
{
    const auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    const auto query = declares + "\nSELECT * FROM " + tableName + " WHERE " + predicate + ";";

    const auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Predicate: " + predicate + ", error: " + result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetStats(), "Stats are empty for: " + predicate);

    const auto plan = result.GetStats()->GetPlan();
    UNIT_ASSERT_C(plan, "Plan is empty for: " + predicate);

    NJson::TJsonValue planJson;
    UNIT_ASSERT_C(NJson::ReadJsonTree(*plan, &planJson, true), "Failed to parse plan JSON");
    UNIT_ASSERT_C(CountPlanNodesByKv(planJson, "Index", indexName) == 0, indexName + " was autoselected for: " + predicate);
}

void TestJsonIndexAlterTableWithIntegerPk(const std::string& pkType) {
    auto kikimr = Kikimr(/* enableJsonIndex */ true, /* enableJsonIndexAutoSelect */ true);
    auto db = kikimr.GetQueryClient();

    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

    {
        const auto query = std::format(R"(
            CREATE TABLE TestTable (
                Key {},
                Text Json,
                Data Utf8,
                PRIMARY KEY (Key)
            );
        )", pkType);
        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    {
        const auto query = R"(
            UPSERT INTO TestTable (Key, Text, Data) VALUES
                (1, Json('{"k1": 1}'), "d1"),
                (2, Json('{"k1": 2}'), "d2"),
                (3, Json('{"k2": 3}'), "d3"),
                (4, Json('{"k1": 10}'), "d4"),
                (5, Json('{}'), "d5");
        )";

        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    {
        const auto query = R"(
            ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text)
        )";

        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    {
        const auto query = std::format(R"(
            UPSERT INTO TestTable (Key, Text, Data) VALUES
                (6, '{{"k1": 6}}', "d6"),
                (2, '{{"k1": 99}}', "d2_updated");
        )");

        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    ValidatePredicate(db, R"(JSON_EXISTS(Text, '$.k1'))");
    ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10)");
    ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 99)");

    ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10)");
    ValidateAutoSelect(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 99)");
    ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') LIMIT 2)");

    {
        const auto query = std::format(R"(
            SELECT Key, Data FROM TestTable VIEW json_idx
            WHERE JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10
            ORDER BY Key;
        )");

        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
    }

    ValidateNoAutoSelect(db, std::format("Key = {} AND JSON_EXISTS(Text, '$.k1')", 1));
    ValidateNoAutoSelect(db, std::format("JSON_EXISTS(Text, '$.k1') AND Key = {}", 1));
    ValidateNoAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') OR Data = 'd5')");
    ValidateNoAutoSelect(db, "Data = 'd3'");

    {
        const auto query = std::format(R"(
            SELECT Key FROM TestTable VIEW json_idx
            WHERE JSON_EXISTS(Text, '$.k2')
            ORDER BY Key;
        )");

        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
    }
}

void TestJsonCorpus(TTestJsonCorpusOptions tOpts, TPredicateBuilderOptions pOpts) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    const auto jsonType = std::string(tOpts.IsJsonDocument ? "JsonDocument" : "Json");
    CreateTestTable(db, jsonType, /* withIndex */ false);

    TJsonCorpus corpus(TCorpusOptions{.RowCount = tOpts.RowCount, .Seed = tOpts.Seed});

    corpus.UpsertRange(db, "TestTable", jsonType, 0, tOpts.RowCount / 2);
    {
        auto result = db.ExecuteQuery(R"(
                ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text)
            )", TTxControl::NoTx())
                          .ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    corpus.UpsertRange(db, "TestTable", jsonType, tOpts.RowCount / 2, tOpts.RowCount / 2);

    auto execQ = [&](const std::string& sql, const std::optional<TParams>& params) {
        if (params) {
            return db.ExecuteQuery(sql, TTxControl::NoTx(), *params).ExtractValueSync();
        }
        return db.ExecuteQuery(sql, TTxControl::NoTx()).ExtractValueSync();
    };

    size_t okCount = 0;
    size_t errCount = 0;

    auto predicates = TPredicateBuilder().BuildBatch(corpus, tOpts.IsStrict, tOpts.MaxPredicates, tOpts.Seed, pOpts);
    for (const auto& p : predicates) {
        const auto sqlMain = std::format("SELECT Key FROM TestTable VIEW PRIMARY KEY WHERE {} ORDER BY Key", p.Sql);
        const auto sqlIndex = std::format("SELECT Key FROM TestTable VIEW json_idx WHERE {} ORDER BY Key", p.Sql);

        auto idxResult = execQ(sqlIndex, p.Params);
        auto mainResult = execQ(sqlMain, p.Params);

        if (p.ExpectExtractError) {
            UNIT_ASSERT_C(!idxResult.IsSuccess(), "Expected extract error for predicate: " << p.Sql);
            UNIT_ASSERT_STRING_CONTAINS_C(idxResult.GetIssues().ToString(), p.ExpectedErrorSubstr, "for predicate: " << p.Sql);
            UNIT_ASSERT_C(mainResult.IsSuccess(), "Main query failed for predicate: " << p.Sql << " err: " << mainResult.GetIssues().ToString());
            ++errCount;

            Cerr << p.Sql << ", err" << Endl;
        } else {
            UNIT_ASSERT_C(idxResult.IsSuccess(), "INDEX query failed for predicate: " << p.Sql << " err: " << idxResult.GetIssues().ToString());
            UNIT_ASSERT_C(mainResult.IsSuccess(), "MAIN query failed for predicate: " << p.Sql << " err: " << mainResult.GetIssues().ToString());
            CompareYson(FormatResultSetYson(mainResult.GetResultSet(0)), FormatResultSetYson(idxResult.GetResultSet(0)));
            ++okCount;

            Cerr << p.Sql << ", size: " << idxResult.GetResultSet(0).RowsCount() << Endl;
        }
    }

    Cerr << "JsonIndexCorpus: ok=" << okCount << " err=" << errCount << " total=" << predicates.size() << Endl;
}


}  // namespace NKikimr::NKqp
