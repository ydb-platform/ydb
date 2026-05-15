#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

#include <ydb/core/base/json_index.h>
#include <ydb/core/kqp/ut/indexes/json/kqp_json_index_corpus.h>
#include <ydb/core/kqp/ut/indexes/json/kqp_json_index_predicate.h>

#include <optional>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

namespace {

const auto strSuffix = [](const std::string& s) {
    return std::string("\0\3", 2) + s;
};

const auto numSuffix = [](double v) {
    std::string s;
    s.push_back('\0');
    s.push_back('\4');
    s.append(reinterpret_cast<const char*>(&v), sizeof(double));
    return s;
};

const std::string trueSuffix = std::string("\0\1", 2);
const std::string falseSuffix = std::string("\0\0", 2);
const std::string nullSuffix = std::string("\0\2", 2);

TKikimrRunner Kikimr(bool enableJsonIndex = true) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(enableJsonIndex);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    return TKikimrRunner(settings);
}

void CreateTestTable(TQueryClient& db, const std::string& type = "Json", bool withIndex = false) {
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

TResultSet ReadIndex(TQueryClient& db, const char* table = "indexImplTable") {
    const auto query = std::format(R"(
        SELECT * FROM `TestTable/json_idx/{}`;
    )", table);
    auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return result.GetResultSet(0);
}

void TestAddJsonIndex(const std::string& type, bool nullable, bool covered) {
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
        )" + std::string(covered ? " COVER (Data)" : "");

        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
    auto index = ReadIndex(db);
    if (covered) {
        CompareYson(R"([
            [["d1"];[10u];""];
            [["data 2"];[11u];""];
            [["very long unit test data 3"];[12u];""];
            [["data 4"];[13u];""];
            [["data 5"];[14u];""];
            [["array data 6"];[15u];""];
            [["object data 7"];[16u];""];
            [["data 4"];[13u];"\0\0"];
            [["array data 6"];[15u];"\0\0"];
            [["very long unit test data 3"];[12u];"\0\1"];
            [["data 5"];[14u];"\0\2"];
            [["array data 6"];[15u];"\0\3item 1"];
            [["d1"];[10u];"\0\3literal string"];
            [["array data 6"];[15u];"\0\4\0\0\0\0\0\200F@"];
            [["data 2"];[11u];"\0\4\xB0rh\x91\xED|\xBF?"];
            [["object data 7"];[16u];"\3id"];
            [["object data 7"];[16u];"\3id\0\4\0\0\0\0@\x87\xE4@"];
            [["object data 7"];[16u];"\6brand"];
            [["object data 7"];[16u];"\6brand\0\3bricks"];
            [["object data 7"];[16u];"\6parts"];
            [["object data 7"];[16u];"\6parts\3id"];
            [["object data 7"];[16u];"\6parts\3id\0\4\0\0\0\0\x80\xC3\xDF@"];
            [["object data 7"];[16u];"\6parts\3id\0\4\0\0\0\0\xC0\xC2\xDF@"];
            [["object data 7"];[16u];"\6parts\5name"];
            [["object data 7"];[16u];"\6parts\5name\0\0031x3"];
            [["object data 7"];[16u];"\6parts\5name\0\0033x5"];
            [["object data 7"];[16u];"\6parts\6count"];
            [["object data 7"];[16u];"\6parts\6count\0\4\0\0\0\0\0\0\x1C@"];
            [["object data 7"];[16u];"\6parts\6count\0\4\0\0\0\0\0\0001@"];
            [["object data 7"];[16u];"\6price"];
            [["object data 7"];[16u];"\6price\0\2"];
            [["object data 7"];[16u];"\x0bpart_count"];
            [["object data 7"];[16u];"\x0bpart_count\0\4\0\0\0\0\0\xE4\x95@"]
        ])", FormatResultSetYson(index));
    } else {
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
        ])", FormatResultSetYson(index));
    }
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

void ValidatePredicate(TQueryClient& db, const std::string& predicate, TParams params = TParamsBuilder().Build()) {
    static constexpr const char* table = "TestTable";
    static constexpr const char* indexTable = "json_idx";

    auto query = [&](const std::string& indexPart, const std::string& pred) {
        return std::format(R"(
            SELECT Key, Text FROM {} {} WHERE {} ORDER BY Key;
        )", table, (indexPart.empty() ? "" : "VIEW  " + indexPart), pred);
    };

    auto mainResult = db.ExecuteQuery(query("", predicate), TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_C(mainResult.IsSuccess(), mainResult.GetIssues().ToString());

    auto indexResult = db.ExecuteQuery(query(indexTable, predicate), TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_C(indexResult.IsSuccess(), indexResult.GetIssues().ToString());

    // Cerr << "MAIN: " << Endl << FormatResultSetYson(mainResult.GetResultSet(0)) << Endl;
    // Cerr << "INDEX: " << Endl << FormatResultSetYson(indexResult.GetResultSet(0)) << Endl;

    Cerr << predicate << ", main size: " << mainResult.GetResultSet(0).RowsCount() << ", index size: " << indexResult.GetResultSet(0).RowsCount() << Endl;
    CompareYson(FormatResultSetYson(mainResult.GetResultSet(0)), FormatResultSetYson(indexResult.GetResultSet(0)));
}

void ValidateError(TQueryClient& db, const std::string& predicate, const std::string& errorMessage = "Failed to extract search terms from predicate") {
    static constexpr const char* table = "TestTable";
    static constexpr const char* indexTable = "json_idx";

    auto query = [&](const std::string& indexPart, const std::string& pred) {
        return std::format(R"(
            SELECT * FROM {} {} WHERE {} ORDER BY Key;
        )", table, (indexPart.empty() ? "" : "VIEW  " + indexPart), pred);
    };

    auto result = db.ExecuteQuery(query(indexTable, predicate), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), "Predicate: " + predicate + ", issues: " + result.GetIssues().ToString());
    UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), errorMessage, "for predicate = " << predicate);
}

void ValidateError(TQueryClient& db, const std::string& predicate, TParams params,
    const std::string& errorMessage = "Failed to extract search terms from predicate") {
    static constexpr const char* table = "TestTable";
    static constexpr const char* indexTable = "json_idx";

    auto query = [&](const std::string& indexPart, const std::string& pred) {
        return std::format(R"(
            SELECT * FROM {} {} WHERE {} ORDER BY Key;
        )", table, (indexPart.empty() ? "" : "VIEW  " + indexPart), pred);
    };

    auto result = db.ExecuteQuery(query(indexTable, predicate), TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), "Predicate: " + predicate + ", issues: " + result.GetIssues().ToString());
    UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), errorMessage, "for predicate = " << predicate);
}

void TestSelectJsonWithIndex(const std::string& jsonType, const std::optional<bool>& jsonExistsStrict,
    const std::function<void(TQueryClient&, const std::function<std::string(const std::string&)>&)>& body)
{
    auto kikimr = Kikimr();
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

void ValidateTokens(TQueryClient& db, const std::string& predicate,
    std::vector<NJsonIndex::TToken> expected, TParams params,
    const std::string& defaultOperator = "and")
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
    const std::string& defaultOperator = "and")
{
    std::vector<NJsonIndex::TToken> withoutParams;
    withoutParams.reserve(expected.size());
    for (const auto& e : expected) {
        withoutParams.push_back(NJsonIndex::TToken{TString(e), TString("")});
    }
    ValidateTokens(db, predicate, std::move(withoutParams), TParamsBuilder().Build(), defaultOperator);
}

TExecuteQueryResult WriteJsonIndexWithKeys(TQueryClient& db, const std::string& stmt, const std::string& tableName,
    const std::string& jsonType, const std::vector<std::pair<ui64, ui64>>& values, bool withReturning = false)
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

}  // namespace

Y_UNIT_TEST_SUITE(KqpJsonIndexes) {
    Y_UNIT_TEST(AddJsonIndexJson) {
        TestAddJsonIndex("Json", true, false);
    }

    Y_UNIT_TEST(AddJsonIndexJsonDocument) {
        TestAddJsonIndex("JsonDocument", true, false);
    }

    Y_UNIT_TEST(AddJsonIndexJsonNotNull) {
        TestAddJsonIndex("Json", false, false);
    }

    Y_UNIT_TEST(AddJsonIndexJsonDocumentNotNull) {
        TestAddJsonIndex("JsonDocument", false, false);
    }

    Y_UNIT_TEST(AddJsonIndexCoveringJson) {
        TestAddJsonIndex("Json", true, true);
    }

    Y_UNIT_TEST(AddJsonIndexCoveringJsonDocument) {
        TestAddJsonIndex("JsonDocument", true, true);
    }

    Y_UNIT_TEST(AddJsonIndexCoveringJsonNotNull) {
        TestAddJsonIndex("Json", false, true);
    }

    Y_UNIT_TEST(AddJsonIndexCoveringJsonDocumentNotNull) {
        TestAddJsonIndex("JsonDocument", false, true);
    }

    Y_UNIT_TEST(OnCreate) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        CreateTestTable(db, "Json", true);

        // TODO: Test it with update after implementing update
    }

    Y_UNIT_TEST(UnsupportedType) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        CreateTestTable(db, "Uint64");

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Error: JSON column 'Text' must have type 'Json' or 'JsonDocument' but got Uint64");
        }
    }

    Y_UNIT_TEST(NoMultipleColumns) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Field1 Json,
                    Field2 Json,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Field1, Field2)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "JSON index supports only 1 key column, but 2 are requested");
        }
    }

    Y_UNIT_TEST(NonUint64Pk) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint32,
                    Field1 Json,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Field1)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Error: JSON index requires primary key column 'Key' to be of type 'Uint64' but got Uint32");
        }
    }

    Y_UNIT_TEST(NoCompositePk) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key1 Uint64,
                    Key2 Uint64,
                    Field1 Json,
                    PRIMARY KEY (Key1, Key2)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Field1)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Error: JSON index requires exactly one primary key column of type 'Uint64', but table has 2 primary key columns");
        }
    }

    Y_UNIT_TEST(DisabledFlagRejectAlter) {
        auto kikimr = Kikimr(/* enableJsonIndex */ false);
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, "Json");

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(DisabledFlagRejectCreate) {
        auto kikimr = Kikimr(/* enableJsonIndex */ false);
        auto db = kikimr.GetQueryClient();

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text Json,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX `json_idx` GLOBAL USING json ON (Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST_QUAD(UpsertJsonIndex, IsJsonDocument, WithReturning) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";

        CreateTestTable(db, jsonType);

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "UPSERT", "TestTable", jsonType, {{1, 1}, {2, 2}, {3, 3}, {4, 4}}, WithReturning);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 1"];[1u];["{\"k1\":[\"v1\",1,false]}"]];
                        [["data 2"];[2u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[3u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 4"];[4u];["{\"k4\":[\"v4\",4,true]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 1"];[1u];["{\"k1\": [\"v1\", 1, false]}"]];
                        [["data 2"];[2u];["{\"k2\": [\"v2\", 2, true]}"]];
                        [["data 3"];[3u];["{\"k3\": [\"v3\", 3, false]}"]];
                        [["data 4"];[4u];["{\"k4\": [\"v4\", 4, true]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];""];
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];""];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];""];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }


        {
            auto writeResult = WriteJsonIndexWithKeys(db, "UPSERT", "TestTable", jsonType, {{1, 3}, {3, 2}, {5, 5}}, WithReturning);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 5"];[5u];["{\"k5\":[\"v5\",5,false]}"]];
                        [["data 2"];[3u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[1u];["{\"k3\":[\"v3\",3,false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 5"];[5u];["{\"k5\": [\"v5\", 5, false]}"]];
                        [["data 2"];[3u];["{\"k2\": [\"v2\", 2, true]}"]];
                        [["data 3"];[1u];["{\"k3\": [\"v3\", 3, false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[1u];"\3k3\0\3v3"];
                [[1u];"\3k3\0\0"];
                [[1u];"\3k3"];
                [[1u];""];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[2u];""];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[3u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k2\0\3v2"];
                [[3u];"\3k2\0\1"];
                [[3u];"\3k2"];
                [[3u];""];
                [[4u];""];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"];
                [[5u];""];
                [[5u];"\3k5"];
                [[5u];"\3k5\0\0"];
                [[5u];"\3k5\0\3v5"];
                [[5u];"\3k5\0\4\0\0\0\0\0\0\x14@"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_QUAD(ReplaceJsonIndex, IsJsonDocument, WithReturning) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";

        CreateTestTable(db, jsonType);

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "REPLACE", "TestTable", jsonType, {{1, 1}, {2, 2}, {3, 3}, {4, 4}}, WithReturning);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 1"];[1u];["{\"k1\":[\"v1\",1,false]}"]];
                        [["data 2"];[2u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[3u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 4"];[4u];["{\"k4\":[\"v4\",4,true]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 1"];[1u];["{\"k1\": [\"v1\", 1, false]}"]];
                        [["data 2"];[2u];["{\"k2\": [\"v2\", 2, true]}"]];
                        [["data 3"];[3u];["{\"k3\": [\"v3\", 3, false]}"]];
                        [["data 4"];[4u];["{\"k4\": [\"v4\", 4, true]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];""];
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];""];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];""];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }


        {
            auto writeResult = WriteJsonIndexWithKeys(db, "REPLACE", "TestTable", jsonType, {{1, 3}, {3, 2}, {5, 5}}, WithReturning);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 5"];[5u];["{\"k5\":[\"v5\",5,false]}"]];
                        [["data 2"];[3u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[1u];["{\"k3\":[\"v3\",3,false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 5"];[5u];["{\"k5\": [\"v5\", 5, false]}"]];
                        [["data 2"];[3u];["{\"k2\": [\"v2\", 2, true]}"]];
                        [["data 3"];[1u];["{\"k3\": [\"v3\", 3, false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[1u];"\3k3\0\3v3"];
                [[1u];"\3k3\0\0"];
                [[1u];"\3k3"];
                [[1u];""];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[2u];""];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[3u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k2\0\3v2"];
                [[3u];"\3k2\0\1"];
                [[3u];"\3k2"];
                [[3u];""];
                [[4u];""];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"];
                [[5u];""];
                [[5u];"\3k5"];
                [[5u];"\3k5\0\0"];
                [[5u];"\3k5\0\3v5"];
                [[5u];"\3k5\0\4\0\0\0\0\0\0\x14@"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_QUAD(InsertJsonIndex, IsJsonDocument, WithReturning) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";

        CreateTestTable(db, jsonType);

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "INSERT", "TestTable", jsonType, {{1, 1}, {2, 2}, {3, 3}, {4, 4}}, WithReturning);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 1"];[1u];["{\"k1\":[\"v1\",1,false]}"]];
                        [["data 2"];[2u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[3u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 4"];[4u];["{\"k4\":[\"v4\",4,true]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 1"];[1u];["{\"k1\": [\"v1\", 1, false]}"]];
                        [["data 2"];[2u];["{\"k2\": [\"v2\", 2, true]}"]];
                        [["data 3"];[3u];["{\"k3\": [\"v3\", 3, false]}"]];
                        [["data 4"];[4u];["{\"k4\": [\"v4\", 4, true]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];""];
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];""];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];""];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "INSERT", "TestTable", jsonType, {{5, 3}, {6, 2}}, WithReturning);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 3"];[5u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 2"];[6u];["{\"k2\":[\"v2\",2,true]}"]];
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 3"];[5u];["{\"k3\": [\"v3\", 3, false]}"]];
                        [["data 2"];[6u];["{\"k2\": [\"v2\", 2, true]}"]];
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];""];
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[2u];"\3k2\0\3v2"];
                [[3u];""];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"];
                [[4u];""];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[5u];"\3k3"];
                [[5u];"\3k3\0\0"];
                [[5u];""];
                [[5u];"\3k3\0\3v3"];
                [[5u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[6u];"\3k2\0\1"];
                [[6u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[6u];"\3k2"];
                [[6u];""];
                [[6u];"\3k2\0\3v2"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "INSERT", "TestTable", jsonType, {{1, 1}, {7, 7}}, WithReturning);
            UNIT_ASSERT_C(!writeResult.IsSuccess(), writeResult.GetIssues().ToString());
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];""];
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[2u];"\3k2\0\3v2"];
                [[3u];""];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"];
                [[4u];""];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[5u];"\3k3"];
                [[5u];"\3k3\0\0"];
                [[5u];""];
                [[5u];"\3k3\0\3v3"];
                [[5u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[6u];"\3k2\0\1"];
                [[6u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[6u];"\3k2"];
                [[6u];""];
                [[6u];"\3k2\0\3v2"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_QUAD(UpdateJsonIndex, IsJsonDocument, WithReturning) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";

        CreateTestTable(db, jsonType);

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "INSERT", "TestTable", jsonType, {{1, 1}, {2, 2}, {3, 3}, {4, 4}}, /* withReturning */ false);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];""];
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];""];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];""];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            TStringBuilder query;
            query << "UPDATE `/Root/TestTable` "
                  << "SET Text = " << jsonType << "('{\"k10\": [\"v10\", 10, true]}'), "
                  << "Data = \"data 10\" "
                  << "WHERE Key IN (2, 3)";
            if (WithReturning) {
                query << " RETURNING *";
            }

            auto updateResult = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(updateResult.IsSuccess(), updateResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 10"];[3u];["{\"k10\":[\"v10\",10,true]}"]];
                        [["data 10"];[2u];["{\"k10\":[\"v10\",10,true]}"]]
                    ])", FormatResultSetYson(updateResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 10"];[3u];["{\"k10\": [\"v10\", 10, true]}"]];
                        [["data 10"];[2u];["{\"k10\": [\"v10\", 10, true]}"]]
                    ])", FormatResultSetYson(updateResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];""];
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\4k10"];
                [[2u];"\4k10\0\1"];
                [[2u];"\4k10\0\3v10"];
                [[2u];"\4k10\0\4\0\0\0\0\0\0$@"];
                [[3u];""];
                [[3u];"\4k10"];
                [[3u];"\4k10\0\1"];
                [[3u];"\4k10\0\3v10"];
                [[3u];"\4k10\0\4\0\0\0\0\0\0$@"];
                [[4u];""];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            TStringBuilder query;
            query << "UPDATE `/Root/TestTable` "
                  << "SET Text = " << jsonType << "('{\"k100\": [\"v100\", 100, false]}'), "
                  << "Data = \"data 100\"";
            if (WithReturning) {
                query << " RETURNING *";
            }

            auto updateResult = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(updateResult.IsSuccess(), updateResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 100"];[4u];["{\"k100\":[\"v100\",100,false]}"]];
                        [["data 100"];[3u];["{\"k100\":[\"v100\",100,false]}"]];
                        [["data 100"];[2u];["{\"k100\":[\"v100\",100,false]}"]];
                        [["data 100"];[1u];["{\"k100\":[\"v100\",100,false]}"]]
                    ])", FormatResultSetYson(updateResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 100"];[4u];["{\"k100\": [\"v100\", 100, false]}"]];
                        [["data 100"];[3u];["{\"k100\": [\"v100\", 100, false]}"]];
                        [["data 100"];[2u];["{\"k100\": [\"v100\", 100, false]}"]];
                        [["data 100"];[1u];["{\"k100\": [\"v100\", 100, false]}"]]
                    ])", FormatResultSetYson(updateResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];""];
                [[1u];"\5k100"];
                [[1u];"\5k100\0\0"];
                [[1u];"\5k100\0\3v100"];
                [[1u];"\5k100\0\4\0\0\0\0\0\0Y@"];
                [[2u];""];
                [[2u];"\5k100"];
                [[2u];"\5k100\0\0"];
                [[2u];"\5k100\0\3v100"];
                [[2u];"\5k100\0\4\0\0\0\0\0\0Y@"];
                [[3u];""];
                [[3u];"\5k100"];
                [[3u];"\5k100\0\0"];
                [[3u];"\5k100\0\3v100"];
                [[3u];"\5k100\0\4\0\0\0\0\0\0Y@"];
                [[4u];""];
                [[4u];"\5k100"];
                [[4u];"\5k100\0\0"];
                [[4u];"\5k100\0\3v100"];
                [[4u];"\5k100\0\4\0\0\0\0\0\0Y@"]
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_QUAD(DeleteJsonIndex, IsJsonDocument, WithReturning) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";

        CreateTestTable(db, jsonType);

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "INSERT", "TestTable", jsonType, {{1, 1}, {2, 2}, {3, 3}, {4, 4}}, /* withReturning */ false);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            TStringBuilder query;
            query << "DELETE FROM `/Root/TestTable` WHERE Key IN (2, 4)";
            if (WithReturning) {
                query << " RETURNING *";
            }

            auto deleteResult = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(deleteResult.IsSuccess(), deleteResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 4"];[4u];["{\"k4\":[\"v4\",4,true]}"]];
                        [["data 2"];[2u];["{\"k2\":[\"v2\",2,true]}"]]
                    ])", FormatResultSetYson(deleteResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 4"];[4u];["{\"k4\": [\"v4\", 4, true]}"]];
                        [["data 2"];[2u];["{\"k2\": [\"v2\", 2, true]}"]]
                    ])", FormatResultSetYson(deleteResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson(R"([
                [[1u];""];
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[3u];""];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
            ])", FormatResultSetYson(result.GetResultSet(0)));
        }

        {
            TStringBuilder query;
            query << "DELETE FROM `/Root/TestTable`";
            if (WithReturning) {
                query << " RETURNING *";
            }

            auto deleteResult = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(deleteResult.IsSuccess(), deleteResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYson(R"([
                        [["data 3"];[3u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 1"];[1u];["{\"k1\":[\"v1\",1,false]}"]]
                    ])", FormatResultSetYson(deleteResult.GetResultSet(0)));
                } else {
                    CompareYson(R"([
                        [["data 3"];[3u];["{\"k3\": [\"v3\", 3, false]}"]];
                        [["data 1"];[1u];["{\"k1\": [\"v1\", 1, false]}"]]
                    ])", FormatResultSetYson(deleteResult.GetResultSet(0)));
                }
            }
        }

        {
            std::string query = R"(
                SELECT * FROM `/Root/TestTable/json_idx/indexImplTable` ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            CompareYson("[]", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST_QUAD(SelectJsonExists_ContextObject, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$"));
        });
    }

    Y_UNIT_TEST_QUAD(SelectJsonExists_MemberAccess, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$.k1"));
            ValidatePredicate(db, jsonExists("$.k2"));
            ValidatePredicate(db, jsonExists("$.k3"));
            ValidatePredicate(db, jsonExists("$.k4"));
            ValidatePredicate(db, jsonExists("$.k5"));
            ValidatePredicate(db, jsonExists("$.k6"));
            ValidatePredicate(db, jsonExists("$.k7"));
            ValidatePredicate(db, jsonExists("$.k8"));

            ValidatePredicate(db, jsonExists("$.k1.k1"));
            ValidatePredicate(db, jsonExists("$.k1.k2"));
            ValidatePredicate(db, jsonExists("$.k1.k3"));
            ValidatePredicate(db, jsonExists("$.k1.k4"));
            ValidatePredicate(db, jsonExists("$.k1.k5"));
            ValidatePredicate(db, jsonExists("$.k2.k1"));
            ValidatePredicate(db, jsonExists("$.k2.k2"));
            ValidatePredicate(db, jsonExists("$.k2.k3"));
            ValidatePredicate(db, jsonExists("$.k2.k4"));
            ValidatePredicate(db, jsonExists("$.k2.k5"));
            ValidatePredicate(db, jsonExists("$.k3.k1"));
            ValidatePredicate(db, jsonExists("$.k4.k1"));

            ValidatePredicate(db, jsonExists("$.\"\""));
            ValidatePredicate(db, jsonExists("$.\"\".\"\""));
            ValidatePredicate(db, jsonExists("$.\"\".\"\".\"\""));
            ValidatePredicate(db, jsonExists("$.\"\".\"\".\"\".\"\""));
            ValidatePredicate(db, jsonExists("$.\"\".\"\".\"\".\"\".\"\""));

            ValidatePredicate(db, jsonExists("$.*"));
            ValidatePredicate(db, jsonExists("$.k1.*"));
            ValidatePredicate(db, jsonExists("$.k2.*"));
            ValidatePredicate(db, jsonExists("$.k1.k1.*"));
            ValidatePredicate(db, jsonExists("$.k1.*.k1"));
            ValidatePredicate(db, jsonExists("$.k1.*.*"));
        });
    }

    Y_UNIT_TEST_QUAD(SelectJsonExists_ArrayAccess, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$[0]"));
            ValidatePredicate(db, jsonExists("$[0, 3]"));
            ValidatePredicate(db, jsonExists("$[1 to 3]"));
            ValidatePredicate(db, jsonExists("$[last]"));
            ValidatePredicate(db, jsonExists("$[*]"));
            ValidatePredicate(db, jsonExists("$[0][0][0]"));
            ValidatePredicate(db, jsonExists("$[0].k1"));
            ValidatePredicate(db, jsonExists("$[0, 3].k1"));
            ValidatePredicate(db, jsonExists("$[1 to 3].k1"));
            ValidatePredicate(db, jsonExists("$[last].k1"));
            ValidatePredicate(db, jsonExists("$[*].k1"));
            ValidatePredicate(db, jsonExists("$[0].*"));
            ValidatePredicate(db, jsonExists("$[*].*"));
            ValidatePredicate(db, jsonExists("$.k1[0]"));
            ValidatePredicate(db, jsonExists("$.k1[0, 3]"));
            ValidatePredicate(db, jsonExists("$.k1[1 to 3]"));
            ValidatePredicate(db, jsonExists("$.k1[last]"));
            ValidatePredicate(db, jsonExists("$.k1[0 to last]"));
            ValidatePredicate(db, jsonExists("$.k1[*]"));
            ValidatePredicate(db, jsonExists("$.*[0]"));
            ValidatePredicate(db, jsonExists("$.*[*]"));
        });
    }

    Y_UNIT_TEST_QUAD(SelectJsonExists_Methods, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            auto validateMethod = [&](const std::string& method) {
                ValidatePredicate(db, jsonExists(std::format("$.{}", method)));
                ValidatePredicate(db, jsonExists(std::format("$.k1.{}", method)));
                ValidatePredicate(db, jsonExists(std::format("$.*.{}", method)));
                ValidatePredicate(db, jsonExists(std::format("$[0].{}", method)));
                ValidatePredicate(db, jsonExists(std::format("$[*].{}", method)));
            };

            validateMethod("type()");
            validateMethod("size()");
            validateMethod("double()");
            validateMethod("ceiling()");
            validateMethod("floor()");
            validateMethod("abs()");
            validateMethod("keyvalue()");

            validateMethod("keyvalue().size()");
            validateMethod("keyvalue().name");
            validateMethod("keyvalue().value");
            validateMethod("keyvalue().value.size()");

            validateMethod("size().double()");
            validateMethod("abs().ceiling()");
            validateMethod("abs().floor().type()");
        });
    }

    // All 6 literal types with == inside a filter, plus @ itself (not a sub-member)
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterEqual, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            // @.field == literal, all literal types
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k2 == -1.5)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k2 == false)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k5 == null)"));
            // Both sides are paths (index terms merged with AND)
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == @.k1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k3 == @.k4)"));
            // @ itself as the filter path (not a sub-member), all literal types
            ValidatePredicate(db, jsonExists("$.k1 ? (@ == 1)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ == -1.5)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ == \"1\")"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ == true)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ == false)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ == null)"));
        });
    }

    // All comparison operators in a filter
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterComparisonOps, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? (@.k1 < 10)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 <= -1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 > 0)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 >= -2)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 != 0)"));

            ValidatePredicate(db, jsonExists("$ ? (+1 == @.k1)"));
            ValidatePredicate(db, jsonExists("$ ? (-(+(-10)) > @.k1)"));
            ValidatePredicate(db, jsonExists("$ ? (\"text\" == @.k3)"));
            ValidatePredicate(db, jsonExists("$ ? (null == @.k5)"));
        });
    }

    // AND and OR boolean operators inside filter predicates
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterLogicalOps, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 && @.k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k4 == true && @.k5 == null)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 > 0 && @.k1 < 100)"));

            ValidatePredicate(db, jsonExists("$ ? ((@.k1 == 1) || (@.k1 == 0))"));
            ValidatePredicate(db, jsonExists("$ ? ((@.k4 == true) || (@.k2 == false))"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@.k1 == 10) || (@.k1 == 20))"));
        });
    }

    // Corner cases for the filter context path: deep nesting, array subscript, empty key
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterPaths, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? (@.k1.k2.k3.k4 == \"1\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1[0] == 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k6[2] == false)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1 == 10)"));
            ValidatePredicate(db, jsonExists("$ ? (@.\"\" == null)"));
            ValidatePredicate(db, jsonExists("$ ? (@.\"\" == 1)"));
        });
    }

    // Predicates and boolean operators inside filter
    Y_UNIT_TEST_QUAD(SelectJsonExists_Predicates, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            // Predicates are not allowed in JsonExists without a filter
            ValidateError(db, jsonExists("exists($.k1)"));
            ValidateError(db, jsonExists("$.k1 starts with \"abc\""));
            ValidateError(db, jsonExists("$.k1 like_regex \"abc\""));
            ValidateError(db, jsonExists("($.k1 == 10) is unknown"));
            ValidateError(db, jsonExists("$.k1 == 10"));
            ValidateError(db, jsonExists("$.k1 != 10"));
            ValidateError(db, jsonExists("$.k1 > 10"));
            ValidateError(db, jsonExists("$.k1 < 10"));
            ValidateError(db, jsonExists("$.k1 >= 10"));
            ValidateError(db, jsonExists("$.k1 <= 10"));
            ValidateError(db, jsonExists("!($.k1 == 10)"));
            ValidateError(db, jsonExists("$.k1 == 10 && $.k2 == 20"));
            ValidateError(db, jsonExists("$.k1 == 10 || $.k2 == 20"));

            ValidatePredicate(db, jsonExists("$ ? (exists(@.k1))"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 starts with \"abc\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 like_regex \"abc\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 10)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 != 10)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 > 10)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 < 10)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 >= 10)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 <= 10)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 10 && @.k2 == 20)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 10 || @.k2 == 20)"));

            ValidatePredicate(db, jsonExists("$.k1 ? (exists(@))"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ starts with \"abc\")"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ like_regex \"abc\")"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ == 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ != 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ > 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ < 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ >= 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ <= 10)"));

            // Nested predicates are not allowed even in a filter
            ValidateError(db, jsonExists("$ ? ((@.k1 == 10) is unknown)"));
            ValidateError(db, jsonExists("$ ? (!(@.k1 == 10))"));
        });
    }

    Y_UNIT_TEST_QUAD(SelectJsonExists_Literals, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidateError(db, jsonExists("null"));
            ValidateError(db, jsonExists("1"));
            ValidateError(db, jsonExists("\"str\""));
            ValidateError(db, jsonExists("true"));
            ValidateError(db, jsonExists("false"));
        });
    }

    // Filter with != (inequality) and range comparisons (<, <=, >, >=)
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterInequality, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? (@.k1 != 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k3 != \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k5 != null)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k4 != false)"));

            ValidatePredicate(db, jsonExists("$.k1 ? (@ != 1)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ != null)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@ != \"1\")"));

            ValidatePredicate(db, jsonExists("$ ? (@.k1 < 0)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 <= 0)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 > 0)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 >= 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k2 < 0)"));
            ValidatePredicate(db, jsonExists("$ ? (0 < @.k1)"));
            ValidatePredicate(db, jsonExists("$ ? (0 >= @.k2)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 > 999)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k99 != 1)"));
        });
    }

    // Three-way AND/OR and mixed (AND+OR) filter predicates
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterAndOrComplex, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 && @.k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == \"1\" && @.k2 == \"22\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 && @.k2 == false)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 >= 0 && @.k1 <= 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k4 == true && @.k5 == null)"));

            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 || @.k1 == 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k4 == true || @.k2 == false)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == \"1\" || @.k2 == \"22\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k99 == 1 || @.k98 == 2)"));

            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 && @.k3 == \"text\" && @.k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 && @.k3 == \"text\" && @.k4 == false)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 || @.k1 == 1 || @.k1 == \"1\")"));

            // Mixing AND and OR inside filter: OR wins, index search uses OR semantics
            ValidatePredicate(db, jsonExists("$ ? ((@.k1 == 0 && @.k4 == true) || @.k2 == \"22\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 1 || (@.k1 == \"1\" && @.k2 == \"22\"))"));

            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1 == 10 || @.k1 == 20)"));
            ValidatePredicate(db, jsonExists("$.k2 ? (@.k1 == 2 && @.k2 == true)"));
        });
    }

    // Filter with arithmetic operators combined with && and ||: OR dominance
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterArithmeticWithBooleanOps, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? (@.k1 + @.k2 == 5 || @.k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 - @.k2 > 0 || @.k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 * @.k2 != 0 || @.k5 == null)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 / @.k2 < 1 || @.k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 % @.k2 == 0 || @.k4 == false)"));

            ValidatePredicate(db, jsonExists("$ ? (@.k1 + @.k2 == 5 && @.k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 - @.k2 > 0 && @.k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 * @.k2 != 0 && @.k5 == null)"));

            ValidatePredicate(db, jsonExists("$ ? (@.k1 + @.k2 == 5 || @.k3 + @.k4 == 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 - @.k2 > 0 || @.k3 - @.k4 < 0)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 + @.k2 == 5 && @.k3 + @.k4 == 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 + @.k2 == 5 || @.k3 - @.k4 < 0 || @.k5 == null)"));

            ValidatePredicate(db, jsonExists("$ ? ((@.k1 + @.k2 == 5 && @.k3 == \"text\") || @.k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 || (@.k1 + @.k2 == 5 && @.k3 == \"text\"))"));

            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1 + @.k2 == 5 || @.k1 == 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1 - @.k2 > 0 && @.k1 == 10)"));
        });
    }

    // Filter with path-vs-path comparison operators combined with && and ||: OR dominance
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterComparisonWithBooleanOps, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? (@.k1 < @.k2 || @.k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 > @.k2 || @.k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 <= @.k2 || @.k5 == null)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 >= @.k2 || @.k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == @.k2 || @.k4 == false)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 != @.k2 || @.k3 == \"text\")"));

            ValidatePredicate(db, jsonExists("$ ? (@.k1 < @.k2 && @.k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 > @.k2 && @.k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 != @.k2 && @.k5 == null)"));

            ValidatePredicate(db, jsonExists("$ ? (@.k1 < @.k2 || @.k3 > @.k4)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == @.k2 || @.k3 != @.k4)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 < @.k2 && @.k3 > @.k4)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 < @.k2 && @.k3 > @.k4 || @.k5 == null)"));

            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 || @.k2 < @.k3)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 < @.k2 || @.k3 == \"text\" || @.k4 == true)"));

            ValidatePredicate(db, jsonExists("$ ? ((@.k1 < @.k2 && @.k3 > @.k4) || @.k5 == null)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 == 0 || (@.k2 < @.k3 && @.k4 == true))"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1 < @.k2 || @.k1 == 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1 > @.k2 && @.k1 == 10)"));
        });
    }

    // Filter with paths: deep nesting, array subscripts inside filter, empty key
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterPathsDeep, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? (@.k1.k2.k3.k4 == \"1\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1.k2.k3.k4 == \"2\")"));

            ValidatePredicate(db, jsonExists("$ ? (@.k6[0] == 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k6[1] == \"1\")"));
            ValidatePredicate(db, jsonExists("$ ? (@.k6[2] == false)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k6[0] == false)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k6[123] == null)"));

            ValidatePredicate(db, jsonExists("$ ? (@.k1[0] == 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1[1] == 2)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1[2] == 3)"));

            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1 == 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1 == 20)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1 == 999)"));

            ValidatePredicate(db, jsonExists("$[*] ? (@.k1 == \"1\")"));
            ValidatePredicate(db, jsonExists("$[0] ? (@.k1 == \"1\")"));
            ValidatePredicate(db, jsonExists("$[*] ? (@.\"\" == \"\")"));

            ValidatePredicate(db, jsonExists("$ ? (@.\"\" == null)"));
            ValidatePredicate(db, jsonExists("$ ? (@.\"\" == 1)"));
            ValidatePredicate(db, jsonExists("$ ? (@.\"\" == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.\"\" == false)"));
            ValidatePredicate(db, jsonExists("$ ? (@.\"\" == \"1\")"));
        });
    }

    // Combined key access + array subscript + method with filter
    Y_UNIT_TEST_QUAD(SelectJsonExists_PathArrayMethodWithFilter, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$.k1[*] ? (@.k1 == 10)"));
            ValidatePredicate(db, jsonExists("$.k1[0] ? (@.k1 == 10)"));
            ValidatePredicate(db, jsonExists("$.k1[last] ? (@.k1 == 20)"));
            ValidatePredicate(db, jsonExists("$.k1[*] ? (@.k1 == 999)"));

            ValidatePredicate(db, jsonExists("$.* ? (@ == 1)"));
            ValidatePredicate(db, jsonExists("$.* ? (@ == \"1\")"));
            ValidatePredicate(db, jsonExists("$.* ? (@ == true)"));
            ValidatePredicate(db, jsonExists("$.* ? (@ == null)"));
            ValidatePredicate(db, jsonExists("$.* ? (@ == 42)"));

            ValidatePredicate(db, jsonExists("$.k1.size() ? (@ == 3)"));
            ValidatePredicate(db, jsonExists("$.k1.size() ? (@ > 0)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.size() == 3)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.size() > 0)"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.k2.k3 != null)"));
            ValidatePredicate(db, jsonExists("$.k2 ? (@.k1 == 2 && @.k2 == true)"));
            ValidatePredicate(db, jsonExists("$.k2 ? (@.k1 == 2 || @.k2.type() == \"boolean\")"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@.k1.abs() - @.k2.abs()) == 0)"));
        });
    }

    // Nested filter: result of an inner filter (@ ? (pred)) is accessed as an object
    Y_UNIT_TEST_QUAD(SelectJsonExists_NestedFilter, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0)).k2 == -1.5)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0)).k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0)).k4 == false)"));

            ValidatePredicate(db, jsonExists("$.k2 ? ((@ ? (@.k1 == 2)).k2 == true)"));
            ValidatePredicate(db, jsonExists("$.k2 ? ((@ ? (@.k1 == 2)).k2 == false)"));

            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k1 == 10)).k1 == 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k1 == 20)).k1 == 20)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k1 == 99)).k1 == 99)"));

            ValidatePredicate(db, jsonExists("$[*] ? ((@ ? (@.k1 == \"1\")).k2 == \"22\")"));
            ValidatePredicate(db, jsonExists("$[*] ? ((@ ? (@.k1 == \"x\")).k2 == \"22\")"));

            ValidatePredicate(db, jsonExists("$.k1.k2.k3 ? ((@ ? (@.k2 == \"b\")).k2 == \"b\")"));
            ValidatePredicate(db, jsonExists("$.k1.k2.k3 ? ((@ ? (@.k2 == \"b\")).k1 == \"b\")"));

            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 != 0)).k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 > 0)).k2 == \"22\")"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k2 < 0)).k1 == 0)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k1 >= 10)).k1 > 0)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k1 <= 10)).k1 == 10)"));

            ValidatePredicate(db, jsonExists("$ ? ((@ ? (0 == @.k1)).k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (\"1\" == @.k1)).k2 == \"22\")"));

            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.\"\" == null)).\"\" == null)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.\"\" == 1)).\"\" == 1)"));
            ValidatePredicate(db, jsonExists("$[*] ? ((@ ? (@.\"\" == \"\")).\"\" == \"\")"));
        });
    }

    // Nested filter where the inner predicate uses AND or OR
    Y_UNIT_TEST_QUAD(SelectJsonExists_NestedFilterAndOr, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0 && @.k4 == true)).k5 == null)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0 && @.k3 == \"text\")).k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0 && @.k4 == false)).k5 == null)"));

            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == \"1\" && @.k2 == \"22\")).k1 == \"1\")"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == \"1\" && @.k2 == \"99\")).k1 == \"1\")"));

            ValidatePredicate(db, jsonExists("$.k2 ? ((@ ? (@.k1 == 2 && @.k2 == true)).k2 == true)"));

            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0 || @.k1 == 1)).k3 == \"text\")"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0 || @.k1 == 1)).k4 == true)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == \"1\" || @.k2 == \"22\")).k2 == \"22\")"));

            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k4 == true || @.k5 == null)).k1 == 0)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k99 == 1 || @.k98 == 2)).k1 == 0)"));

            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k1 == 10 || @.k1 == 20)).k1 > 0)"));
        });
    }

    // Nested filter combined with other path constructs: array subscript, wildcards, double nesting
    Y_UNIT_TEST_QUAD(SelectJsonExists_NestedFilterPaths, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$ ? ((@[0] ? (@.k1 == \"1\")).k2 == \"22\")"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@[0] ? (@.k1 == 10)).k1 == 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@[last] ? (@.k1 == 20)).k1 == 20)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@[0] ? (@.k1 == 99)).k1 == 10)"));

            ValidatePredicate(db, jsonExists("$[*] ? ((@[*] ? (@.k1 == 1)).k1 == 1)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@[*] ? (@.k1 == 10)).k1 == 10)"));

            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k2.k3.k4 == \"1\")).k2.k3.k4 == \"1\")"));
            ValidatePredicate(db, jsonExists("$.k1.k2 ? ((@ ? (@.k4[0] == 0)).k3[0].k1 == \"a\")"));

            ValidatePredicate(db, jsonExists("$ ? ((@ ? ((@ ? (@.k1 == 0)).k4 == true)).k5 == null)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? ((@ ? (@.k1 == 10)).k1 == 10)).k1 > 0)"));

            ValidatePredicate(db, jsonExists("$ ? (exists($.k1 ? ((@ ? (@.k1 == 10)).k1 > 0)))"));
            ValidatePredicate(db, jsonExists("$ ? (exists($.k1.k2.k3 ? ((@ ? (@.k2 == \"b\")).k2 == \"b\")))"));

            ValidatePredicate(db, jsonExists("$.k1.k2.k3 ? ((@ ? (@.k1 == \"a\")).k1 starts with \"a\")"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k3 == \"text\")).k3 starts with \"tex\")"));
        });
    }

    // Combined key access + array subscript + methods + predicates + filters + literals + nested filter + AND/OR
    Y_UNIT_TEST_QUAD(SelectJsonExists_Mix, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$.k1[*] ? (exists(@.k1 ? (@.type() starts with \"s\")))"));
            ValidatePredicate(db, jsonExists("$.k1 ? (@.k2[*].k3 != null && -@.k1.floor() > +3)"));

            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k1 == 10 || @.k1 == 20)).k1 > 0)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k1 == 10 || @.k1 == 20)).k1 <= 10)"));
            ValidatePredicate(db, jsonExists("$.k1 ? ((@ ? (@.k1 == 10 || @.k1 == 20)).k1 < 0)"));

            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0 && @.k4 == true)).k2 < 0)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0 && @.k4 == true)).k2 >= -2)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0 && @.k4 == true)).k3 != \"blah\")"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k1 == 0 && @.k4 == true)).k2 > -1)"));

            ValidatePredicate(db, jsonExists("$.k1[0] ? ((@ ? (@.k1 == 10)).k1 >= 10)"));
            ValidatePredicate(db, jsonExists("$.k1[last] ? ((@ ? (@.k1 == 20)).k1 > 15)"));
            ValidatePredicate(db, jsonExists("$.k1[0] ? ((@ ? (@.k1 == 10)).k1 < 5)"));

            ValidatePredicate(db, jsonExists("$.k2 ? (-@.k1 < 0 && @.k2 == true)"));
            ValidatePredicate(db, jsonExists("$ ? (@.k1 <= 1 && @.k2 < 0)"));

            ValidatePredicate(db, jsonExists("$.k1 ? (@.k1.abs() > 5 && @.k1 != null)"));

            ValidatePredicate(db, jsonExists("$ ? (exists(@.k1) && @.k2 < 0)"));

            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k3 starts with \"te\")).k2 < 0)"));
            ValidatePredicate(db, jsonExists("$ ? ((@ ? (@.k3 starts with \"te\")).k1 != 99)"));

            ValidatePredicate(db, jsonExists("$.k1.k2.k3[*] ? ((@ ? (@.k1 == \"a\")).k1 > \"\")"));
            ValidatePredicate(db, jsonExists("$.k1.k2.k4[*] ? (@ != null && @ > 0)"));
        });
    }

    Y_UNIT_TEST_TWIN(SelectJsonValue_RequiresReturning, IsJsonDocument) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::nullopt, [](TQueryClient& db, const auto&) {
            // Main table scan works fine without RETURNING
            {
                auto result = db.ExecuteQuery(
                    R"(SELECT Key FROM TestTable WHERE JSON_VALUE(Text, '$.k1') == "1"u ORDER BY Key;)",
                    TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            // Index query without RETURNING fails at compile time
            {
                auto result = db.ExecuteQuery(
                    R"(SELECT Key FROM TestTable VIEW json_idx WHERE JSON_VALUE(Text, '$.k1') == "1"u ORDER BY Key;)",
                    TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                    "RETURNING clause is required for JSON_VALUE in JSON index predicates");
            }

            // With RETURNING: index and main table return the same results
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "1"u)");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 1l)");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN ("1"u, "v"u))");
        });
    }

    Y_UNIT_TEST_TWIN(SelectJsonValue_InListParam, IsJsonDocument) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::nullopt, [](TQueryClient& db, const auto&) {
            // JV IN $p
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN $p)",
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Utf8("1")
                    .AddListItem().Utf8("v")
                    .EndList().Build().Build());
            // JV IN ($p1, $p2)
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN ($p1, $p2))",
                TParamsBuilder()
                    .AddParam("$p1").Utf8("1").Build()
                    .AddParam("$p2").Utf8("v").Build()
                    .Build());
            // JV IN (l1, l2)
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN ("1"u, "v"u))");

            // Integer
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) IN $p)",
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Int64(1)
                    .AddListItem().Int64(0)
                    .EndList().Build().Build());
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) IN ($p1, $p2))",
                TParamsBuilder()
                    .AddParam("$p1").Int64(1).Build()
                    .AddParam("$p2").Int64(0).Build()
                    .Build());
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) IN (1l, 0l))");

            // Finished path
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1.type()' RETURNING Utf8) IN $p)",
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Utf8("string")
                    .AddListItem().Utf8("number")
                    .EndList().Build().Build());
        });
    }
}

Y_UNIT_TEST_SUITE(KqpJsonIndexTokens) {
    Y_UNIT_TEST(JsonExists) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Basic path exists cases
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.key'))", {"\4key"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 2)'))", {"\3k1\3k2" + numSuffix(2)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == true && @.k2 == false)'))", {"\3k1" + trueSuffix, "\3k2" + falseSuffix}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")'))", {"\3k1" + nullSuffix, "\3k2" + strSuffix("str")}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.key') == true)", {"\4key"});

            // Negated JSON_EXISTS is not supported by JSON index
            ValidateError(db, R"(JSON_EXISTS(Text, '$.key') == false)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.key') != true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.key') IS NULL)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.key') IS NOT NULL)"); // returns false != null -> exists

            // AND combinations
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null && @.k2 == "str")') AND JSON_EXISTS(Text, '$ ? (@.k3 == true && @.k4 == false)'))",
                {"\3k1" + nullSuffix, "\3k2" + strSuffix("str"), "\3k3" + trueSuffix, "\3k4" + falseSuffix}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")') AND JSON_EXISTS(Text, '$ ? (@.k3 == true && @.k4 == false)'))",
                {"\3k1" + nullSuffix, "\3k2" + strSuffix("str"), "\3k3" + trueSuffix, "\3k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null && @.k2 == "str")') AND JSON_EXISTS(Text, '$ ? (@.k3 == true || @.k4 == false)'))",
                {"\3k1" + nullSuffix, "\3k2" + strSuffix("str"), "\3k3" + trueSuffix, "\3k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")') AND JSON_EXISTS(Text, '$ ? (@.k3 == true || @.k4 == false)'))",
                {"\3k1" + nullSuffix, "\3k2" + strSuffix("str"), "\3k3" + trueSuffix, "\3k4" + falseSuffix}, "or");

            // OR combinations
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))",
                {"\3k1", "\3k2"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null && @.k2 == "str")') OR JSON_EXISTS(Text, '$ ? (@.k3 == true && @.k4 == false)'))",
                {"\3k1" + nullSuffix, "\3k2" + strSuffix("str"), "\3k3" + trueSuffix, "\3k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")') OR JSON_EXISTS(Text, '$ ? (@.k3 == true && @.k4 == false)'))",
                {"\3k1" + nullSuffix, "\3k2" + strSuffix("str"), "\3k3" + trueSuffix, "\3k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null && @.k2 == "str")') OR JSON_EXISTS(Text, '$ ? (@.k3 == true || @.k4 == false)'))",
                {"\3k1" + nullSuffix, "\3k2" + strSuffix("str"), "\3k3" + trueSuffix, "\3k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")') OR JSON_EXISTS(Text, '$ ? (@.k3 == true || @.k4 == false)'))",
                {"\3k1" + nullSuffix, "\3k2" + strSuffix("str"), "\3k3" + trueSuffix, "\3k4" + falseSuffix}, "or");

            // Mixed combinations
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))",
                {"\3k1", "\3k2", "\3k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))",
                {"\3k1", "\3k2", "\3k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3'))",
                {"\3k1", "\3k2", "\3k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3'))",
                {"\3k1", "\3k2", "\3k3"}, "or");
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')) AND JSON_EXISTS(Text, '$.k3'))",
                {"\3k1", "\3k2", "\3k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND (JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3')))",
                {"\3k1", "\3k2", "\3k3"}, "and");
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')) AND (JSON_EXISTS(Text, '$.k3') AND JSON_EXISTS(Text, '$.k4')))",
                {"\3k1", "\3k2", "\3k3", "\3k4"}, "and");
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')) OR JSON_EXISTS(Text, '$.k3'))",
                {"\3k1", "\3k2", "\3k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') OR (JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3')))",
                {"\3k1", "\3k2", "\3k3"}, "or");
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')) OR (JSON_EXISTS(Text, '$.k3') OR JSON_EXISTS(Text, '$.k4')))",
                {"\3k1", "\3k2", "\3k3", "\3k4"}, "or");

            // AND with non-indexable predicate
            ValidateTokens(db,
                R"(Data = "d1" AND JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND Data = "d1" AND JSON_EXISTS(Text, '$.k2'))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND Data = "d1")",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(Data = "d1" AND JSON_EXISTS(Text, '$.k1'))",
                {"\3k1"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND Data = "d1")",
                {"\3k1"}, "and");
            ValidateTokens(db,
                R"(Data = "d1" AND JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))",
                {"\3k1", "\3k2", "\3k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND Data = "d1" AND JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))",
                {"\3k1", "\3k2", "\3k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND Data = "d1" AND JSON_EXISTS(Text, '$.k3'))",
                {"\3k1", "\3k2", "\3k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3') AND Data = "d1")",
                {"\3k1", "\3k2", "\3k3"}, "and");

            // OR with non-indexable predicate - not extractable
            ValidateError(db, R"(Data = "d1" OR JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR Data = "d1" OR JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR Data = "d1")");
            ValidateError(db, R"(Data = "d1" OR JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR Data = "d1")");
            ValidateError(db, R"(Data = "d1" OR JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR Data = "d1" OR JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR Data = "d1" OR JSON_EXISTS(Text, '$.k3'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3') OR Data = "d1")");

            // Mixed AND/OR with Data - not extractable if the non-indexable predicate is on the OR branch
            ValidateError(db, R"(Data = "d1" OR JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR Data = "d1")");
            ValidateError(db, R"(Data = "d1" OR JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR Data = "d1" OR JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR Data = "d1")");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') OR Data = "d1")");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') AND Data = "d1")", {"\3k1", "\3k2"}, "or");
            ValidateTokens(db, R"((JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')) AND Data = "d1")", {"\3k1", "\3k2"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR Data = "d1" AND JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND Data = "d1" OR JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "or");

            // NOT JSON_EXISTS and wrapped-NOT forms fall through to "nothing to extract"
            ValidateError(db, R"(NOT JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')))");
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')))");

            // Filter equality - covers every literal type, the token carries the value suffix
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == null)'))", {"\3k1\3k2" + nullSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == true)'))", {"\3k1\3k2" + trueSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == false)'))", {"\3k1\3k2" + falseSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == "abc")'))", {"\3k1\3k2" + strSuffix("abc")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 42)'))", {"\3k1\3k2" + numSuffix(42)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == -1.5)'))", {"\3k1\3k2" + numSuffix(-1.5)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 == @.k2)'))", {"\3k1\3k2" + numSuffix(2)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? ("s" == @.k2)'))", {"\3k1\3k2" + strSuffix("s")});

            // Filter inequality / range - path only, no value suffix
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 != 2)'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 > 2)'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 < 2)'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 >= 2)'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 <= 2)'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 != null)'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 != "abc")'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 > @.k2)'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 < @.k2)'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 >= @.k2)'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 <= @.k2)'))", {"\3k1\3k2"});

            // Filter path-vs-path comparisons - two tokens, AND mode (value suffix dropped)
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == @.k2)'))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 != @.k2)'))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 > @.k2)'))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 < @.k2)'))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 >= @.k2)'))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 <= @.k2)'))", {"\3k1", "\3k2"}, "and");

            // Filter arithmetic (path vs literal) - path only, no value suffix
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + 1 == 2)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 - 1 == 0)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 * 2 == 4)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 / 2 == 1)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 % 2 == 0)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + 1 > 2)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 * 2 != 4)'))", {"\3k1"});

            // Filter arithmetic (path vs path) - two tokens, AND mode
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + @.k2 == 5)'))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 - @.k2 > 0)'))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 * @.k2 < 10)'))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 / @.k2 >= 1)'))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 % @.k2 != 0)'))", {"\3k1", "\3k2"}, "and");

            // Filter unary operators - path only
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (-@.k1 == -1)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (+@.k1 == 1)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (-@.k1 > 0)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.abs() == 1)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.abs() > 5)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.size() == 3)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.size() > 0)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.size() ? (@ == 3)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (-@.k1.abs() == -1)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.abs() - @.k2.abs() == 0)'))", {"\3k1", "\3k2"}, "and");

            // && / || inside jsonpath - mode propagates from inner operator
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 && @.k2 != 2)'))",
                {"\3k1" + numSuffix(1), "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 || @.k2 != 2)'))",
                {"\3k1" + numSuffix(1), "\3k2"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 > 0 && @.k2 < 10)'))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 > 0 || @.k2 < 10)'))",
                {"\3k1", "\3k2"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + 1 == 2 && @.k2 * 2 == 4)'))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + 1 == 2 || @.k2 * 2 == 4)'))",
                {"\3k1", "\3k2"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.abs() == 1 && @.k2.size() > 0)'))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (-@.k1 == -1 || @.k2 % 2 == 0)'))",
                {"\3k1", "\3k2"}, "or");

            // Three-way && / || inside jsonpath
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 && @.k2 == 2 && @.k3 == 3)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 || @.k2 == 2 || @.k3 == 3)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "or");

            // Mixed && and || inside jsonpath
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@.k1 == 1 && @.k2 == 2) || @.k3 == 3)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 || (@.k2 == 2 && @.k3 == 3))'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@.k1 > 0 && @.k2 != null) || @.k3 == "text")'))",
                {"\3k1", "\3k2", "\3k3" + strSuffix("text")}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + @.k2 == 5 && @.k3 == "text" || @.k4 == null)'))",
                {"\3k1", "\3k2", "\3k3" + strSuffix("text"), "\3k4" + nullSuffix}, "or");

            // Outer SQL AND/OR over filters with &&/|| inside
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 && @.k2 == 2)') AND JSON_EXISTS(Text, '$ ? (@.k3 > 0)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 || @.k2 == 2)') AND JSON_EXISTS(Text, '$ ? (@.k3 > 0)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 && @.k2 == 2)') OR JSON_EXISTS(Text, '$ ? (@.k3 > 0)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 + @.k2 == 5)') AND JSON_EXISTS(Text, '$ ? (-@.k3 == -3)'))",
                {"\3k1", "\3k2", "\3k3"}, "and");

            // Outer range comparison with bool literal (non-equality): errors
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') > true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') >= true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') < true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') <= true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') > false)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') >= false)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') < false)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') <= false)");

            // Flipped side (literal op JSON_EXISTS)
            ValidateError(db, R"(true > JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(false >= JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(false == JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(true != JSON_EXISTS(Text, '$.k1'))");

            // JSON_EXISTS comparison with true rewrites to JSON_EXISTS without boolean comparison
            ValidateTokens(db, R"(true == JSON_EXISTS(Text, '$.k1'))", {"\3k1"});
            ValidateTokens(db, R"(false != JSON_EXISTS(Text, '$.k1'))", {"\3k1"});

            // JSON_EXISTS comparison with false rewrites to NOT JSON_EXISTS
            ValidateError(db, R"(false == JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(true != JSON_EXISTS(Text, '$.k1'))");

            // Outer comparison between two JSON_EXISTS: errors
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') == JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') != JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') > JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') >= JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') < JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') <= JSON_EXISTS(Text, '$.k2'))");

            // Outer AND/OR over range comparisons with bool literal
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') >= true AND JSON_EXISTS(Text, '$.k2') <= true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') > false OR JSON_EXISTS(Text, '$.k2') < true)");
            // AND: non-indexable range cmp on k1, but standalone JE($.k2) IS indexable - post-filter applies
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') >= true AND JSON_EXISTS(Text, '$.k2'))", {"\3k2"});
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') >= true OR JSON_EXISTS(Text, '$.k2') == true)");

            // Outer AND/OR over cross JSON_EXISTS comparisons
            // AND: JE1 > JE2 not indexable, but standalone JE($.k3) IS indexable - post-filter applies
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') > JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))", {"\3k3"});
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') != JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3'))");
            ValidateError(db, R"((JSON_EXISTS(Text, '$.k1') >= JSON_EXISTS(Text, '$.k2')) AND (JSON_EXISTS(Text, '$.k3') <= JSON_EXISTS(Text, '$.k4')))");
            ValidateError(db, R"((JSON_EXISTS(Text, '$.k1') == JSON_EXISTS(Text, '$.k2')) OR (JSON_EXISTS(Text, '$.k3') != JSON_EXISTS(Text, '$.k4')))");

            // NOT of these outer comparisons falls through to "nothing to extract"
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') > true))");
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') >= false))");
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') < true))");
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') <= false))");
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') == JSON_EXISTS(Text, '$.k2')))");
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') > JSON_EXISTS(Text, '$.k2')))");

            // Nested JSON_QUERY as JSON source for JSON_EXISTS - not extractable
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(Text, '$.k1' WITHOUT ARRAY WRAPPER), '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(Text, 'lax $.a' WITHOUT ARRAY WRAPPER), 'lax $.b'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(Text, 'strict $.a' WITHOUT ARRAY WRAPPER), 'strict $.b'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(Text, '$.a' WITH CONDITIONAL WRAPPER), '$.b'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(Text, '$.a' WITH UNCONDITIONAL WRAPPER), '$.b'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(Text, '$ ? (@.x == 1)' WITHOUT ARRAY WRAPPER), '$.y'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(Text, '$.a[0]' WITHOUT ARRAY WRAPPER), '$.b'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.b' WITHOUT ARRAY WRAPPER), '$.c'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(JSON_QUERY(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.b' WITHOUT ARRAY WRAPPER), '$.c' WITHOUT ARRAY WRAPPER), '$.d'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(JSON_QUERY(JSON_QUERY(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.b' WITHOUT ARRAY WRAPPER), '$.c' WITHOUT ARRAY WRAPPER), '$.d' WITHOUT ARRAY WRAPPER), '$.e'))");
            ValidateError(db, R"(JSON_EXISTS(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$ ? (@.k1 == 1 && @.k2 == 2)') == true)");

            // AND: JE in JSON_QUERY + indexable JE -> extract indexable
            ValidateTokens(db, R"(JSON_EXISTS(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.k1') AND JSON_EXISTS(Text, '$.k2'))", {"\3k2"});
            // OR: indexable JE + JE in JSON_QUERY -> error
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.b'))");

            // JSON_EXISTS with TRUE ON ERROR is negation
            ValidateError(db, R"(JSON_EXISTS(Text, '$.key' TRUE ON ERROR))");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.key' FALSE ON ERROR))", {"\4key"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.key' ERROR ON ERROR))", {"\4key"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.key' UNKNOWN ON ERROR))", {"\4key"});
        });
    }

    // JSON index does not support JSON_QUERY, every predicate that references it should fail term extraction
    Y_UNIT_TEST(JsonQuery) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateError(db, R"(JSON_QUERY(Text, '$.k1') IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, '$.k1' WITHOUT ARRAY WRAPPER) IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, 'lax $.k1' WITHOUT ARRAY WRAPPER) IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, 'strict $.k1' WITHOUT ARRAY WRAPPER) IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, 'strict $.k1' WITHOUT ARRAY WRAPPER NULL ON EMPTY) IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, 'strict $.k1' WITHOUT ARRAY WRAPPER NULL ON ERROR) IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, '$.k1' WITH UNCONDITIONAL WRAPPER) IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, '$.k1' WITH CONDITIONAL WRAPPER) IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, '$.k1' WITH UNCONDITIONAL ARRAY WRAPPER) IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, '$ ? (@.k1 == 1)') IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, '$.k1.*') IS NOT NULL)");
            ValidateError(db, R"(JSON_QUERY(Text, '$.k1[0]') IS NOT NULL)");
            ValidateError(db, R"(NOT (JSON_QUERY(Text, '$.k1') IS NOT NULL))");
            ValidateError(db, R"((JSON_QUERY(Text, '$.k1') IS NULL))");

            ValidateError(db, R"((JSON_QUERY(Text, '$.k1') IS NOT NULL) AND (JSON_QUERY(Text, '$.k2') IS NOT NULL))");
            ValidateError(db, R"((JSON_QUERY(Text, '$.k1') IS NOT NULL) OR (JSON_QUERY(Text, '$.k2') IS NOT NULL))");
            ValidateError(db, R"((JSON_QUERY(Text, '$.k1') IS NOT NULL) OR (JSON_QUERY(Text, '$.k2') IS NOT NULL) AND (JSON_QUERY(Text, '$.k3') IS NOT NULL))");
            ValidateError(db, R"((JSON_QUERY(Text, '$.k1') IS NOT NULL) AND (JSON_QUERY(Text, '$.k2') IS NOT NULL) OR (JSON_QUERY(Text, '$.k3') IS NOT NULL))");
            ValidateError(db, R"(((JSON_QUERY(Text, '$.k1') IS NOT NULL) AND (JSON_QUERY(Text, '$.k2') IS NOT NULL)) OR (JSON_QUERY(Text, '$.k3') IS NOT NULL))");

            ValidateError(db, R"((JSON_EXISTS(Text, '$.k1') AND (JSON_QUERY(Text, '$.k2') IS NOT NULL)))");
            ValidateError(db, R"((JSON_EXISTS(Text, '$.k1') OR (JSON_QUERY(Text, '$.k2') IS NOT NULL)))");
            ValidateError(db, R"((JSON_VALUE(Text, '$.k1' RETURNING Utf8) = "1") AND (JSON_QUERY(Text, '$.k2') IS NOT NULL))");
        });
    }

    // RETURNING clause is mandatory for JSON_VALUE in JSON index predicates
    Y_UNIT_TEST(JsonValueRequiresReturning) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            static const char* kErr = "RETURNING clause is required for JSON_VALUE in JSON index predicates";

            // Without RETURNING
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1') == "v"u)", kErr);
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1') != "v"u)", kErr);
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1') > "a"u)", kErr);
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1') < "z"u)", kErr);
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1') IN ("a"u, "b"u))", kErr);
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1.k2') == "v"u)", kErr);
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1.type()') == "v"u)", kErr);
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1') == JSON_VALUE(Text, '$.k2' RETURNING Utf8))", kErr);
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == JSON_VALUE(Text, '$.k2'))", kErr);

            // With RETURNING
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "v"u)", {"\3k1" + strSuffix("v")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING String) == "v"s)", {"\3k1" + strSuffix("v")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 1l)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Double) == 1.0)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool))", {"\3k1" + trueSuffix});
        });
    }

    Y_UNIT_TEST(JsonValue) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Supported RETURNING types
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int8) == 1t)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint8) == 1ut)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int16) == 1s)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint16) == 1us)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint32) == 1u)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 1l)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint64) == 1ul)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Float) == 1.0f)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Double) == 1.0)", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING String) == "value"s)", {"\3k1" + strSuffix("value")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "value"u)", {"\3k1" + strSuffix("value")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == true)", {"\3k1" + trueSuffix});

            // Not supported RETURNING types
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Date) == Date("2021-01-01"))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Datetime) == Datetime("2021-01-01T00:00:00Z"))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Timestamp) == Timestamp("2021-01-01T00:00:00Z"))");

            // Explicit RETURNING Utf8 (implicit default no longer allowed)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "1")", {"\3k1" + strSuffix("1")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "string")", {"\3k1" + strSuffix("string")});

            // Negation
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Utf8) IS NULL)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Utf8) IS NOT NULL)"); 

            // JV(...) == true is equivalent to standalone JV(...) - collects trueSuffix token
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == true)", {"\3k1" + trueSuffix});
            ValidateTokens(db, R"(true == JSON_VALUE(Text, '$.k1' RETURNING Bool))", {"\3k1" + trueSuffix});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) != false)", {"\3k1" + trueSuffix});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool))", {"\3k1" + trueSuffix});

            // JV comparison with false rewrites to NOT JSON_VALUE
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == false)");
            ValidateError(db, R"(false == JSON_VALUE(Text, '$.k1' RETURNING Bool))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) != true)");
            ValidateError(db, R"(NOT JSON_VALUE(Text, '$.k1' RETURNING Bool))");

            // JV RETURNING Bool with range comparisons - not extractable
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) > true)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) >= true)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) < true)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) <= true)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) > false)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) >= false)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) < false)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) <= false)");

            // Comparison with other literals
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 10)", {"\3k1" + numSuffix(10)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) > 10)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) < 10)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) >= 10)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) <= 10)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) != 10)", {"\3k1"});

            // JV op JV - both collectable
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == JSON_VALUE(Text, '$.k2' RETURNING Int32))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) != JSON_VALUE(Text, '$.k2' RETURNING Int32))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) > JSON_VALUE(Text, '$.k2' RETURNING Int32))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) >= JSON_VALUE(Text, '$.k2' RETURNING Int32))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) < JSON_VALUE(Text, '$.k2' RETURNING Int32))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) <= JSON_VALUE(Text, '$.k2' RETURNING Int32))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k2' RETURNING Int32) == JSON_VALUE(Text, '$.k1' RETURNING Int32))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == JSON_VALUE(Text, '$.k2' RETURNING Utf8))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) != JSON_VALUE(Text, '$.k2' RETURNING Utf8))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) > JSON_VALUE(Text, '$.k2' RETURNING Utf8))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) >= JSON_VALUE(Text, '$.k2' RETURNING Utf8))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) < JSON_VALUE(Text, '$.k2' RETURNING Utf8))", {"\3k1", "\3k2"}, "and");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) <= JSON_VALUE(Text, '$.k2' RETURNING Utf8))", {"\3k1", "\3k2"}, "and");

            // JSON_VALUE RETURNING Bool comparison is not supported
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == JSON_VALUE(Text, '$.k2' RETURNING Bool))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) != JSON_VALUE(Text, '$.k2' RETURNING Bool))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) > JSON_VALUE(Text, '$.k2' RETURNING Bool))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) >= JSON_VALUE(Text, '$.k2' RETURNING Bool))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) < JSON_VALUE(Text, '$.k2' RETURNING Bool))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) <= JSON_VALUE(Text, '$.k2' RETURNING Bool))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k2' RETURNING Bool) == JSON_VALUE(Text, '$.k1' RETURNING Bool))");
            ValidateError(db, R"(NOT (JSON_VALUE(Text, '$.k1' RETURNING Bool) == JSON_VALUE(Text, '$.k2' RETURNING Bool)))");
            ValidateError(db, R"(NOT (JSON_VALUE(Text, '$.k1' RETURNING Bool) > JSON_VALUE(Text, '$.k2' RETURNING Bool)))");

            // For some nodes inside the path, the collected result cannot be combined with == operator
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 starts with "1"' RETURNING Utf8) == "true")", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.size()' RETURNING Int32) == 2)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.*' RETURNING Int32) == 2)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[*]' RETURNING Int32) == 2)", {"\3k1" + numSuffix(2)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 + 1' RETURNING Int32) == 2)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == 2' RETURNING Bool))", {"\3k1" + numSuffix(2)});

            // BETWEEN clause (replaces with JSON_VALUE >= 1 AND JSON_VALUE <= 10)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) BETWEEN 1 AND 10)", {"\3k1"});

            // AND/OR combinations - numeric equality
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 AND JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2)",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2)}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2)",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2)}, "or");

            // AND/OR combinations - string equality
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" AND JSON_VALUE(Text, '$.k2' RETURNING Utf8) == "b")",
                {"\3k1" + strSuffix("a"), "\3k2" + strSuffix("b")}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" OR JSON_VALUE(Text, '$.k2' RETURNING Utf8) == "b")",
                {"\3k1" + strSuffix("a"), "\3k2" + strSuffix("b")}, "or");

            // AND/OR with range comparisons - path-only tokens
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) > 5 AND JSON_VALUE(Text, '$.k2' RETURNING Int32) < 10)",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) > 5 OR JSON_VALUE(Text, '$.k2' RETURNING Int32) < 10)",
                {"\3k1", "\3k2"}, "or");

            // AND/OR mixing equality and range
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" AND JSON_VALUE(Text, '$.k2' RETURNING Int32) > 0)",
                {"\3k1" + strSuffix("a"), "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" OR JSON_VALUE(Text, '$.k2' RETURNING Int32) > 0)",
                {"\3k1" + strSuffix("a"), "\3k2"}, "or");

            // Three-way AND/OR
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 AND JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2 AND JSON_VALUE(Text, '$.k3' RETURNING Int32) == 3)",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2 OR JSON_VALUE(Text, '$.k3' RETURNING Int32) == 3)",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "or");

            // Mixed AND/OR (AND binds tighter): both cases produce "or"
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 AND JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2 OR JSON_VALUE(Text, '$.k3' RETURNING Int32) == 3)",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "or");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2 AND JSON_VALUE(Text, '$.k3' RETURNING Int32) == 3)",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "or");

            // Comparison operators with strings - path-only token (no value suffix for non-equality)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) > "abc")", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) < "xyz")", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) >= "abc")", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) <= "xyz")", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) != "abc")", {"\3k1"});

            // Flipped operand order - string comparisons
            ValidateTokens(db, R"("abc" < JSON_VALUE(Text, '$.k1' RETURNING Utf8))", {"\3k1"});
            ValidateTokens(db, R"("abc" > JSON_VALUE(Text, '$.k1' RETURNING Utf8))", {"\3k1"});
            ValidateTokens(db, R"("abc" != JSON_VALUE(Text, '$.k1' RETURNING Utf8))", {"\3k1"});

            // Flipped operand order - numeric comparisons
            ValidateTokens(db, R"(10 < JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\3k1"});
            ValidateTokens(db, R"(10 > JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\3k1"});
            ValidateTokens(db, R"(10 >= JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\3k1"});
            ValidateTokens(db, R"(10 <= JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\3k1"});
            ValidateTokens(db, R"(10 != JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\3k1"});

            // STARTS WITH - path only token
            ValidateTokens(db, R"(StartsWith(JSON_VALUE(Text, '$.k1' RETURNING Utf8), "prefix"))", {"\3k1"});
            ValidateTokens(db, R"(StartsWith(JSON_VALUE(Text, '$.k1' RETURNING Utf8), "prefix") AND JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a")", {"\3k1" + strSuffix("a")});

            // ENDS WITH - path only token
            ValidateTokens(db, R"(EndsWith(JSON_VALUE(Text, '$.k1' RETURNING Utf8), "suffix"))", {"\3k1"});

            // LIKE - path only token / ILIKE - not extractable (Re2)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) LIKE "pattern%")", {"\3k1"});
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) ILIKE "pattern%")"); // udf

            // REGEXP - not extractable (Re2)
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) REGEXP "^pattern$")"); // udf

            // String concatenation (||) in a comparison: JV1 inside concat is not indexable
            // If JV1 is the only JSON node - nothing to extract
            ValidateError(db, R"((JSON_VALUE(Text, '$.k1' RETURNING Utf8) || "suffix") == "value_suffix")");
            // AND: JV1 inside concat is non-indexable, but JV2 == "b" IS indexable - post-filter applies
            ValidateTokens(db, R"((JSON_VALUE(Text, '$.k1' RETURNING Utf8) || "suffix") == "value_suffix" AND JSON_VALUE(Text, '$.k2' RETURNING Utf8) == "b")", {"\3k2" + strSuffix("b")});

            // Nested JSON_QUERY as JSON source for JSON_VALUE - not extractable
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(Text, '$.k1' WITHOUT ARRAY WRAPPER), '$.k2' RETURNING Int32) == 1)");
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(Text, 'lax $.a' WITHOUT ARRAY WRAPPER), '$.b' RETURNING Utf8) == "1"u)");
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(Text, 'strict $.a' WITHOUT ARRAY WRAPPER), '$.b' RETURNING Int64) == 1l)");
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(Text, '$.a' WITH CONDITIONAL WRAPPER), '$.b') == "x"u)");
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(Text, '$.a' WITH UNCONDITIONAL WRAPPER), '$.b' RETURNING String) == "s"s)");
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(Text, '$.a[0]' WITHOUT ARRAY WRAPPER), '$.b' RETURNING Double) == 1.0)");
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(Text, '$ ? (@.x == 1)' WITHOUT ARRAY WRAPPER), '$.y' RETURNING Bool) == true)");
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.b' WITHOUT ARRAY WRAPPER), '$.c' RETURNING Int32) == 1)");
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(JSON_QUERY(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.b' WITHOUT ARRAY WRAPPER), '$.c' WITHOUT ARRAY WRAPPER), '$.d') == "1")");
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(JSON_QUERY(JSON_QUERY(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.b' WITHOUT ARRAY WRAPPER), '$.c' WITHOUT ARRAY WRAPPER), '$.d' WITHOUT ARRAY WRAPPER), '$.e' RETURNING Int32) == 1)");

            // AND: JV1 inside JSON_QUERY is non-indexable, but JV2 == "w" IS indexable - post-filter applies
            ValidateTokens(db, R"(JSON_VALUE(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.k' RETURNING Utf8) == "v"u AND JSON_VALUE(Text, '$.b' RETURNING Utf8) == "w")", {"\2b" + strSuffix("w")});
            // OR: JV1 inside JSON_QUERY is non-indexable, but JV2 == "w" IS indexable - post-filter does not apply
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.k' RETURNING Utf8) == "v"u OR JSON_VALUE(Text, '$.b' RETURNING Utf8) == "w")");

            // DEFAULT ON EMPTY with non-NULL value is negation
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int DEFAULT 12 ON EMPTY) > 10)");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int ERROR ON EMPTY) > 10)", {"\4key"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int NULL ON EMPTY) > 10)", {"\4key"});

            // DEFAULT ON ERROR with non-NULL value is negation
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int DEFAULT 12 ON ERROR) > 10)");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int ERROR ON ERROR) > 10)", {"\4key"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int NULL ON ERROR) > 10)", {"\4key"});

            // Both DEFAULT ON EMPTY and DEFAULT ON ERROR with non-NULL value are negation too
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int DEFAULT 12 ON EMPTY DEFAULT 12 ON ERROR) > 10)");
        });
    }

    Y_UNIT_TEST(LargeIntegerPrecisionLoss) {
        // Int64/Uint64 values outside [-2^53, 2^53] lose precision when cast to double
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            constexpr double rounded = 9007199254740992.0;

            // Positive side: supported 2^53 - 1 / 2^53 and rounded 2^53 + 1
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 9007199254740991l)",
                {"\3k1" + numSuffix(rounded - 1.0)});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Uint64) == 9007199254740991ul)",
                {"\3k1" + numSuffix(rounded - 1.0)});

            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 9007199254740992l)",
                {"\3k1" + numSuffix(rounded)});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Uint64) == 9007199254740992ul)",
                {"\3k1" + numSuffix(rounded)});

            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 9007199254740993l)",
                {"\3k1"});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Uint64) == 9007199254740993ul)",
                {"\3k1"});

            // Negative side: supported -(2^53 - 1) / -2^53 and rounded -(2^53 + 1)
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == -9007199254740991l)",
                {"\3k1" + numSuffix(-rounded + 1.0)});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == -9007199254740992l)",
                {"\3k1" + numSuffix(-rounded)});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == -9007199254740993l)",
                {"\3k1"});
        });
    }

    Y_UNIT_TEST(JsonCombinations) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // No JSON_* in the filter - "no JSON_* functions found"
            ValidateError(db, R"(Key = 1ul)");
            ValidateError(db, R"((Data = "a"u) OR (Data = "b"u))");

            // JSON_* only (tokens in explain are successful)
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1'))", {"\3k1"});

            // JSON_* together with a non-JSON column
            ValidateTokens(db, R"((JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u)))", {"\3k1"});
            ValidateError(db, R"((JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u)))");

            // JSONPath that cannot be parsed for index extraction
            ValidateError(db, R"(JSON_EXISTS(Text, '$.[0'))", "Invalid json path");

            // OR: an indexable branch and a non-indexable branch (JSON_VALUE in an arithmetic expression)
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR ((JSON_VALUE(Text, '$.k2' RETURNING Int32) + 10) > 11))");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND ((JSON_VALUE(Text, '$.k2' RETURNING Int32) + 10) > 11))", {"\3k1"});

            ValidateError(db, R"(((JSON_VALUE(Text, '$.k2' RETURNING Int32) + 10) > 11) OR JSON_EXISTS(Text, '$.k1'))");
            ValidateTokens(db, R"(((JSON_VALUE(Text, '$.k2' RETURNING Int32) + 10) > 11) AND JSON_EXISTS(Text, '$.k1'))", {"\3k1"});

            // AND: indexable JSON with unsupported JSON (RETURNING Date) - collect error
            ValidateTokens(db, R"((JSON_EXISTS(Text, '$.k1') AND (JSON_VALUE(Text, '$.k1' RETURNING Date) == Date("2021-01-01"))))", {"\3k1"});
            ValidateError(db, R"((JSON_EXISTS(Text, '$.k1') OR (JSON_VALUE(Text, '$.k1' RETURNING Date) == Date("2021-01-01"))))");

            // OR: one disjunct is indexable, the other is not
            ValidateError(db, R"((JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR ((JSON_VALUE(Text, '$.k1' RETURNING Int32) + 10) > 11)))");
            ValidateTokens(db, R"((JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 AND ((JSON_VALUE(Text, '$.k1' RETURNING Int32) + 10) > 11)))",
                {"\3k1" + numSuffix(1)}, "and");

            // AND: several indexable JSON_* in one filter
            ValidateTokens(db, R"((JSON_EXISTS(Text, '$.k1') AND (JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1)))",
                {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"((JSON_EXISTS(Text, '$.a') AND (JSON_VALUE(Text, '$.b' RETURNING Int32) == 0)))",
                {"\2a", "\2b" + numSuffix(0)});

            // OR: only JSON_*; three-way
            ValidateTokens(db,
                R"((JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1) OR (JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2) OR (JSON_VALUE(Text, '$.k3' RETURNING Int32) == 3))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "or");

            // OR: a non-JSON disjunct
            ValidateError(db, R"((JSON_EXISTS(Text, '$.k1') OR (JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1) OR (Key = 1ul)))");
            ValidateError(db, R"((JSON_EXISTS(Text, '$.k1') AND (JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1) OR (Key = 1ul)))");
            ValidateTokens(db, R"((JSON_EXISTS(Text, '$.k1') OR (JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1) AND (Key = 1ul)))", {"\3k1"}, "or");
            ValidateTokens(db, R"((JSON_EXISTS(Text, '$.k1') AND (JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1) AND (Key = 1ul)))", {"\3k1" + numSuffix(1)}, "and");

            // (indexable subexpression) OR (indexable) - "or" mode for tokens
            ValidateTokens(db,
                R"(((JSON_EXISTS(Text, '$.a') AND (JSON_VALUE(Text, '$.b' RETURNING Int32) == 0)) OR (JSON_EXISTS(Text, '$.c'))))",
                {"\2a", "\2b" + numSuffix(0), "\2c"}, "or");

            // AND: three indexable JSON_* in one filter
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.a') AND (JSON_VALUE(Text, '$.b' RETURNING Int32) == 0) AND (JSON_VALUE(Text, '$.c' RETURNING Utf8) == "z"u)))",
                {"\2a", "\2b" + numSuffix(0), "\2c" + strSuffix("z")}, "and");

            // AND with JSON_QUERY in the same predicate
            ValidateError(db, R"((JSON_EXISTS(Text, '$.k1') AND (JSON_QUERY(Text, '$.k2') IS NOT NULL)))");

            // (OR of indexable predicates) AND (non-indexable JSON predicate) - OR lookup + post-filter
            // Case 1: non-indexable is arithmetic JSON_VALUE
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')) AND ((JSON_VALUE(Text, '$.x' RETURNING Int32) + 1) > 0))",
                {"\3k1", "\3k2"}, "or");
            // Case 2: symmetric (non-indexable first)
            ValidateTokens(db,
                R"(((JSON_VALUE(Text, '$.x' RETURNING Int32) + 1) > 0) AND (JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')))",
                {"\3k1", "\3k2"}, "or");
            // Case 3: non-indexable is RETURNING Date (treated as post-filter)
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')) AND (JSON_VALUE(Text, '$.k3' RETURNING Date) == Date("2021-01-01")))",
                {"\3k1", "\3k2"}, "or");
            // Case 4: OR branch contains (indexable AND non-indexable)
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') OR (JSON_EXISTS(Text, '$.k2') AND ((JSON_VALUE(Text, '$.x' RETURNING Int32) + 1) > 0)))",
                {"\3k1", "\3k2"}, "or");
            // Case 5: symmetric (non-indexable-AND first)
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') AND ((JSON_VALUE(Text, '$.x' RETURNING Int32) + 1) > 0)) OR JSON_EXISTS(Text, '$.k2'))",
                {"\3k1", "\3k2"}, "or");
            // Case 6: OR of two indexable JV comparisons AND a non-indexable RETURNING Date JV
            ValidateTokens(db,
                R"((JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2) AND (JSON_VALUE(Text, '$.k3' RETURNING Date) == Date("2021-01-01")))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2)}, "or");

            // Non-indexable RETURNING types now caught by whitelist (Date, Datetime, Timestamp already tested above)
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Date) > Date("2021-01-01"))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Datetime) > Datetime("2021-01-01T00:00:00Z"))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Timestamp) > Timestamp("2021-01-01T00:00:00Z"))");

            // Nested JSON_* functions as source - specific error message
            ValidateError(db, R"(JSON_VALUE(JSON_QUERY(Text, '$.k1'), '$.k2') == "1")");

            // JSON_EXISTS TRUE ON ERROR + JE -> JE tokens
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            // Symmetric: error operand on right -> JE tokens
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))",
                {"\2a"});

            // JSON_VALUE DEFAULT 12 ON EMPTY + JE -> JE tokens
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON EMPTY) > 10 AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a') AND JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON EMPTY) > 10)",
                {"\2a"});

            // JSON_VALUE DEFAULT 12 ON ERROR + JE -> JE tokens
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON ERROR) > 10 AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a') AND JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON ERROR) > 10)",
                {"\2a"});

            // Both ON EMPTY and ON ERROR with non-NULL DEFAULT
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON EMPTY DEFAULT 12 ON ERROR) > 10
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND (JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON EMPTY DEFAULT 12 ON ERROR) > 10))",
                {"\2a"});

            // Multiple non-indexable JV forms AND'd with a single JPRED
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON ERROR) > 10
                   AND JSON_EXISTS(Text, '$.k2' TRUE ON ERROR)
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON ERROR) > 10
                   AND JSON_EXISTS(Text, '$.a')
                   AND JSON_EXISTS(Text, '$.k2' TRUE ON ERROR))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                    AND JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON ERROR) > 10
                   AND JSON_EXISTS(Text, '$.k2' TRUE ON ERROR))",
                {"\2a"});

            // JV(... RETURNING Bool) == literal + JE -> JE tokens
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == false AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a') AND JSON_VALUE(Text, '$.k1' RETURNING Bool) == false)",
                {"\2a"});

            // Range comparison on Bool + JE
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) > true AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a') AND JSON_VALUE(Text, '$.k1' RETURNING Bool) > true)",
                {"\2a"});

            // JV(Bool) compared with another JV(Bool) + JE
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == JSON_VALUE(Text, '$.k2' RETURNING Bool)
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND JSON_VALUE(Text, '$.k1' RETURNING Bool) == JSON_VALUE(Text, '$.k2' RETURNING Bool))",
                {"\2a"});

            // both error-producing returning types should behave identically inside AND
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == true
                   AND JSON_VALUE(Text, '$.k2' RETURNING Date) == Date("2021-01-01")
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\3k1" + trueSuffix, "\2a"});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k2' RETURNING Date) == Date("2021-01-01")
                   AND JSON_VALUE(Text, '$.k1' RETURNING Bool) == true
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\3k1" + trueSuffix, "\2a"});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k2' RETURNING Date) == Date("2021-01-01")
                   AND JSON_EXISTS(Text, '$.a')
                   AND JSON_VALUE(Text, '$.k1' RETURNING Bool) == true)",
                {"\3k1" + trueSuffix, "\2a"});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == true
                   AND JSON_EXISTS(Text, '$.a')
                   AND JSON_VALUE(Text, '$.k2' RETURNING Date) == Date("2021-01-01"))",
                {"\3k1" + trueSuffix, "\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND JSON_VALUE(Text, '$.k1' RETURNING Bool) == true
                   AND JSON_VALUE(Text, '$.k2' RETURNING Date) == Date("2021-01-01"))",
                {"\3k1" + trueSuffix, "\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND JSON_VALUE(Text, '$.k2' RETURNING Date) == Date("2021-01-01")
                   AND JSON_VALUE(Text, '$.k1' RETURNING Bool) == true)",
                {"\3k1" + trueSuffix, "\2a"});

            ValidateTokens(db,
                R"((JSON_VALUE(Text, '$.k1' RETURNING Bool) == false)
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND (JSON_VALUE(Text, '$.k1' RETURNING Bool) == false))",
                {"\2a"});
            ValidateTokens(db,
                R"(NOT (JSON_VALUE(Text, '$.k1' RETURNING Bool) == false)
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a", "\3k1" + trueSuffix});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND NOT (JSON_VALUE(Text, '$.k1' RETURNING Bool) == false))",
                {"\2a", "\3k1" + trueSuffix});

            ValidateTokens(db,
                R"((JSON_VALUE(Text, '$.k1' RETURNING Bool) != false)
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a", "\3k1" + trueSuffix});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND (JSON_VALUE(Text, '$.k1' RETURNING Bool) != false))",
                {"\2a", "\3k1" + trueSuffix});
            ValidateTokens(db,
                R"(NOT (JSON_VALUE(Text, '$.k1' RETURNING Bool) != false)
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND NOT (JSON_VALUE(Text, '$.k1' RETURNING Bool) != false))",
                {"\2a"});

            // Same shape but with a comparison form that is supported alone -
            // proves that NOT does not change tokens regardless of inner form
            ValidateTokens(db,
                R"(NOT (JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1)
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND NOT (JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1))",
                {"\2a"});

            // Inner OR: JE OR (non-JSON column predicate)
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u))
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            // Inner OR: JE OR (arithmetic JV - nullopt branch)
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR ((JSON_VALUE(Text, '$.k2' RETURNING Int32) + 10) > 11))
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            // Inner OR: JE OR (RETURNING Bool comparison - error branch)
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR (JSON_VALUE(Text, '$.k2' RETURNING Bool) != true))
                   AND JSON_EXISTS(Text, '$.a'))",
                {"\2a"});
            // Symmetric: outer AND has the bad OR on the right
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a')
                   AND (JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u)))",
                {"\2a"});
            // Two valid JPREDs combined with the bad OR: tokens of both JPREDs
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u))
                   AND JSON_EXISTS(Text, '$.a')
                   AND (JSON_VALUE(Text, '$.b' RETURNING Int32) == 0))",
                {"\2a", "\2b" + numSuffix(0)});

            // Same forms alone 
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON ERROR) > 10)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == false)");

            // Same forms inside OR with another JPRED -> error
            ValidateError(db,
                R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.a'))");
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON ERROR) > 10
                   OR JSON_EXISTS(Text, '$.a'))");
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == false
                   OR JSON_EXISTS(Text, '$.a'))");

            // (JE OR PRED) at top level - error
            ValidateError(db,
                R"(JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u))");
            ValidateError(db,
                R"(JSON_EXISTS(Text, '$.k1')
                   OR (JSON_VALUE(Text, '$.k2' RETURNING Bool) >= true))");

            // AND of multiple error-producing forms with no JPRED at all -> error
            ValidateError(db,
                R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR)
                   AND JSON_VALUE(Text, '$.k2' RETURNING Bool) == false)");
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int DEFAULT 12 ON ERROR) > 10
                   AND JSON_VALUE(Text, '$.k2' RETURNING Bool) == false)");

            // Error AND non-recognised PRED (no JPRED either) -> error
            ValidateError(db,
                R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND Data = "d1"u)");
        });
    }

    Y_UNIT_TEST(AndOrCombinations) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            /*
                J - indexable predicate (let J = JSON_EXISTS(Text, '$.k1'))
                P - non-indexable predicate (let P = Data = "d1"u)
                PJ - non-indexable predicate with JSON_* (let PJ = JSON_EXISTS(Text, '$.k1' TRUE ON ERROR)))
            */

            // J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1'))", {"\3k1"}, "and");
            // P -> ERROR
            ValidateError(db, R"((Data = "d1"u))");
            // PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");

            // AND rule: at least one of the sides must be indexable
            // OR rule: all sides must be indexable

            // J AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "and");
            // J AND P -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u))", {"\3k1"}, "and");
            // P AND J -> OK
            ValidateTokens(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1'))", {"\3k1"}, "and");
            // J AND PJ -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))", {"\3k1"}, "and");
            // PJ AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1'))", {"\3k1"}, "and");
            // P AND P -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND (Data = "d1"u))");
            // P AND PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ AND P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u))");
            // PJ AND PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");

            // J OR J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "or");
            // J OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u))");
            // P OR J -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1'))");
            // J OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ OR J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1'))");
            // P OR P -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR (Data = "d1"u))");
            // P OR PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u))");
            // PJ OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");

            // J AND J AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))", {"\3k1", "\3k2", "\3k3"}, "and");
            // J AND J AND P -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND (Data = "d1"u))", {"\3k1", "\3k2"}, "and");
            // J AND J AND PJ -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))", {"\3k1", "\3k2"}, "and");
            // J AND P AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u) AND JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "and");
            // J AND P AND P -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u) AND (Data = "d1"u))", {"\3k1"}, "and");
            // J AND P AND PJ -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))", {"\3k1"}, "and");
            // J AND PJ AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "and");
            // J AND PJ AND P -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u))", {"\3k1"}, "and");
            // J AND PJ AND PJ -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))", {"\3k1"}, "and");
            // P AND J AND J -> OK
            ValidateTokens(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "and");
            // P AND J AND P -> OK
            ValidateTokens(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u))", {"\3k1"}, "and");
            // P AND J AND PJ -> OK
            ValidateTokens(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))", {"\3k1"}, "and");
            // P AND P AND J -> OK
            ValidateTokens(db, R"((Data = "d1"u) AND (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1'))", {"\3k1"}, "and");
            // P AND P AND P -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND (Data = "d1"u) AND (Data = "d1"u))");
            // P AND P AND PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P AND PJ AND J -> OK
            ValidateTokens(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1'))", {"\3k1"}, "and");
            // P AND PJ AND P -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u))");
            // P AND PJ AND PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ AND J AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "and");
            // PJ AND J AND P -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u))", {"\3k1"}, "and");
            // PJ AND J AND PJ -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))", {"\3k1"}, "and");
            // PJ AND P AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1'))", {"\3k1"}, "and");
            // PJ AND P AND P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u) AND (Data = "d1"u))");
            // PJ AND P AND PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ AND PJ AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1'))", {"\3k1"}, "and");
            // PJ AND PJ AND P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u))");
            // PJ AND PJ AND PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // J AND J OR J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3'))", {"\3k1", "\3k2", "\3k3"}, "or");
            // J AND J OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') OR (Data = "d1"u))");
            // J AND J OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // J AND P OR J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u) OR JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "or");
            // J AND P OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u) OR (Data = "d1"u))");
            // J AND P OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // J AND PJ OR J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "or");
            // J AND PJ OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u))");
            // J AND PJ OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P AND J OR J -> OK
            ValidateTokens(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "or");
            // P AND J OR P -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u))");
            // P AND J OR PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P AND P OR J -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1'))");
            // P AND P OR P -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND (Data = "d1"u) OR (Data = "d1"u))");
            // P AND P OR PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P AND PJ OR J -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1'))");
            // P AND PJ OR P -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u))");
            // P AND PJ OR PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ AND J OR J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "or");
            // PJ AND J OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u))");
            // PJ AND J OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ AND P OR J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1'))");
            // PJ AND P OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u) OR (Data = "d1"u))");
            // PJ AND P OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ AND PJ OR J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1'))");
            // PJ AND PJ OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u))");
            // PJ AND PJ OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // J OR J AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))", {"\3k1", "\3k2", "\3k3"}, "or");
            // J OR J AND P -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') AND (Data = "d1"u))", {"\3k1", "\3k2"}, "or");
            // J OR J AND PJ -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))", {"\3k1", "\3k2"}, "or");
            // J OR P AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u) AND JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "or");
            // J OR P AND P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u) AND (Data = "d1"u))");
            // J OR P AND PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // J OR PJ AND J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k2'))", {"\3k1", "\3k2"}, "or");
            // J OR PJ AND P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u))");
            // J OR PJ AND PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P OR J AND J -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))");
            // P OR J AND P -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u))");
            // P OR J AND PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P OR P AND J -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1'))");
            // P OR P AND P -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR (Data = "d1"u) AND (Data = "d1"u))");
            // P OR P AND PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P OR PJ AND J -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1'))");
            // P OR PJ AND P -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u))");
            // P OR PJ AND PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ OR J AND J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))");
            // PJ OR J AND P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1') AND (Data = "d1"u))");
            // PJ OR J AND PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ OR P AND J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1'))");
            // PJ OR P AND P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u) AND (Data = "d1"u))");
            // PJ OR P AND PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ OR PJ AND J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1'))");
            // PJ OR PJ AND P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND (Data = "d1"u))");
            // PJ OR PJ AND PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) AND JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // J OR J OR J -> OK
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3'))", {"\3k1", "\3k2", "\3k3"}, "or");
            // J OR J OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR (Data = "d1"u))");
            // J OR J OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // J OR P OR J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u) OR JSON_EXISTS(Text, '$.k2'))");
            // J OR P OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u) OR (Data = "d1"u))");
            // J OR P OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // J OR PJ OR J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k2'))");
            // J OR PJ OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u))");
            // J OR PJ OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P OR J OR J -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))");
            // P OR J OR P -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u))");
            // P OR J OR PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P OR P OR J -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1'))");
            // P OR P OR P -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR (Data = "d1"u) OR (Data = "d1"u))");
            // P OR P OR PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // P OR PJ OR J -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1'))");
            // P OR PJ OR P -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u))");
            // P OR PJ OR PJ -> ERROR
            ValidateError(db, R"((Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ OR J OR J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))");
            // PJ OR J OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1') OR (Data = "d1"u))");
            // PJ OR J OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ OR P OR J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1'))");
            // PJ OR P OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u) OR (Data = "d1"u))");
            // PJ OR P OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
            // PJ OR PJ OR J -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1'))");
            // PJ OR PJ OR P -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR (Data = "d1"u))");
            // PJ OR PJ OR PJ -> ERROR
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR) OR JSON_EXISTS(Text, '$.k1' TRUE ON ERROR))");
        });
    }

    Y_UNIT_TEST(PassingVariables) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // JSON_VALUE: variable on right side, equality, all scalar types
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING 1 AS var RETURNING Bool))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING -1 AS var RETURNING Bool))", {"\3k1" + numSuffix(-1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING 1.0 AS var RETURNING Bool))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING -1.0 AS var RETURNING Bool))", {"\3k1" + numSuffix(-1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING "123"u AS var RETURNING Bool))", {"\3k1" + strSuffix("123")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING true AS var RETURNING Bool))", {"\3k1" + trueSuffix});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING false AS var RETURNING Bool))", {"\3k1" + falseSuffix});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING NULL AS var RETURNING Bool))", {"\3k1" + nullSuffix});

            // JSON_VALUE: variable on left side
            ValidateTokens(db, R"(JSON_VALUE(Text, '$var == $.k1' PASSING 1 AS var RETURNING Bool))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$var == $.k1' PASSING "hello"u AS var RETURNING Bool))", {"\3k1" + strSuffix("hello")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$var == $.k1' PASSING true AS var RETURNING Bool))", {"\3k1" + trueSuffix});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$var == $.k1' PASSING NULL AS var RETURNING Bool))", {"\3k1" + nullSuffix});

            // JSON_VALUE: non-equality operators with variable -> path only (literal dropped)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 != $var' PASSING 5 AS var RETURNING Bool))", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 < $var' PASSING 5 AS var RETURNING Bool))", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 <= $var' PASSING 5 AS var RETURNING Bool))", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 > $var' PASSING 0 AS var RETURNING Bool))", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 >= $var' PASSING 0 AS var RETURNING Bool))", {"\3k1"});

            // JSON_VALUE: multiple variables in AND
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '($.k1 == $v1) && ($.k2 == $v2)' PASSING "x"u AS v1, 1 AS v2 RETURNING Bool))",
                {"\3k1" + strSuffix("x"), "\3k2" + numSuffix(1)}, "and");

            // JSON_VALUE: multiple variables in OR
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '($.k1 == $v1) || ($.k2 == $v2)' PASSING "x"u AS v1, 1 AS v2 RETURNING Bool))",
                {"\3k1" + strSuffix("x"), "\3k2" + numSuffix(1)}, "or");

            // JSON_VALUE: mixed variable and literal
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '($.k1 == $var) && ($.k2 == 42)' PASSING "x"u AS var RETURNING Bool))",
                {"\3k1" + strSuffix("x"), "\3k2" + numSuffix(42)}, "and");

            // JSON_VALUE: non-literal PASSING types -> error
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING Json('123') AS var RETURNING Bool))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING CurrentUtcTimestamp() AS var RETURNING Bool))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING Date("2021-01-01") AS var RETURNING Bool))");

            // JSON_EXISTS: filter with variable, equality, all scalar types
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $var)' PASSING 1 AS var))", {"\3k1\3k2" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $var)' PASSING "hello"u AS var))", {"\3k1\3k2" + strSuffix("hello")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $var)' PASSING true AS var))", {"\3k1\3k2" + trueSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $var)' PASSING false AS var))", {"\3k1\3k2" + falseSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $var)' PASSING NULL AS var))", {"\3k1\3k2" + nullSuffix});

            // JSON_EXISTS: variable on left side in filter equality
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? ($var == @.k2)' PASSING "val"u AS var))", {"\3k1\3k2" + strSuffix("val")});

            // JSON_EXISTS: non-equality filter operators with variable -> path only
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 < $var)' PASSING 10 AS var))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 != $var)' PASSING "x"u AS var))", {"\3k1\3k2"});

            // JSON_EXISTS: multiple variables in filter AND
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v1 && @.k3 == $v2)' PASSING "x"u AS v1, 1 AS v2))",
                {"\3k1\3k2" + strSuffix("x"), "\3k1\3k3" + numSuffix(1)}, "and");

            // JSON_EXISTS: multiple variables in filter OR
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ((@.k2 == $v1) || (@.k2 == $v2))' PASSING "a"u AS v1, "b"u AS v2))",
                {"\3k1\3k2" + strSuffix("a"), "\3k1\3k2" + strSuffix("b")}, "or");

            // JSON_EXISTS: non-literal PASSING types -> error
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $var)' PASSING Json('123') AS var))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $var)' PASSING CurrentUtcTimestamp() AS var))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $var)' PASSING Date("2021-01-01") AS var))");

            // Variable not referenced
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' RETURNING Bool))", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == $var' PASSING 10 AS var2 RETURNING Bool))", {"\3k1"});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1 == $var && $.k2 == $var2' PASSING 10 AS var2 RETURNING Bool))",
                {"\3k1", "\3k2" + numSuffix(10)}, "and");
        });
    }

    Y_UNIT_TEST(Parameters) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            auto utfParam = [](const std::string& name) {
                return TParamsBuilder().AddParam("$" + name).Utf8("v").Build().Build();
            };
            auto intParam = [](const std::string& name, i32 value = 1) {
                return TParamsBuilder().AddParam("$" + name).Int32(value).Build().Build();
            };

            // External param on rhs: JSON_VALUE(...) == $param

            // Basic path expansion
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}}, utfParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\4key", "$param"}}, utfParam("param"));

            // Deep member access
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.a.b' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\2a\2b", "$param"}}, utfParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.a.b.c' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\2a\2b\2c", "$param"}}, utfParam("param"));

            // Quoted key
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.aba."caba"' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\4aba\5caba", "$param"}}, utfParam("param"));

            // Reversed operand order
            ValidateTokens(db, R"($param == JSON_VALUE(Text, '$.k1' RETURNING Utf8))",
                {NJsonIndex::TToken{"\3k1", "$param"}}, utfParam("param"));
            ValidateTokens(db, R"($param == JSON_VALUE(Text, '$.a.b.c' RETURNING Utf8))",
                {NJsonIndex::TToken{"\2a\2b\2c", "$param"}}, utfParam("param"));

            // Different RETURNING types
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}}, intParam("param"));

            // Non-equality operators drop the param suffix (path only)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) != $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) < $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) <= $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) > $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) >= $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));

            // Multiple external params AND
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p1 AND JSON_VALUE(Text, '$.k2' RETURNING Utf8) == $p2)",
                {NJsonIndex::TToken{"\3k1", "$p1"}, NJsonIndex::TToken{"\3k2", "$p2"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .Build(),
                "and");

            // Multiple external params OR
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p1 OR JSON_VALUE(Text, '$.k2' RETURNING Utf8) == $p2)",
                {NJsonIndex::TToken{"\3k1", "$p1"}, NJsonIndex::TToken{"\3k2", "$p2"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .Build(),
                "or");

            // Mixed param and literal
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p1 AND JSON_VALUE(Text, '$.k2' RETURNING Utf8) == "x"u)",
                {NJsonIndex::TToken{"\3k1", "$p1"}, NJsonIndex::TToken{"\3k2" + strSuffix("x"), ""}},
                TParamsBuilder().AddParam("$p1").Utf8("a").Build().Build(),
                "and");

            // External param via PASSING: JSON_EXISTS(Text, '... $v ...' PASSING $param AS v)

            // Basic path expansion via PASSING
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", "$param"}}, utfParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.key ? (@ == $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\4key", "$param"}}, utfParam("param"));

            // Deep filter path
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.a.b ? (@.c == $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\2a\2b\2c", "$param"}}, utfParam("param"));

            // Reversed order inside filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ($v == @.k2)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", "$param"}}, utfParam("param"));

            // JSON_VALUE with PASSING param
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1 == $v' PASSING $param AS v RETURNING Bool))",
                {NJsonIndex::TToken{"\3k1", "$param"}}, utfParam("param"));
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$v == $.k1' PASSING $param AS v RETURNING Bool))",
                {NJsonIndex::TToken{"\3k1", "$param"}}, utfParam("param"));
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.a.b.c == $v' PASSING $param AS v RETURNING Bool))",
                {NJsonIndex::TToken{"\2a\2b\2c", "$param"}}, utfParam("param"));

            // Non-equality in filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 < $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 != $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));

            // Multiple PASSING params AND in filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v1 && @.k3 == $v2)' PASSING $p1 AS v1, $p2 AS v2))",
                {NJsonIndex::TToken{"\3k1\3k2", "$p1"}, NJsonIndex::TToken{"\3k1\3k3", "$p2"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .Build(),
                "and");

            // Multiple PASSING params OR in filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ((@.k2 == $v1) || (@.k2 == $v2))' PASSING $p1 AS v1, $p2 AS v2))",
                {NJsonIndex::TToken{"\3k1\3k2", "$p1"}, NJsonIndex::TToken{"\3k1\3k2", "$p2"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .Build(),
                "or");
        });
    }

    Y_UNIT_TEST(ParametersTokens) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            auto utfParam = [](const std::string& name) {
                return TParamsBuilder().AddParam("$" + name).Utf8("v").Build().Build();
            };
            auto intParam = [](const std::string& name, i32 value = 1) {
                return TParamsBuilder().AddParam("$" + name).Int32(value).Build().Build();
            };
            auto dblParam = [](const std::string& name) {
                return TParamsBuilder().AddParam("$" + name).Double(1.0).Build().Build();
            };

            // Array subscripts don't stop param collection
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key[0]' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\4key", "$param"}}, utfParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.a.b[0].c' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\2a\2b\2c", "$param"}}, utfParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[*]' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}}, utfParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[last]' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}}, utfParam("param"));

            // Wildcard member access stops param collection
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.*' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, utfParam("param"));

            // Top-level wildcard member access
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.*' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"", ""}}, utfParam("param"));

            // Methods stop param collection
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.size()' RETURNING Int32) == $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.type()' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, utfParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.abs()' RETURNING Double) == $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, dblParam("param"));

            // Unary arithmetic stops param collection
            ValidateTokens(db, R"(JSON_VALUE(Text, '-$.k1' RETURNING Double) == $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, dblParam("param"));
            ValidateTokens(db, R"(JSON_VALUE(Text, '+$.k1' RETURNING Double) == $param)",
                {NJsonIndex::TToken{"\3k1", ""}}, dblParam("param"));

            // Binary arithmetic of two paths
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 + $.k2' RETURNING Double) == $param)",
                {NJsonIndex::TToken{"\3k1", ""}, NJsonIndex::TToken{"\3k2", ""}}, dblParam("param"), "and");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 - $.k2' RETURNING Double) == $param)",
                {NJsonIndex::TToken{"\3k1", ""}, NJsonIndex::TToken{"\3k2", ""}}, dblParam("param"), "and");

            // Context object as path
            ValidateTokens(db, R"(JSON_VALUE(Text, '$' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"", "$param"}}, utfParam("param"));

            // Empty key name
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.""' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\1", "$param"}}, utfParam("param"));

            // Reversed non-equality operators (param on the left)
            ValidateTokens(db, R"($param != JSON_VALUE(Text, '$.k1' RETURNING Int32))",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
            ValidateTokens(db, R"($param < JSON_VALUE(Text, '$.k1' RETURNING Int32))",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
            ValidateTokens(db, R"($param <= JSON_VALUE(Text, '$.k1' RETURNING Int32))",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
            ValidateTokens(db, R"($param > JSON_VALUE(Text, '$.k1' RETURNING Int32))",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
            ValidateTokens(db, R"($param >= JSON_VALUE(Text, '$.k1' RETURNING Int32))",
                {NJsonIndex::TToken{"\3k1", ""}}, intParam("param"));
        });
    }

    Y_UNIT_TEST(ReturningTypes) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int8) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").Int8(1).Build().Build());
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int16) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").Int16(1).Build().Build());
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").Int64(1).Build().Build());
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint8) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").Uint8(1).Build().Build());
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint16) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").Uint16(1).Build().Build());
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint32) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").Uint32(1).Build().Build());
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint64) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").Uint64(1).Build().Build());
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Float) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").Float(1.0f).Build().Build());
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Double) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").Double(1.0).Build().Build());
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING String) == $param)",
                {NJsonIndex::TToken{"\3k1", "$param"}},
                TParamsBuilder().AddParam("$param").String("v").Build().Build());
        });
    }

    Y_UNIT_TEST(ParametersCombinations) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            auto utfParam = [](const std::string& name) {
                return TParamsBuilder().AddParam("$" + name).Utf8("v").Build().Build();
            };

            // Mixed param and literal OR
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p1 OR JSON_VALUE(Text, '$.k2' RETURNING Utf8) == "x"u)",
                {NJsonIndex::TToken{"\3k1", "$p1"}, NJsonIndex::TToken{"\3k2" + strSuffix("x"), ""}},
                TParamsBuilder().AddParam("$p1").Utf8("a").Build().Build(),
                "or");

            // Three external params AND
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p1
                    AND JSON_VALUE(Text, '$.k2' RETURNING Utf8) == $p2
                    AND JSON_VALUE(Text, '$.k3' RETURNING Utf8) == $p3)",
                {NJsonIndex::TToken{"\3k1", "$p1"}, NJsonIndex::TToken{"\3k2", "$p2"}, NJsonIndex::TToken{"\3k3", "$p3"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .AddParam("$p3").Utf8("c").Build()
                    .Build(),
                "and");

            // Three external params OR
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p1
                    OR JSON_VALUE(Text, '$.k2' RETURNING Utf8) == $p2
                    OR JSON_VALUE(Text, '$.k3' RETURNING Utf8) == $p3)",
                {NJsonIndex::TToken{"\3k1", "$p1"}, NJsonIndex::TToken{"\3k2", "$p2"}, NJsonIndex::TToken{"\3k3", "$p3"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .AddParam("$p3").Utf8("c").Build()
                    .Build(),
                "or");

            // Mixed AND + OR (OR wins)
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p1
                    AND JSON_VALUE(Text, '$.k2' RETURNING Utf8) == $p2
                    OR  JSON_VALUE(Text, '$.k3' RETURNING Utf8) == $p3)",
                {NJsonIndex::TToken{"\3k1", "$p1"}, NJsonIndex::TToken{"\3k2", "$p2"}, NJsonIndex::TToken{"\3k3", "$p3"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .AddParam("$p3").Utf8("c").Build()
                    .Build(),
                "or");

            // Param AND non-indexable predicate
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $param AND Data == "d1"u)",
                {NJsonIndex::TToken{"\3k1", "$param"}}, utfParam("param"));

            // Two params for the same field AND (both kept)
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p1
                    AND JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p2)",
                {NJsonIndex::TToken{"\3k1", "$p1"}, NJsonIndex::TToken{"\3k1", "$p2"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .Build(),
                "and");

            // Two params for the same field OR (both kept)
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p1
                    OR JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p2)",
                {NJsonIndex::TToken{"\3k1", "$p1"}, NJsonIndex::TToken{"\3k1", "$p2"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .Build(),
                "or");

            // Same param in both AND branches
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $p
                    AND JSON_VALUE(Text, '$.k2' RETURNING Utf8) == $p)",
                {NJsonIndex::TToken{"\3k1", "$p"}, NJsonIndex::TToken{"\3k2", "$p"}},
                TParamsBuilder().AddParam("$p").Utf8("a").Build().Build(),
                "and");
        });
    }

    Y_UNIT_TEST(PassingParameters) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            auto utfParam = [](const std::string& name) {
                return TParamsBuilder().AddParam("$" + name).Utf8("v").Build().Build();
            };
            auto intParam = [](const std::string& name, i32 value = 1) {
                return TParamsBuilder().AddParam("$" + name).Int32(value).Build().Build();
            };

            // Array subscript in filter path doesn't stop collection
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2[0] == $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", "$param"}}, utfParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2[*] == $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", "$param"}}, utfParam("param"));

            // Filter on context object
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1", "$param"}}, utfParam("param"));

            // Wildcard in outer path before filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.* ? (@.k1 == $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"", ""}}, utfParam("param"));

            // Non-equality operators in filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 > $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 >= $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 <= $v)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ($v > @.k2)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ($v >= @.k2)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ($v < @.k2)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ($v <= @.k2)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ($v != @.k2)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, intParam("param"));

            // Three PASSING params AND in filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v1 && @.k3 == $v2 && @.k4 == $v3)'
                    PASSING $p1 AS v1, $p2 AS v2, $p3 AS v3))",
                {NJsonIndex::TToken{"\3k1\3k2", "$p1"}, NJsonIndex::TToken{"\3k1\3k3", "$p2"}, NJsonIndex::TToken{"\3k1\3k4", "$p3"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .AddParam("$p3").Utf8("c").Build()
                    .Build(),
                "and");

            // Three PASSING params OR in filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ((@.k2 == $v1) || (@.k3 == $v2) || (@.k4 == $v3))'
                    PASSING $p1 AS v1, $p2 AS v2, $p3 AS v3))",
                {NJsonIndex::TToken{"\3k1\3k2", "$p1"}, NJsonIndex::TToken{"\3k1\3k3", "$p2"}, NJsonIndex::TToken{"\3k1\3k4", "$p3"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .AddParam("$p3").Utf8("c").Build()
                    .Build(),
                "or");

            // Mixed AND + OR in filter (OR wins)
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v1 && @.k3 == $v2 || @.k4 == $v3)'
                    PASSING $p1 AS v1, $p2 AS v2, $p3 AS v3))",
                {NJsonIndex::TToken{"\3k1\3k2", "$p1"}, NJsonIndex::TToken{"\3k1\3k3", "$p2"}, NJsonIndex::TToken{"\3k1\3k4", "$p3"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .AddParam("$p3").Utf8("c").Build()
                    .Build(),
                "or");

            // PASSING param and literal AND in filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v && @.k3 == 42)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", "$param"}, NJsonIndex::TToken{"\3k1\3k3" + numSuffix(42), ""}},
                utfParam("param"),
                "and");

            // PASSING param and literal OR in filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ((@.k2 == $v) || (@.k3 == 42))' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", "$param"}, NJsonIndex::TToken{"\3k1\3k3" + numSuffix(42), ""}},
                utfParam("param"),
                "or");

            // Variable defined in PASSING but not used in filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 5)' PASSING $param AS v))",
                {NJsonIndex::TToken{"\3k1\3k2" + numSuffix(5), ""}}, utfParam("param"));

            // Two variables defined, only one used
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v1)' PASSING $p1 AS v1, $p2 AS v2))",
                {NJsonIndex::TToken{"\3k1\3k2", "$p1"}},
                TParamsBuilder()
                    .AddParam("$p1").Utf8("a").Build()
                    .AddParam("$p2").Utf8("b").Build()
                    .Build());

            // Variable missing from PASSING
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)'))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}}, TParamsBuilder().Build());
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING $p AS differentName))",
                {NJsonIndex::TToken{"\3k1\3k2", ""}},
                TParamsBuilder().AddParam("$p").Utf8("a").Build().Build());
        });
    }

    Y_UNIT_TEST(MixedParameters) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            auto utfParam = [](const std::string& name) {
                return TParamsBuilder().AddParam("$" + name).Utf8("v").Build().Build();
            };

            // AND of external param and PASSING param
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $pExt
                    AND JSON_EXISTS(Text, '$.k2 ? (@.k3 == $v)' PASSING $pPass AS v))",
                {NJsonIndex::TToken{"\3k1", "$pExt"}, NJsonIndex::TToken{"\3k2\3k3", "$pPass"}},
                TParamsBuilder()
                    .AddParam("$pExt").Utf8("a").Build()
                    .AddParam("$pPass").Utf8("b").Build()
                    .Build(),
                "and");

            // OR of external param and PASSING param
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $pExt
                    OR JSON_EXISTS(Text, '$.k2 ? (@.k3 == $v)' PASSING $pPass AS v))",
                {NJsonIndex::TToken{"\3k1", "$pExt"}, NJsonIndex::TToken{"\3k2\3k3", "$pPass"}},
                TParamsBuilder()
                    .AddParam("$pExt").Utf8("a").Build()
                    .AddParam("$pPass").Utf8("b").Build()
                    .Build(),
                "or");

            // External param + PASSING param + literal AND
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $pExt
                    AND JSON_EXISTS(Text, '$.k2 ? (@.k3 == $v)' PASSING $pPass AS v)
                    AND JSON_EXISTS(Text, '$.k4'))",
                {NJsonIndex::TToken{"\3k1", "$pExt"}, NJsonIndex::TToken{"\3k2\3k3", "$pPass"}, NJsonIndex::TToken{"\3k4", ""}},
                TParamsBuilder()
                    .AddParam("$pExt").Utf8("a").Build()
                    .AddParam("$pPass").Utf8("b").Build()
                    .Build(),
                "and");

            // External param + PASSING param + literal OR
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $pExt
                    OR JSON_EXISTS(Text, '$.k2 ? (@.k3 == $v)' PASSING $pPass AS v)
                    OR JSON_EXISTS(Text, '$.k4'))",
                {NJsonIndex::TToken{"\3k1", "$pExt"}, NJsonIndex::TToken{"\3k2\3k3", "$pPass"}, NJsonIndex::TToken{"\3k4", ""}},
                TParamsBuilder()
                    .AddParam("$pExt").Utf8("a").Build()
                    .AddParam("$pPass").Utf8("b").Build()
                    .Build(),
                "or");

            // Same param as both PASSING variable and external comparison
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k1 == $v)' PASSING $param AS v)
                    AND JSON_VALUE(Text, '$.k2' RETURNING Utf8) == $param)",
                {NJsonIndex::TToken{"\3k1\3k1", "$param"}, NJsonIndex::TToken{"\3k2", "$param"}},
                utfParam("param"),
                "and");
        });
    }

    Y_UNIT_TEST(ParameterErrors) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            auto utfParam = [](const std::string& name) {
                return TParamsBuilder().AddParam("$" + name).Utf8("v").Build().Build();
            };
            auto boolParam = [](const std::string& name) {
                return TParamsBuilder().AddParam("$" + name).Bool(true).Build().Build();
            };

            // NOT wrapping param
            ValidateError(db, R"(NOT (JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $param))",
                utfParam("param"));

            // Bool RETURNING
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == $param)",
                boolParam("param"),
                "Comparison JSON_VALUE with RETURNING Bool is not supported");

            // Date-family RETURNING
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Date) == $param)",
                TParamsBuilder().AddParam("$param").Date(TInstant::Now()).Build().Build(),
                "Date/time types in RETURNING clause are not supported");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Datetime) == $param)",
                TParamsBuilder().AddParam("$param").Datetime(TInstant::Now()).Build().Build(),
                "Date/time types in RETURNING clause are not supported");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Timestamp) == $param)",
                TParamsBuilder().AddParam("$param").Timestamp(TInstant::Now()).Build().Build(),
                "Date/time types in RETURNING clause are not supported");

            // OR with non-indexable predicate
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == $param OR Data == "d1"u)",
                utfParam("param"));

            // Variable bound to column reference
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1 == $v' PASSING Data AS v RETURNING Bool))");

            // JSON_EXISTS result compared to param
            ValidateError(db, R"(JSON_EXISTS(Text, '$.key') == $param)",
                boolParam("param"));

            // JSON_QUERY source
            ValidateError(db,
                R"(JSON_VALUE(JSON_QUERY(Text, '$.k1' WITHOUT ARRAY WRAPPER), '$.k2' RETURNING Utf8) == $param)",
                utfParam("param"));

            // IS NULL / IS NOT NULL on JSON_VALUE
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IS NULL)",
                utfParam("param"));
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IS NOT NULL)",
                utfParam("param"));
        });
    }

    Y_UNIT_TEST(JsonExistsPaths) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Context object - empty path token
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$'))", {""});

            // Empty key
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.""'))", {"\1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.""'))", {"\3k1\1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$."".k1'))", {"\1\3k1"});

            // Array access at the root
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[0]'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[3]'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[last]'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[1 to 3]'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[0, 3]'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[*]'))", {""});

            // Array access after a key - should not stop collection
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[0]'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[3]'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[last].k2'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[1 to 3]'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[0, 3]'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[*]'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[0 to last].k2'))", {"\3k1\3k2"});

            // Chains of array access
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[*][0]'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[0][*]'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[0][0][0]'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[*].k1'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[0].k1'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[last].k1'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[*].k2'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[0].k2.k3'))", {"\3k1\3k2\3k3"});

            // Wildcard member access stops collection
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.*'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.*'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.k2.*'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.*.k2'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.*.*'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.*.k1'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.*[0].k1'))", {""});

            // Methods stop collection
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.size()'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.type()'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.double()'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.ceiling()'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.floor()'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.abs()'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.keyvalue()'))", {"\3k1"});

            // Methods chained with member access after them
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.keyvalue().name'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.keyvalue().value'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.size().double()'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.abs().ceiling()'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.abs().floor().type()'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.keyvalue().value.size()'))", {"\3k1"});

            // Methods at the root
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.size()'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.type()'))", {""});

            // Quoted keys preserve content as-is (no nested splitting on dots)
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$."key with spaces"'))", {"\x10key with spaces"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$."key.with.dot"'))", {"\rkey.with.dot"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$."key.with.dot".sub'))", {"\rkey.with.dot\4sub"});

            // SQL-keyword names work as ordinary keys
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.to'))", {"\3to"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.last'))", {"\5last"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.exists'))", {"\7exists"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.size'))", {"\5size"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.type'))", {"\5type"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.flag'))", {"\5flag"});

            // lax / strict prefixes
            ValidateTokens(db, R"(JSON_EXISTS(Text, 'lax $.k1'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, 'strict $.k1'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, 'lax $.k1[*]'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, 'strict $.k1[*]'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, 'lax $.k1.*'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, 'strict $.k1.*'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, 'lax $ ? (@.k1 == 1)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, 'strict $ ? (@.k1 == 1)'))", {"\3k1" + numSuffix(1)});
        });
    }

    Y_UNIT_TEST(JsonValuePaths) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Array variants after a key, with literal RHS
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[0]' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[*]' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[last]' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[1 to 3]' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[0, 3]' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[0 to last]' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});

            // Array variants with numeric literal RHS
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[0]' RETURNING Int32) == 5)", {"\3k1" + numSuffix(5)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[last]' RETURNING Int32) == 5)", {"\3k1" + numSuffix(5)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[*]' RETURNING Int32) == 5)", {"\3k1" + numSuffix(5)});

            // Reversed operand order
            ValidateTokens(db, R"("x" == JSON_VALUE(Text, '$.k1[0]' RETURNING Utf8))", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(5 == JSON_VALUE(Text, '$.k1[*]' RETURNING Int32))", {"\3k1" + numSuffix(5)});

            // Chains of array access with trailing key
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[*][0]' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[0][*]' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$[0].k1' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$[*].k1' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$[last].k1' RETURNING Utf8) == "x")", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[0].k2' RETURNING Utf8) == "x")", {"\3k1\3k2" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[*].k2.k3' RETURNING Utf8) == "x")", {"\3k1\3k2\3k3" + strSuffix("x")});

            // keyvalue() and chains with member access after keyvalue()
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.keyvalue().name' RETURNING Utf8) == "x")", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.keyvalue().value' RETURNING Utf8) == "x")", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.keyvalue().value.size()' RETURNING Int32) == 1)", {"\3k1"});

            // Filter inside the path - filter stops collection, the outer literal is dropped
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 ? (@.k2 == 2)' RETURNING Int32) == 5)", {"\3k1\3k2" + numSuffix(2)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$ ? (@.k1 == 1)' RETURNING Utf8) == "x")", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 ? (@.k2 == "y")' RETURNING Utf8) == "x")", {"\3k1\3k2" + strSuffix("y")});

            // Context object as path with literal
            ValidateTokens(db, R"(JSON_VALUE(Text, '$' RETURNING Utf8) == "abc")", {"" + strSuffix("abc")});
            ValidateTokens(db, R"("abc" == JSON_VALUE(Text, '$' RETURNING Utf8))", {"" + strSuffix("abc")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$' RETURNING Int32) == 7)", {"" + numSuffix(7)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$' RETURNING Bool) == true)", {"" + trueSuffix});

            // Empty key as path with literal
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.""' RETURNING Utf8) == "abc")", {"\1" + strSuffix("abc")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.""' RETURNING Int32) == 7)", {"\1" + numSuffix(7)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$."".k1' RETURNING Utf8) == "v")", {"\1\3k1" + strSuffix("v")});
        });
    }

    Y_UNIT_TEST(JsonExistsFilters) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // starts with
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 starts with "ab")'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 starts with "abc")'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2.k3 starts with "x")'))", {"\3k1\3k2\3k3"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ starts with "x")'))", {"\3k1"});

            // like_regex
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 like_regex "[a-z]+")'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 like_regex "[a-z]+")'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 like_regex "p" flag "i")'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ like_regex "x")'))", {"\3k1"});

            // exists(@.path) inside a filter
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (exists(@.k1))'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (exists(@.k2))'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (exists(@.k2.k3))'))", {"\3k1\3k2\3k3"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (exists(@))'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (exists(@.k2[0]))'))", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (exists(@.k2[*]))'))", {"\3k1\3k2"});

            // unary not and comparison - error, predicate inside predicate
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 == 1))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (!(@.k2 == 1))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 != 1))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 > 1))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 < 1))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 >= 1))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 <= 1))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 == 1 && @.k2 == 2))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 == 1 || @.k2 == 2))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 starts with "x"))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (!(@.k1 like_regex "x"))'))");

            // (...) is unknown - error, predicate inside predicate
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? ((@.k1 == 1) is unknown)'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? ((@.k2 == 1) is unknown)'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? ((@.k1 != 1) is unknown)'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? ((@.k1 starts with "x") is unknown)'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? ((@.k1 like_regex "x") is unknown)'))");

            // unary not and exists(@.k1) - error, predicate-in-predicate
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (! exists(@.k1))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (! exists(@.k2))'))");

            // exists(...) cannot wrap another predicate either
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (exists(@.k1 == 1))'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$ ? (exists(@.k1 starts with "x"))'))");

            // Filter-predicate combined with the outer JE(...) == TRUE rewrite
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 starts with "x")') == true)", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (exists(@.k2))') == true)", {"\3k1\3k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 1)') == true)", {"\3k1\3k2" + numSuffix(1)});

            // Filter-predicate combined with AND/OR
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 starts with "a")') AND JSON_EXISTS(Text, '$.k3 ? (exists(@.k4))'))",
                {"\3k1\3k2", "\3k3\3k4"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 starts with "a")') OR JSON_EXISTS(Text, '$.k3 ? (@.k4 == 1)'))",
                {"\3k1\3k2", "\3k3\3k4" + numSuffix(1)}, "or");
        });
    }

    Y_UNIT_TEST(JsonExistsFilterPaths) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Deep paths inside filter with literal-equality
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2.k3 == 1)'))", {"\3k1\3k2\3k3" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2.k3.k4 == "x")'))", {"\3k1\3k2\3k3\3k4" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.k2.k3.k4 == "x")'))", {"\3k1\3k2\3k3\3k4" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.k2 ? (@.k3.k4 == null)'))", {"\3k1\3k2\3k3\3k4" + nullSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.k2 ? (@.k3.k4 == true)'))", {"\3k1\3k2\3k3\3k4" + trueSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.k2 ? (@.k3.k4 == false)'))", {"\3k1\3k2\3k3\3k4" + falseSuffix});
            // Reversed-order literal/path
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (1 == @.k2.k3)'))", {"\3k1\3k2\3k3" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? ("x" == @.k2.k3.k4)'))", {"\3k1\3k2\3k3\3k4" + strSuffix("x")});

            // Array access inside filter path
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1[0] == 1)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1[*] == 1)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1[last] == 1)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1[1 to 3] == 1)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1[0, 3] == 1)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2[0].k3 == "x")'))", {"\3k1\3k2\3k3" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2[*].k3 == "x")'))", {"\3k1\3k2\3k3" + strSuffix("x")});

            // @ as filter root with all literal types
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == 1)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == -1.5)'))", {"\3k1" + numSuffix(-1.5)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == "x")'))", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == null)'))", {"\3k1" + nullSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == true)'))", {"\3k1" + trueSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@ == false)'))", {"\3k1" + falseSuffix});
            // Reversed order
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (1 == @)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? ("x" == @)'))", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (true == @)'))", {"\3k1" + trueSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (null == @)'))", {"\3k1" + nullSuffix});

            // $ root filter with literal: empty path token plus suffix
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@ == 1)'))", {"" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@ == "x")'))", {"" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@ == null)'))", {"" + nullSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@ == true)'))", {"" + trueSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@ == false)'))", {"" + falseSuffix});

            // Empty key inside / before filter
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@."" == 1)'))", {"\1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@."" == null)'))", {"\1" + nullSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@."" == "x")'))", {"\1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$."" ? (@.k1 == 1)'))", {"\1\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@."" == 1)'))", {"\3k1\1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$."" ? (@."" == 1)'))", {"\1\1" + numSuffix(1)});

            // Outer array access before filter (does not stop collection)
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[0] ? (@.k2 == 1)'))", {"\3k1\3k2" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[*] ? (@.k2 == 1)'))", {"\3k1\3k2" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[last] ? (@.k2 == 1)'))", {"\3k1\3k2" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[0] ? (@.k1 == "x")'))", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[*] ? (@.k1 == "x")'))", {"\3k1" + strSuffix("x")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[last] ? (@.k1 == "x")'))", {"\3k1" + strSuffix("x")});

            // Outer wildcard / method before filter
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.* ? (@.k2 == 1)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.* ? (@.k1 == 1)'))", {""});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.size() ? (@ > 0)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.size() ? (@ == 3)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.keyvalue() ? (@.name == "k")'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.abs() ? (@ == 1)'))", {"\3k1"});
        });
    }

    Y_UNIT_TEST(JsonExistsNestedFilters) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Basic nested filter
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == 1)).k2 == 2)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == 1)).k2 == "x")'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == "a")).k2 == "b")'))", {"\3k1" + strSuffix("a")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == null)).k2 == 2)'))", {"\3k1" + nullSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == true)).k2 == false)'))", {"\3k1" + trueSuffix});

            // Outer prefix carried through to inner filter
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? ((@ ? (@.k1 == 10)).k1 == 10)'))", {"\3k1\3k1" + numSuffix(10)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.k2 ? ((@ ? (@.k3 == "x")).k4 == "y")'))", {"\3k1\3k2\3k3" + strSuffix("x")});

            // Outer array before nested filter
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$[*] ? ((@ ? (@.k1 == 1)).k2 == 2)'))", {"\3k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1[*] ? ((@ ? (@.k1 == 10)).k1 == 10)'))", {"\3k1\3k1" + numSuffix(10)});

            // Nested filter with AND/OR inside the inner predicate
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == 1 && @.k2 == 2)).k3 == 3)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2)}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == 1 || @.k2 == 2)).k3 == 3)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2)}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == "a" && @.k2 == "b")).k3 == "c")'))",
                {"\3k1" + strSuffix("a"), "\3k2" + strSuffix("b")}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == true || @.k2 == null)).k3 == 3)'))",
                {"\3k1" + trueSuffix, "\3k2" + nullSuffix}, "or");

            // Three-way AND/OR inside nested filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == 1 && @.k2 == 2 && @.k3 == 3)).k4 == 4)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == 1 || @.k2 == 2 || @.k3 == 3)).k4 == 4)'))",
                {"\3k1" + numSuffix(1), "\3k2" + numSuffix(2), "\3k3" + numSuffix(3)}, "or");

            // Double-nested filter
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? ((@ ? ((@ ? (@.k1 == 0)).k4 == true)).k5 == null)'))",
                {"\3k1" + numSuffix(0)});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ((@ ? ((@ ? (@.k1 == 10)).k1 == 10)).k1 > 0)'))",
                {"\3k1\3k1" + numSuffix(10)});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? ((@ ? ((@ ? (@.k1 <= 10)).k1 == 10)).k1 > 0)'))",
                {"\3k1\3k1"});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? ((@ ? ((@ ? (@.k1 == "a" && @.k2 == "b")).k3 == 0)).k4 == 1)'))",
                {"\3k1" + strSuffix("a"), "\3k2" + strSuffix("b")}, "and");

            // Nested filter combined with the outer JE == TRUE rewrite
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == 1)).k2 == 2)') == true)", {"\3k1" + numSuffix(1)});

            // Nested filter combined with AND/OR at SQL level
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == 1)).k2 == 2)')
                   AND JSON_EXISTS(Text, '$.k3'))",
                {"\3k1" + numSuffix(1), "\3k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 == 1)).k2 == 2)')
                   OR JSON_EXISTS(Text, '$.k3'))",
                {"\3k1" + numSuffix(1), "\3k3"}, "or");

            // Inner nested filter with non-equality operators
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 != 1)).k2 == 2)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 > 0)).k2 == 2)'))", {"\3k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@ ? (@.k1 < 0)).k2 == 2)'))", {"\3k1"});
        });
    }

    Y_UNIT_TEST(JsonPruningPrefixRelationships) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // OR pruning between distinct paths
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1.k2'))", {"\3k1"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.k2') OR JSON_EXISTS(Text, '$.k1'))", {"\3k1"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1.k2.k3'))", {"\3k1"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k1.k2') OR JSON_EXISTS(Text, '$.k1.k2.k3'))", {"\3k1"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.k2') OR JSON_EXISTS(Text, '$.k1.k2.k3'))", {"\3k1\3k2"}, "or");

            // AND pruning between distinct paths
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1.k2'))", {"\3k1\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.k2') AND JSON_EXISTS(Text, '$.k1'))", {"\3k1\3k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1.k2.k3'))", {"\3k1\3k2\3k3"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k1.k2') AND JSON_EXISTS(Text, '$.k1.k2.k3'))", {"\3k1\3k2\3k3"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.k2') AND JSON_EXISTS(Text, '$.k1.k2.k3'))", {"\3k1\3k2\3k3"}, "and");

            // OR pruning when one operand carries a literal-suffix
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" OR JSON_EXISTS(Text, '$.k1.k2'))", {"\3k1" + strSuffix("a"), "\3k1\3k2"}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR JSON_EXISTS(Text, '$.k1.k2'))", {"\3k1" + numSuffix(1), "\3k1\3k2"}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" AND JSON_EXISTS(Text, '$.k1.k2'))", {"\3k1" + strSuffix("a"), "\3k1\3k2"}, "and");

            // Distinct literal suffixes on the same path
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" AND JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "b")",
                {"\3k1" + strSuffix("a"), "\3k1" + strSuffix("b")}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" OR JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "b")",
                {"\3k1" + strSuffix("a"), "\3k1" + strSuffix("b")}, "or");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 AND JSON_VALUE(Text, '$.k1' RETURNING Int32) == 2)",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR JSON_VALUE(Text, '$.k1' RETURNING Int32) == 2)",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "or");

            // AND with a nested OR that contains a prefix-related path
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND (JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k1.k2')))",
                {"\3k1", "\3k2"}, "or");
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k1.k2')) AND JSON_EXISTS(Text, '$.k1'))",
                {"\3k1", "\3k2"}, "or");

            // OR pruning across same-jsonpath filter alternatives
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 || @.k1 == 2)'))", {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 && @.k1 == 2)'))", {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "and");

            // Prefix relationships inside a single jsonpath
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (exists(@.k2) || exists(@))'))", {"\3k1"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (exists(@.k2) && exists(@))'))", {"\3k1\3k2"}, "and");
        });
    }

    Y_UNIT_TEST(JsonValueBetweenAndIn) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // BETWEEN - expands to (<= a) AND (>= b)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) BETWEEN 1 AND 10)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) BETWEEN 1l AND 10l)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Double) BETWEEN 1.0 AND 10.0)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) BETWEEN "a" AND "z")", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) BETWEEN "a" AND "z")", {"\3k1"});
            // BETWEEN with a deeper path
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.k2' RETURNING Int32) BETWEEN 1 AND 10)", {"\3k1\3k2"});

            // NOT BETWEEN - expands to (< a) OR (> b)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) NOT BETWEEN 1 AND 10)", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) NOT BETWEEN "a" AND "z")", {"\3k1"});

            // NOT IN - negation
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) NOT IN ("a", "b", "c"))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) NOT IN (1, 2, 3))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) NOT IN ("a", "b"))");

            // IN
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN ("a", "b", "c"))",
                {"\3k1" + strSuffix("a"), "\3k1" + strSuffix("b"), "\3k1" + strSuffix("c")}, "or");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, 2, 3))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2), "\3k1" + numSuffix(3)}, "or");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN ("a", "b"))",
                {"\3k1" + strSuffix("a"), "\3k1" + strSuffix("b")}, "or");

            // IN with all supported scalars
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int8) IN (1t, 2t))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint8) IN (1ut, 2ut))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int16) IN (1s, 2s))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint16) IN (1us, 2us))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint32) IN (1u, 2u))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) IN (1l, 2l))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint64) IN (1ul, 2ul))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Float) IN (1.0f, 2.5f))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2.5)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Double) IN (1.0, -2.5))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(-2.5)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN ("x"s, "y"s))",
                {"\3k1" + strSuffix("x"), "\3k1" + strSuffix("y")}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.k2' RETURNING Int32) IN (7, 8))",
                {"\3k1\3k2" + numSuffix(7), "\3k1\3k2" + numSuffix(8)}, "or");

            // IN with Just (unwrapped transparently)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (42))",
                {"\3k1" + numSuffix(42)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (Just(5), 6))",
                {"\3k1" + numSuffix(5), "\3k1" + numSuffix(6)}, "or");

            // TODO: CAST type mismatch
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (CAST("7" AS Int32), 8))", {"\3k1" + strSuffix("7"), "\3k1" + numSuffix(8)}, "or");

            // IN with mixed types
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, 2u, 3l))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2), "\3k1" + numSuffix(3)}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN ("x", "y"u))",
                {"\3k1" + strSuffix("x"), "\3k1" + strSuffix("y")}, "or");

            // IN with AND/OR and other indexed predicates
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, 2, 3)
                   AND JSON_EXISTS(Text, '$.k2'))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2), "\3k1" + numSuffix(3), "\3k2"}, "or");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, 2)
                   OR JSON_EXISTS(Text, '$.k2'))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2), "\3k2"}, "or");
            ValidateTokens(db,
                R"((JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, 2))
                   OR (JSON_VALUE(Text, '$.k2' RETURNING Int32) IN (10, 20)))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2), "\3k2" + numSuffix(10), "\3k2" + numSuffix(20)}, "or");
            ValidateTokens(db,
                R"((JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, 2))
                   AND (JSON_VALUE(Text, '$.k2' RETURNING Int32) IN (10, 20)))",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2), "\3k2" + numSuffix(10), "\3k2" + numSuffix(20)}, "or");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, 2)
                   OR JSON_VALUE(Text, '$.k2' RETURNING Int32) == 5)",
                {"\3k1" + numSuffix(1), "\3k1" + numSuffix(2), "\3k2" + numSuffix(5)}, "or");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 3
                   AND JSON_VALUE(Text, '$.k2' RETURNING Int32) IN (7, 8))",
                {"\3k1" + numSuffix(3), "\3k2" + numSuffix(7), "\3k2" + numSuffix(8)}, "or");

            // RETURNING Bool with IN
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) IN (true, false))",
                "SQL IN with JSON_VALUE with RETURNING Bool is not supported");

            // NULL and nullable values in list
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, NULL))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (NULL, 2))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (NULL))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (NULL, NULL))");

            // Members in list
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN ("1"u, Data))", {"\3k1"}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN (Data, "2"u))", {"\3k1"}, "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN (Data))", {"\3k1"}, "and");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN (Data, Data || "data"u))", {"\3k1"}, "and");

            // Parameters in list
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN ($p))",
                {NJsonIndex::TToken{"\3k1", "$p"}},
                TParamsBuilder().AddParam("$p").Int32(1).Build().Build());
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, $p))",
                {NJsonIndex::TToken{"\3k1", "$p"}, NJsonIndex::TToken{"\3k1" + numSuffix(1)}},
                TParamsBuilder().AddParam("$p").Int32(2).Build().Build(), "or");

            // BETWEEN combined with other indexable predicates
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) BETWEEN 1 AND 10
                   AND JSON_EXISTS(Text, '$.k2'))",
                {"\3k1", "\3k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) BETWEEN 1 AND 10
                   AND JSON_VALUE(Text, '$.k2' RETURNING Int32) == 5)",
                {"\3k1", "\3k2" + numSuffix(5)}, "and");

            // NOT BETWEEN combined with other indexable predicate
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) NOT BETWEEN 1 AND 10
                   AND JSON_EXISTS(Text, '$.k2'))",
                {"\3k1", "\3k2"}, "and");
        });
    }

    Y_UNIT_TEST(JsonValueComparisons) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // = (single equals) is parsed as ==
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) = "abc")", {"\3k1" + strSuffix("abc")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) = 5)", {"\3k1" + numSuffix(5)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) = true)", {"\3k1" + trueSuffix});

            // <> as alias of !=, path-only
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) <> "abc")", {"\3k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) <> 5)", {"\3k1"});

            // CAST in the literal position - YQL collapses CAST of a literal
            // back to the typed literal, so the token carries the value suffix
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == CAST("10" AS Int32))", {"\3k1" + numSuffix(10)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == CAST(10 AS Utf8))", {"\3k1" + strSuffix("10")});
            // CAST of a non-literal column - not a TCoDataCtor on RHS, path-only
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == CAST(Data AS Utf8))", {"\3k1"});

            // Just(...) wrapper unwrapped
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == Just("abc"u))", {"\3k1" + strSuffix("abc")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == Just(5))", {"\3k1" + numSuffix(5)});

            // column reference on RHS - not a TCoDataCtor, path-only token
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == Data)", {"\3k1"});
            // Reversed order
            ValidateTokens(db, R"(Data == JSON_VALUE(Text, '$.k1' RETURNING Utf8))", {"\3k1"});

            // UDF on the JSON_VALUE side - the JSON predicate itself is wrapped, not extractable
            ValidateError(db, R"(String::AsciiToUpper(JSON_VALUE(Text, '$.k1' RETURNING Utf8)) == "ABC")");
        });
    }

    Y_UNIT_TEST(JsonValueHandlerVariants) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // DEFAULT NULL ON EMPTY / ON ERROR - allowed (equivalent to no handler)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT NULL ON EMPTY) == 1)", {"\4key" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT NULL ON ERROR) == 1)", {"\4key" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT NULL ON EMPTY DEFAULT NULL ON ERROR) == 1)", {"\4key" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Utf8 DEFAULT NULL ON EMPTY) == "v")", {"\4key" + strSuffix("v")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Utf8 DEFAULT NULL ON ERROR) == "v")", {"\4key" + strSuffix("v")});

            // Range comparison with DEFAULT NULL - path-only
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT NULL ON EMPTY) > 10)", {"\4key"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT NULL ON ERROR) > 10)", {"\4key"});

            // Combinations of NULL / ERROR / DEFAULT NULL handlers - all allowed
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 NULL ON EMPTY ERROR ON ERROR) == 1)", {"\4key" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 ERROR ON EMPTY NULL ON ERROR) == 1)", {"\4key" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 NULL ON EMPTY DEFAULT NULL ON ERROR) == 1)", {"\4key" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT NULL ON EMPTY ERROR ON ERROR) == 1)", {"\4key" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 ERROR ON EMPTY DEFAULT NULL ON ERROR) == 1)", {"\4key" + numSuffix(1)});

            // Mixed - non-NULL DEFAULT on either side blocks extraction
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT 12 ON EMPTY NULL ON ERROR) == 1)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 NULL ON EMPTY DEFAULT 12 ON ERROR) == 1)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT 12 ON EMPTY DEFAULT NULL ON ERROR) == 1)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT NULL ON EMPTY DEFAULT 12 ON ERROR) == 1)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 DEFAULT 12 ON EMPTY ERROR ON ERROR) == 1)");
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Int32 ERROR ON EMPTY DEFAULT 12 ON ERROR) == 1)");

            // Handler combinations with the JSON_VALUE itself as Bool predicate
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Bool DEFAULT NULL ON EMPTY))", {"\4key" + trueSuffix});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key' RETURNING Bool DEFAULT NULL ON ERROR))", {"\4key" + trueSuffix});
            ValidateError(db, R"(JSON_VALUE(Text, '$.key' RETURNING Bool DEFAULT true ON ERROR))");
        });
    }

    Y_UNIT_TEST(JsonPassingEdgeCases) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // PASSING bound to a non-literal arithmetic expression
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1 == $v' PASSING (1 + 2) AS v RETURNING Bool))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING (1 + 2) AS v))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1 == $v' PASSING (1 * 3) AS v RETURNING Bool))");

            // PASSING bound to a conditional expression
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1 == $v' PASSING IF(Data = "a", 1, 2) AS v RETURNING Bool))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING IF(Data = "a", 1, 2) AS v))");

            // PASSING bound to a column reference
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1 == $v' PASSING Data AS v RETURNING Bool))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING Data AS v))");

            // CAST around a scalar literal in PASSING - unwrapped, literal binds
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1 == $v' PASSING CAST(10 AS Int32) AS v RETURNING Bool))",
                {"\3k1" + numSuffix(10)});
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1 == $v' PASSING CAST("abc" AS Utf8) AS v RETURNING Bool))",
                {"\3k1" + strSuffix("abc")});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING CAST(10 AS Int32) AS v))",
                {"\3k1\3k2" + numSuffix(10)});

            // CAST around a parameter in PASSING - unwrapped, param binds
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1 == $v' PASSING CAST($p AS Utf8) AS v RETURNING Bool))",
                {NJsonIndex::TToken{"\3k1", "$p"}},
                TParamsBuilder().AddParam("$p").Utf8("a").Build().Build());
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING CAST($p AS Int32) AS v))",
                {NJsonIndex::TToken{"\3k1\3k2", "$p"}},
                TParamsBuilder().AddParam("$p").Int32(5).Build().Build());

            // SQL keywords as PASSING variable names
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $exists)' PASSING 1 AS exists))", "Error: mismatched input");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $select)' PASSING "x"u AS select))", "Error: mismatched input");

            // PASSING with non-supported types
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING Json('"x"') AS v))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING Date('2026-01-01') AS v))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING DateTime('2026-01-01T00:00:00Z') AS v))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == $v)' PASSING Timestamp('2026-01-01T00:00:00Z') AS v))");
        });
    }

    Y_UNIT_TEST(CrossColumnPredicates) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        // Custom table with two JSON columns; index lives on Text only.
        {
            const auto query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text JsonDocument,
                    Text2 JsonDocument,
                    Data Utf8,
                    PRIMARY KEY (Key)
                );
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

        // AND/OR of indexable predicates on different JSON columns - rejected.
        ValidateError(db,
            R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text2, '$.k2'))",
            "Cross-column predicates are not supported");
        ValidateError(db,
            R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text2, '$.k2'))",
            "Cross-column predicates are not supported");
        ValidateError(db,
            R"(JSON_EXISTS(Text2, '$.k1') AND JSON_EXISTS(Text, '$.k2'))",
            "Cross-column predicates are not supported");
        ValidateError(db,
            R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" AND JSON_VALUE(Text2, '$.k2' RETURNING Utf8) == "b")",
            "Cross-column predicates are not supported");
        ValidateError(db,
            R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "a" OR JSON_VALUE(Text2, '$.k2' RETURNING Utf8) == "b")",
            "Cross-column predicates are not supported");
        ValidateError(db,
            R"(JSON_EXISTS(Text, '$.k1') AND JSON_VALUE(Text2, '$.k2' RETURNING Utf8) == "b")",
            "Cross-column predicates are not supported");

        // Same predicate on different columns combined with non-indexable
        ValidateError(db,
            R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text2, '$.k2') AND Data = "x"u)",
            "Cross-column predicates are not supported");
        // Cross-column comparison directly between JSON columns
        ValidateError(db,
            R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == JSON_VALUE(Text2, '$.k2' RETURNING Utf8))",
            "Cross-column predicates are not supported");
        ValidateError(db,
            R"(JSON_VALUE(Text2, '$.k1' RETURNING Utf8) == JSON_VALUE(Text, '$.k2' RETURNING Utf8))",
            "Cross-column predicates are not supported");
    }

    // JSON_VALUE IN $param
    Y_UNIT_TEST(JsonValueInListParam) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Collectable path
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) IN $p)",
                {NJsonIndex::TToken{"\3k1", "$p"}},
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Int64(1)
                    .AddListItem().Int64(0)
                    .EndList().Build().Build(),
                "or");

            // Deep path
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.k2' RETURNING Utf8) IN $p)",
                {NJsonIndex::TToken{"\3k1\3k2", "$p"}},
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Utf8("a")
                    .EndList().Build().Build(),
                "or");

            // Non-collectable path 1
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.type()' RETURNING Utf8) IN $p)",
                {NJsonIndex::TToken{"\3k1", ""}},
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Utf8("string")
                    .EndList().Build().Build(),
                "and");

            // Non-collectable path 2
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.*' RETURNING Utf8) IN $p)",
                {NJsonIndex::TToken{"\3k1", ""}},
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Utf8("v")
                    .EndList().Build().Build(),
                "and");

            // Supported scalar item types in list
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN $p)",
                {NJsonIndex::TToken{"\3k1", "$p"}},
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Utf8("x")
                    .EndList().Build().Build(),
                "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN $p)",
                {NJsonIndex::TToken{"\3k1", "$p"}},
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Int32(1)
                    .EndList().Build().Build(),
                "or");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Double) IN $p)",
                {NJsonIndex::TToken{"\3k1", "$p"}},
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Double(1.5)
                    .EndList().Build().Build(),
                "or");

            // RETURNING Bool with list param is not supported
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) IN $p)",
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Bool(true)
                    .EndList().Build().Build());

            // AND with another predicate: OR wins (list param carries OR mode into merge)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) IN $p AND JSON_EXISTS(Text, '$.k2'))",
                {NJsonIndex::TToken{"\3k1", "$p"}, NJsonIndex::TToken{"\3k2", ""}},
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Int64(1)
                    .EndList().Build().Build(),
                "or");

            // OR with another predicate: stays OR
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) IN $p OR JSON_EXISTS(Text, '$.k2'))",
                {NJsonIndex::TToken{"\3k1", "$p"}, NJsonIndex::TToken{"\3k2", ""}},
                TParamsBuilder().AddParam("$p").BeginList()
                    .AddListItem().Int64(1)
                    .EndList().Build().Build(),
                "or");
        });
    }

    Y_UNIT_TEST(JsonFunctionsMisc) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // PASSING with RETURNING Bool combined with outer comparison
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1 == $v' PASSING 1 AS v RETURNING Bool) >= true)",
                "Comparison JSON_VALUE with RETURNING Bool is not supported");
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1 == $v' PASSING 1 AS v RETURNING Bool) < false)",
                "Comparison JSON_VALUE with RETURNING Bool is not supported");

            // JSON_VALUE inside string concatenation on the LHS - non-indexable
            ValidateError(db, R"(("prefix:" || JSON_VALUE(Text, '$.k1' RETURNING Utf8)) == "prefix:abc")");
            // AND with this non-indexable form + indexable JE
            ValidateTokens(db,
                R"(("prefix:" || JSON_VALUE(Text, '$.k1' RETURNING Utf8)) == "prefix:abc" AND JSON_EXISTS(Text, '$.k2'))",
                {"\3k2"});

            // large numeric literals inside a JSON_EXISTS filter
            constexpr double rounded = 9007199254740992.0;
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 9007199254740993)'))",
                {"\3k1\3k2" + numSuffix(rounded)});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == -9007199254740993)'))",
                {"\3k1\3k2" + numSuffix(-rounded)});
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 9007199254740992)'))",
                {"\3k1\3k2" + numSuffix(rounded)});

            // JSON_QUERY as a source for JSON_EXISTS
            ValidateError(db,
                R"(JSON_EXISTS(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.b ? (@.c == $v)' PASSING "x"u AS v))");
            ValidateError(db,
                R"(JSON_EXISTS(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.b ? (@.c == $v)' PASSING $p AS v))",
                TParamsBuilder().AddParam("$p").Utf8("x").Build().Build());
            ValidateError(db,
                R"(JSON_VALUE(JSON_QUERY(Text, '$.a' WITHOUT ARRAY WRAPPER), '$.k == $v' PASSING $p AS v RETURNING Bool))",
                TParamsBuilder().AddParam("$p").Utf8("x").Build().Build());
        });
    }
}
}  // namespace NKikimr::NKqp
