#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

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
            [["object data 7"];[16u];"\2id"];
            [["object data 7"];[16u];"\2id\0\4\0\0\0\0@\x87\xE4@"];
            [["object data 7"];[16u];"\5brand"];
            [["object data 7"];[16u];"\5brand\0\3bricks"];
            [["object data 7"];[16u];"\5parts"];
            [["object data 7"];[16u];"\5parts\2id"];
            [["object data 7"];[16u];"\5parts\2id\0\4\0\0\0\0\x80\xC3\xDF@"];
            [["object data 7"];[16u];"\5parts\2id\0\4\0\0\0\0\xC0\xC2\xDF@"];
            [["object data 7"];[16u];"\5parts\4name"];
            [["object data 7"];[16u];"\5parts\4name\0\0031x3"];
            [["object data 7"];[16u];"\5parts\4name\0\0033x5"];
            [["object data 7"];[16u];"\5parts\5count"];
            [["object data 7"];[16u];"\5parts\5count\0\4\0\0\0\0\0\0\x1C@"];
            [["object data 7"];[16u];"\5parts\5count\0\4\0\0\0\0\0\0001@"];
            [["object data 7"];[16u];"\5price"];
            [["object data 7"];[16u];"\5price\0\2"];
            [["object data 7"];[16u];"\npart_count"];
            [["object data 7"];[16u];"\npart_count\0\4\0\0\0\0\0\xE4\x95@"]
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
            [[16u];"\2id"];
            [[16u];"\2id\0\4\0\0\0\0@\x87\xE4@"];
            [[16u];"\5brand"];
            [[16u];"\5brand\0\3bricks"];
            [[16u];"\5parts"];
            [[16u];"\5parts\2id"];
            [[16u];"\5parts\2id\0\4\0\0\0\0\x80\xC3\xDF@"];
            [[16u];"\5parts\2id\0\4\0\0\0\0\xC0\xC2\xDF@"];
            [[16u];"\5parts\4name"];
            [[16u];"\5parts\4name\0\0031x3"];
            [[16u];"\5parts\4name\0\0033x5"];
            [[16u];"\5parts\5count"];
            [[16u];"\5parts\5count\0\4\0\0\0\0\0\0\x1C@"];
            [[16u];"\5parts\5count\0\4\0\0\0\0\0\0001@"];
            [[16u];"\5price"];
            [[16u];"\5price\0\2"];
            [[16u];"\npart_count"];
            [[16u];"\npart_count\0\4\0\0\0\0\0\xE4\x95@"]
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

void ValidatePredicate(TQueryClient& db, const std::string& predicate) {
    static constexpr const char* Table = "TestTable";
    static constexpr const char* IndexTable = "json_idx";
    auto query = [&](const std::string& indexPart, const std::string& pred) {
        return std::format(R"(
            SELECT Key, Text FROM {} {} WHERE {} ORDER BY Key;
        )", Table, (indexPart.empty() ? "" : "VIEW  " + indexPart), pred);
    };

    auto mainResult = db.ExecuteQuery(query("", predicate), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(mainResult.IsSuccess(), mainResult.GetIssues().ToString());

    auto indexResult = db.ExecuteQuery(query(IndexTable, predicate), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(indexResult.IsSuccess(), indexResult.GetIssues().ToString());

    // Cerr << "MAIN: " << Endl << FormatResultSetYson(mainResult.GetResultSet(0)) << Endl;
    // Cerr << "INDEX: " << Endl << FormatResultSetYson(indexResult.GetResultSet(0)) << Endl;

    Cerr << predicate << ", main size: " << mainResult.GetResultSet(0).RowsCount() << ", index size: " << indexResult.GetResultSet(0).RowsCount() << Endl;
    CompareYson(FormatResultSetYson(mainResult.GetResultSet(0)), FormatResultSetYson(indexResult.GetResultSet(0)));
}

void ValidateError(TQueryClient& db, const std::string& predicate) {
    static constexpr const char* Table = "TestTable";
    static constexpr const char* IndexTable = "json_idx";
    auto query = [&](const std::string& indexPart, const std::string& pred) {
        return std::format(R"(
            SELECT * FROM {} {} WHERE {} ORDER BY Key;
        )", Table, (indexPart.empty() ? "" : "VIEW  " + indexPart), pred);
    };

    auto result = db.ExecuteQuery(query(IndexTable, predicate), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), "Predicate: " + predicate + ", issues: " + result.GetIssues().ToString());
    UNIT_ASSERT_STRING_CONTAINS_C(result.GetIssues().ToString(), "Failed to extract search terms from predicate", "for predicate = " << predicate);
}

void TestSelectJsonExists(bool isJsonDocument, bool isStrict,
    const std::function<void(TQueryClient&, const std::function<std::string(const std::string&)>&)>& body)
{
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

    const std::string jsonType = isJsonDocument ? "JsonDocument" : "Json";
    const auto jsonExists = [&](const std::string& predicate) {
        return std::format("JSON_EXISTS(Text, '{}')", (isStrict ? "strict " : "lax ") + predicate);
    };

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

void ValidateTokens(TQueryClient& db, const std::string& predicate, const std::vector<std::string>& expected,
    const std::string& defaultOperator = "and")
{
    auto settings = TExecuteQuerySettings().ExecMode(EExecMode::Explain);
    auto query = std::format(R"(
        SELECT * FROM TestTable VIEW json_idx WHERE {};
    )", predicate);

    auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), "Predicate: " + predicate + ", error: " + result.GetIssues().ToString());
    UNIT_ASSERT_C(result.GetStats(), "Stats are empty");

    auto plan = result.GetStats()->GetPlan();
    UNIT_ASSERT_C(plan, "Plan is empty");

    Cerr << "PREDICATE: " << predicate << Endl;
    auto ast = result.GetStats()->GetAst();
    UNIT_ASSERT_C(ast, "AST is empty");
    Cout << "AST: " << Endl << *ast << Endl;
    Cout << "PLAN: " << Endl << *plan << Endl;

    NJson::TJsonValue planJson;
    auto success = NJson::ReadJsonTree(*plan, &planJson, true);
    UNIT_ASSERT_C(success, "Failed to read plan as JSON");

    auto op = planJson["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Operators"][0]["DefaultOperator"].GetString();
    UNIT_ASSERT_VALUES_EQUAL(op, '"' + defaultOperator + '"');

    auto splitTokens = planJson["Plan"]["Plans"][0]["Plans"][0]["Plans"][0]["Operators"][0]["Tokens"].GetString();
    auto tokens = SplitString(splitTokens, ", ");
    UNIT_ASSERT_VALUES_EQUAL_C(tokens.size(), expected.size(), "for predicate = " << predicate);

    for (const auto& token : tokens) {
        auto decoded = HexDecode(token);
        if (std::find(expected.begin(), expected.end(), decoded) == expected.end()) {
            UNIT_FAIL("Unexpected token: " << decoded);
        }
    }
}

void TestSelectJsonTokens(const std::function<void(TQueryClient&)>& body) {
    auto kikimr = Kikimr();
    auto db = kikimr.GetQueryClient();

    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

    const std::string jsonType = "JsonDocument";
    CreateTestTable(db, jsonType, /* withIndex */ false);
    FillTestTable(db, "TestTable", jsonType);

    {
        auto query = R"(
            ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text)
        )";
        auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    body(db);
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
                [[1u];"\2k1"];
                [[1u];"\2k1\0\0"];
                [[1u];"\2k1\0\3v1"];
                [[1u];"\2k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\2k2"];
                [[2u];"\2k2\0\1"];
                [[2u];"\2k2\0\3v2"];
                [[2u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];""];
                [[3u];"\2k3"];
                [[3u];"\2k3\0\0"];
                [[3u];"\2k3\0\3v3"];
                [[3u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];""];
                [[4u];"\2k4"];
                [[4u];"\2k4\0\1"];
                [[4u];"\2k4\0\3v4"];
                [[4u];"\2k4\0\4\0\0\0\0\0\0\x10@"]
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
                [[1u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[1u];"\2k3\0\3v3"];
                [[1u];"\2k3\0\0"];
                [[1u];"\2k3"];
                [[1u];""];
                [[2u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[2u];""];
                [[2u];"\2k2"];
                [[2u];"\2k2\0\1"];
                [[2u];"\2k2\0\3v2"];
                [[3u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\2k2\0\3v2"];
                [[3u];"\2k2\0\1"];
                [[3u];"\2k2"];
                [[3u];""];
                [[4u];""];
                [[4u];"\2k4"];
                [[4u];"\2k4\0\1"];
                [[4u];"\2k4\0\3v4"];
                [[4u];"\2k4\0\4\0\0\0\0\0\0\x10@"];
                [[5u];""];
                [[5u];"\2k5"];
                [[5u];"\2k5\0\0"];
                [[5u];"\2k5\0\3v5"];
                [[5u];"\2k5\0\4\0\0\0\0\0\0\x14@"]
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
                [[1u];"\2k1"];
                [[1u];"\2k1\0\0"];
                [[1u];"\2k1\0\3v1"];
                [[1u];"\2k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\2k2"];
                [[2u];"\2k2\0\1"];
                [[2u];"\2k2\0\3v2"];
                [[2u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];""];
                [[3u];"\2k3"];
                [[3u];"\2k3\0\0"];
                [[3u];"\2k3\0\3v3"];
                [[3u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];""];
                [[4u];"\2k4"];
                [[4u];"\2k4\0\1"];
                [[4u];"\2k4\0\3v4"];
                [[4u];"\2k4\0\4\0\0\0\0\0\0\x10@"]
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
                [[1u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[1u];"\2k3\0\3v3"];
                [[1u];"\2k3\0\0"];
                [[1u];"\2k3"];
                [[1u];""];
                [[2u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[2u];""];
                [[2u];"\2k2"];
                [[2u];"\2k2\0\1"];
                [[2u];"\2k2\0\3v2"];
                [[3u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\2k2\0\3v2"];
                [[3u];"\2k2\0\1"];
                [[3u];"\2k2"];
                [[3u];""];
                [[4u];""];
                [[4u];"\2k4"];
                [[4u];"\2k4\0\1"];
                [[4u];"\2k4\0\3v4"];
                [[4u];"\2k4\0\4\0\0\0\0\0\0\x10@"];
                [[5u];""];
                [[5u];"\2k5"];
                [[5u];"\2k5\0\0"];
                [[5u];"\2k5\0\3v5"];
                [[5u];"\2k5\0\4\0\0\0\0\0\0\x14@"]
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
                [[1u];"\2k1"];
                [[1u];"\2k1\0\0"];
                [[1u];"\2k1\0\3v1"];
                [[1u];"\2k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\2k2"];
                [[2u];"\2k2\0\1"];
                [[2u];"\2k2\0\3v2"];
                [[2u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];""];
                [[3u];"\2k3"];
                [[3u];"\2k3\0\0"];
                [[3u];"\2k3\0\3v3"];
                [[3u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];""];
                [[4u];"\2k4"];
                [[4u];"\2k4\0\1"];
                [[4u];"\2k4\0\3v4"];
                [[4u];"\2k4\0\4\0\0\0\0\0\0\x10@"]
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
                [[1u];"\2k1"];
                [[1u];"\2k1\0\0"];
                [[1u];"\2k1\0\3v1"];
                [[1u];"\2k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\2k2"];
                [[2u];"\2k2\0\1"];
                [[2u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[2u];"\2k2\0\3v2"];
                [[3u];""];
                [[3u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[3u];"\2k3\0\3v3"];
                [[3u];"\2k3\0\0"];
                [[3u];"\2k3"];
                [[4u];"\2k4\0\4\0\0\0\0\0\0\x10@"];
                [[4u];""];
                [[4u];"\2k4"];
                [[4u];"\2k4\0\1"];
                [[4u];"\2k4\0\3v4"];
                [[5u];"\2k3"];
                [[5u];"\2k3\0\0"];
                [[5u];""];
                [[5u];"\2k3\0\3v3"];
                [[5u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[6u];"\2k2\0\1"];
                [[6u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[6u];"\2k2"];
                [[6u];""];
                [[6u];"\2k2\0\3v2"]
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
                [[1u];"\2k1"];
                [[1u];"\2k1\0\0"];
                [[1u];"\2k1\0\3v1"];
                [[1u];"\2k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\2k2"];
                [[2u];"\2k2\0\1"];
                [[2u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[2u];"\2k2\0\3v2"];
                [[3u];""];
                [[3u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[3u];"\2k3\0\3v3"];
                [[3u];"\2k3\0\0"];
                [[3u];"\2k3"];
                [[4u];"\2k4\0\4\0\0\0\0\0\0\x10@"];
                [[4u];""];
                [[4u];"\2k4"];
                [[4u];"\2k4\0\1"];
                [[4u];"\2k4\0\3v4"];
                [[5u];"\2k3"];
                [[5u];"\2k3\0\0"];
                [[5u];""];
                [[5u];"\2k3\0\3v3"];
                [[5u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[6u];"\2k2\0\1"];
                [[6u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[6u];"\2k2"];
                [[6u];""];
                [[6u];"\2k2\0\3v2"]
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
                [[1u];"\2k1"];
                [[1u];"\2k1\0\0"];
                [[1u];"\2k1\0\3v1"];
                [[1u];"\2k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\2k2"];
                [[2u];"\2k2\0\1"];
                [[2u];"\2k2\0\3v2"];
                [[2u];"\2k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];""];
                [[3u];"\2k3"];
                [[3u];"\2k3\0\0"];
                [[3u];"\2k3\0\3v3"];
                [[3u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];""];
                [[4u];"\2k4"];
                [[4u];"\2k4\0\1"];
                [[4u];"\2k4\0\3v4"];
                [[4u];"\2k4\0\4\0\0\0\0\0\0\x10@"]
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
                [[1u];"\2k1"];
                [[1u];"\2k1\0\0"];
                [[1u];"\2k1\0\3v1"];
                [[1u];"\2k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];""];
                [[2u];"\3k10"];
                [[2u];"\3k10\0\1"];
                [[2u];"\3k10\0\3v10"];
                [[2u];"\3k10\0\4\0\0\0\0\0\0$@"];
                [[3u];""];
                [[3u];"\3k10"];
                [[3u];"\3k10\0\1"];
                [[3u];"\3k10\0\3v10"];
                [[3u];"\3k10\0\4\0\0\0\0\0\0$@"];
                [[4u];""];
                [[4u];"\2k4"];
                [[4u];"\2k4\0\1"];
                [[4u];"\2k4\0\3v4"];
                [[4u];"\2k4\0\4\0\0\0\0\0\0\x10@"]
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
                [[1u];"\4k100"];
                [[1u];"\4k100\0\0"];
                [[1u];"\4k100\0\3v100"];
                [[1u];"\4k100\0\4\0\0\0\0\0\0Y@"];
                [[2u];""];
                [[2u];"\4k100"];
                [[2u];"\4k100\0\0"];
                [[2u];"\4k100\0\3v100"];
                [[2u];"\4k100\0\4\0\0\0\0\0\0Y@"];
                [[3u];""];
                [[3u];"\4k100"];
                [[3u];"\4k100\0\0"];
                [[3u];"\4k100\0\3v100"];
                [[3u];"\4k100\0\4\0\0\0\0\0\0Y@"];
                [[4u];""];
                [[4u];"\4k100"];
                [[4u];"\4k100\0\0"];
                [[4u];"\4k100\0\3v100"];
                [[4u];"\4k100\0\4\0\0\0\0\0\0Y@"]
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
                [[1u];"\2k1"];
                [[1u];"\2k1\0\0"];
                [[1u];"\2k1\0\3v1"];
                [[1u];"\2k1\0\4\0\0\0\0\0\0\xF0?"];
                [[3u];""];
                [[3u];"\2k3"];
                [[3u];"\2k3\0\0"];
                [[3u];"\2k3\0\3v3"];
                [[3u];"\2k3\0\4\0\0\0\0\0\0\x08@"];
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
            ValidatePredicate(db, jsonExists("$"));
        });
    }

    Y_UNIT_TEST_QUAD(SelectJsonExists_MemberAccess, IsJsonDocument, IsStrict) {
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
            ValidateError(db, jsonExists("null"));
            ValidateError(db, jsonExists("1"));
            ValidateError(db, jsonExists("\"str\""));
            ValidateError(db, jsonExists("true"));
            ValidateError(db, jsonExists("false"));
        });
    }

    // Filter with != (inequality) and range comparisons (<, <=, >, >=)
    Y_UNIT_TEST_QUAD(SelectJsonExists_FilterInequality, IsJsonDocument, IsStrict) {
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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
        TestSelectJsonExists(IsJsonDocument, IsStrict, [](TQueryClient& db, const auto& jsonExists) {
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

    Y_UNIT_TEST(JsonExistsTokens) {
        TestSelectJsonTokens([](TQueryClient& db) {
            // Basic path exists cases
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.key'))", {"\3key"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 2)'))", {"\2k1\2k2" + numSuffix(2)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == true && @.k2 == false)'))", {"\2k1" + trueSuffix, "\2k2" + falseSuffix}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")'))", {"\2k1" + nullSuffix, "\2k2" + strSuffix("str")}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.key') IS NOT NULL)", {"\3key"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.key') == true)", {"\3key"});

            // Negated JSON_EXISTS is not supported by JSON index
            ValidateError(db, R"(JSON_EXISTS(Text, '$.key') == false)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.key') != true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.key') IS NULL)");

            // AND combinations
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2'))",
                {"\2k1", "\2k2"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null && @.k2 == "str")') AND JSON_EXISTS(Text, '$ ? (@.k3 == true && @.k4 == false)'))",
                {"\2k1" + nullSuffix, "\2k2" + strSuffix("str"), "\2k3" + trueSuffix, "\2k4" + falseSuffix}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")') AND JSON_EXISTS(Text, '$ ? (@.k3 == true && @.k4 == false)'))",
                {"\2k1" + nullSuffix, "\2k2" + strSuffix("str"), "\2k3" + trueSuffix, "\2k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null && @.k2 == "str")') AND JSON_EXISTS(Text, '$ ? (@.k3 == true || @.k4 == false)'))",
                {"\2k1" + nullSuffix, "\2k2" + strSuffix("str"), "\2k3" + trueSuffix, "\2k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")') AND JSON_EXISTS(Text, '$ ? (@.k3 == true || @.k4 == false)'))",
                {"\2k1" + nullSuffix, "\2k2" + strSuffix("str"), "\2k3" + trueSuffix, "\2k4" + falseSuffix}, "or");

            // OR combinations
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2'))",
                {"\2k1", "\2k2"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null && @.k2 == "str")') OR JSON_EXISTS(Text, '$ ? (@.k3 == true && @.k4 == false)'))",
                {"\2k1" + nullSuffix, "\2k2" + strSuffix("str"), "\2k3" + trueSuffix, "\2k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")') OR JSON_EXISTS(Text, '$ ? (@.k3 == true && @.k4 == false)'))",
                {"\2k1" + nullSuffix, "\2k2" + strSuffix("str"), "\2k3" + trueSuffix, "\2k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null && @.k2 == "str")') OR JSON_EXISTS(Text, '$ ? (@.k3 == true || @.k4 == false)'))",
                {"\2k1" + nullSuffix, "\2k2" + strSuffix("str"), "\2k3" + trueSuffix, "\2k4" + falseSuffix}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == null || @.k2 == "str")') OR JSON_EXISTS(Text, '$ ? (@.k3 == true || @.k4 == false)'))",
                {"\2k1" + nullSuffix, "\2k2" + strSuffix("str"), "\2k3" + trueSuffix, "\2k4" + falseSuffix}, "or");

            // Mixed combinations
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))",
                {"\2k1", "\2k2", "\2k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))",
                {"\2k1", "\2k2", "\2k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3'))",
                {"\2k1", "\2k2", "\2k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3'))",
                {"\2k1", "\2k2", "\2k3"}, "or");
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')) AND JSON_EXISTS(Text, '$.k3'))",
                {"\2k1", "\2k2", "\2k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') AND (JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3')))",
                {"\2k1", "\2k2", "\2k3"}, "and");
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')) AND (JSON_EXISTS(Text, '$.k3') AND JSON_EXISTS(Text, '$.k4')))",
                {"\2k1", "\2k2", "\2k3", "\2k4"}, "and");
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')) OR JSON_EXISTS(Text, '$.k3'))",
                {"\2k1", "\2k2", "\2k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$.k1') OR (JSON_EXISTS(Text, '$.k2') OR JSON_EXISTS(Text, '$.k3')))",
                {"\2k1", "\2k2", "\2k3"}, "or");
            ValidateTokens(db,
                R"((JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')) OR (JSON_EXISTS(Text, '$.k3') OR JSON_EXISTS(Text, '$.k4')))",
                {"\2k1", "\2k2", "\2k3", "\2k4"}, "or");

            // NOT JSON_EXISTS and wrapped-NOT forms fall through to "nothing to extract"
            ValidateError(db, R"(NOT JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') AND JSON_EXISTS(Text, '$.k2')))");
            ValidateError(db, R"(NOT (JSON_EXISTS(Text, '$.k1') OR JSON_EXISTS(Text, '$.k2')))");

            // Filter equality: covers every literal type; the token carries the value suffix
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == null)'))", {"\2k1\2k2" + nullSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == true)'))", {"\2k1\2k2" + trueSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == false)'))", {"\2k1\2k2" + falseSuffix});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == "abc")'))", {"\2k1\2k2" + strSuffix("abc")});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == 42)'))", {"\2k1\2k2" + numSuffix(42)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 == -1.5)'))", {"\2k1\2k2" + numSuffix(-1.5)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 == @.k2)'))", {"\2k1\2k2" + numSuffix(2)});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? ("s" == @.k2)'))", {"\2k1\2k2" + strSuffix("s")});

            // Filter inequality / range: path only, no value suffix
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 != 2)'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 > 2)'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 < 2)'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 >= 2)'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 <= 2)'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 != null)'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (@.k2 != "abc")'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 > @.k2)'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 < @.k2)'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 >= @.k2)'))", {"\2k1\2k2"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1 ? (2 <= @.k2)'))", {"\2k1\2k2"});

            // Filter path-vs-path comparisons: two tokens, AND mode (value suffix dropped)
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == @.k2)'))", {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 != @.k2)'))", {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 > @.k2)'))", {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 < @.k2)'))", {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 >= @.k2)'))", {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 <= @.k2)'))", {"\2k1", "\2k2"}, "and");

            // Filter arithmetic (path vs literal): path only, no value suffix
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + 1 == 2)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 - 1 == 0)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 * 2 == 4)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 / 2 == 1)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 % 2 == 0)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + 1 > 2)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 * 2 != 4)'))", {"\2k1"});

            // Filter arithmetic (path vs path): two tokens, AND mode
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + @.k2 == 5)'))", {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 - @.k2 > 0)'))", {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 * @.k2 < 10)'))", {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 / @.k2 >= 1)'))", {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 % @.k2 != 0)'))", {"\2k1", "\2k2"}, "and");

            // Filter unary operators: path only
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (-@.k1 == -1)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (+@.k1 == 1)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (-@.k1 > 0)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.abs() == 1)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.abs() > 5)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.size() == 3)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.size() > 0)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$.k1.size() ? (@ == 3)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (-@.k1.abs() == -1)'))", {"\2k1"});
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.abs() - @.k2.abs() == 0)'))", {"\2k1", "\2k2"}, "and");

            // && / || inside jsonpath: mode propagates from inner operator
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 && @.k2 != 2)'))",
                {"\2k1" + numSuffix(1), "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 || @.k2 != 2)'))",
                {"\2k1" + numSuffix(1), "\2k2"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 > 0 && @.k2 < 10)'))",
                {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 > 0 || @.k2 < 10)'))",
                {"\2k1", "\2k2"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + 1 == 2 && @.k2 * 2 == 4)'))",
                {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + 1 == 2 || @.k2 * 2 == 4)'))",
                {"\2k1", "\2k2"}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1.abs() == 1 && @.k2.size() > 0)'))",
                {"\2k1", "\2k2"}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (-@.k1 == -1 || @.k2 % 2 == 0)'))",
                {"\2k1", "\2k2"}, "or");

            // Three-way && / || inside jsonpath
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 && @.k2 == 2 && @.k3 == 3)'))",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3" + numSuffix(3)}, "and");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 || @.k2 == 2 || @.k3 == 3)'))",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3" + numSuffix(3)}, "or");

            // Mixed && and || inside jsonpath
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@.k1 == 1 && @.k2 == 2) || @.k3 == 3)'))",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3" + numSuffix(3)}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 || (@.k2 == 2 && @.k3 == 3))'))",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3" + numSuffix(3)}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? ((@.k1 > 0 && @.k2 != null) || @.k3 == "text")'))",
                {"\2k1", "\2k2", "\2k3" + strSuffix("text")}, "or");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@.k1 + @.k2 == 5 && @.k3 == "text" || @.k4 == null)'))",
                {"\2k1", "\2k2", "\2k3" + strSuffix("text"), "\2k4" + nullSuffix}, "or");

            // Outer SQL AND/OR over filters with &&/|| inside
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 && @.k2 == 2)') AND JSON_EXISTS(Text, '$ ? (@.k3 > 0)'))",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3"}, "and");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 || @.k2 == 2)') AND JSON_EXISTS(Text, '$ ? (@.k3 > 0)'))",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 == 1 && @.k2 == 2)') OR JSON_EXISTS(Text, '$ ? (@.k3 > 0)'))",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3"}, "or");
            ValidateTokens(db,
                R"(JSON_EXISTS(Text, '$ ? (@.k1 + @.k2 == 5)') AND JSON_EXISTS(Text, '$ ? (-@.k3 == -3)'))",
                {"\2k1", "\2k2", "\2k3"}, "and");

            // Outer range comparison with bool literal (non-equality): errors
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') > false)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') > true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') >= false)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') >= true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') < false)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') < true)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') <= false)");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') <= true)");

            // Flipped side (literal op JSON_EXISTS)
            ValidateError(db, R"(true > JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(false >= JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(false == JSON_EXISTS(Text, '$.k1'))");
            ValidateError(db, R"(true != JSON_EXISTS(Text, '$.k1'))");

            // JSON_EXISTS comparison with true rewrites to JSON_EXISTS without boolean comparison
            ValidateTokens(db, R"(true == JSON_EXISTS(Text, '$.k1'))", {"\2k1"});
            ValidateTokens(db, R"(false != JSON_EXISTS(Text, '$.k1'))", {"\2k1"});

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
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') >= true AND JSON_EXISTS(Text, '$.k2'))");
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') >= true OR JSON_EXISTS(Text, '$.k2') == true)");

            // Outer AND/OR over cross JSON_EXISTS comparisons
            ValidateError(db, R"(JSON_EXISTS(Text, '$.k1') > JSON_EXISTS(Text, '$.k2') AND JSON_EXISTS(Text, '$.k3'))");
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
        });
    }

    Y_UNIT_TEST(JsonValueTokens) {
        TestSelectJsonTokens([](TQueryClient& db) {
            // Supported RETURNING types
            ValidateError(db, R"(JSON_VALUE(Text, '$.key') IS NULL)"); // negation
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.key') IS NOT NULL)", {"\3key"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int8) == 1t)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint8) == 1ut)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int16) == 1s)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint16) == 1us)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint32) == 1u)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) == 1l)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Uint64) == 1ul)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Float) == 1.0f)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Double) == 1.0)", {"\2k1" + numSuffix(1)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING String) == "value"s)", {"\2k1" + strSuffix("value")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) == "value"u)", {"\2k1" + strSuffix("value")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == true)", {"\2k1" + trueSuffix});

            // Not supported RETURNING types
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Date) == Date("2021-01-01"))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Datetime) == Datetime("2021-01-01T00:00:00Z"))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Timestamp) == Timestamp("2021-01-01T00:00:00Z"))");

            // Default RETURNING type is Utf8
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1') == "1")", {"\2k1" + strSuffix("1")});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1') == "string")", {"\2k1" + strSuffix("string")});

            // Bool comparison with true
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == true)", {"\2k1" + trueSuffix});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool))", {"\2k1" + trueSuffix});

            // Bool comparison with false (negation)
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Bool) == false)");
            ValidateError(db, R"(NOT JSON_VALUE(Text, '$.k1' RETURNING Bool))");

            // Comparison with other literals
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) > 10)", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) < 10)", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) >= 10)", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) <= 10)", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) != 10)", {"\2k1"});

            // For some nodes inside the path, it cannot be combined with == operator
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 starts with "1"') == "true")", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.size()' RETURNING Int32) == 2)", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1.*' RETURNING Int32) == 2)", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1[*]' RETURNING Int32) == 2)", {"\2k1" + numSuffix(2)});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 + 1' RETURNING Int32) == 2)", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1 == 2' RETURNING Bool))", {"\2k1" + numSuffix(2)});

            // BETWEEN clause
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) BETWEEN 1 AND 10)", {"\2k1", "\2k1"});

            // AND/OR combinations - numeric equality
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 AND JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2)",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2)}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2)",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2)}, "or");

            // AND/OR combinations - string equality
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1') == "a" AND JSON_VALUE(Text, '$.k2') == "b")",
                {"\2k1" + strSuffix("a"), "\2k2" + strSuffix("b")}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1') == "a" OR JSON_VALUE(Text, '$.k2') == "b")",
                {"\2k1" + strSuffix("a"), "\2k2" + strSuffix("b")}, "or");

            // AND/OR with range comparisons - path-only tokens
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) > 5 AND JSON_VALUE(Text, '$.k2' RETURNING Int32) < 10)",
                {"\2k1", "\2k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) > 5 OR JSON_VALUE(Text, '$.k2' RETURNING Int32) < 10)",
                {"\2k1", "\2k2"}, "or");

            // AND/OR mixing equality and range
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1') == "a" AND JSON_VALUE(Text, '$.k2' RETURNING Int32) > 0)",
                {"\2k1" + strSuffix("a"), "\2k2"}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1') == "a" OR JSON_VALUE(Text, '$.k2' RETURNING Int32) > 0)",
                {"\2k1" + strSuffix("a"), "\2k2"}, "or");

            // Three-way AND/OR
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 AND JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2 AND JSON_VALUE(Text, '$.k3' RETURNING Int32) == 3)",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3" + numSuffix(3)}, "and");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2 OR JSON_VALUE(Text, '$.k3' RETURNING Int32) == 3)",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3" + numSuffix(3)}, "or");

            // Mixed AND/OR (AND binds tighter): both cases produce "or"
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 AND JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2 OR JSON_VALUE(Text, '$.k3' RETURNING Int32) == 3)",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3" + numSuffix(3)}, "or");
            ValidateTokens(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == 1 OR JSON_VALUE(Text, '$.k2' RETURNING Int32) == 2 AND JSON_VALUE(Text, '$.k3' RETURNING Int32) == 3)",
                {"\2k1" + numSuffix(1), "\2k2" + numSuffix(2), "\2k3" + numSuffix(3)}, "or");

            // Comparison operators with strings - path-only token (no value suffix for non-equality)
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1') > "abc")", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1') < "xyz")", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1') >= "abc")", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1') <= "xyz")", {"\2k1"});
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1') != "abc")", {"\2k1"});

            // Flipped operand order - string comparisons
            ValidateTokens(db, R"("abc" < JSON_VALUE(Text, '$.k1'))", {"\2k1"});
            ValidateTokens(db, R"("abc" > JSON_VALUE(Text, '$.k1'))", {"\2k1"});
            ValidateTokens(db, R"("abc" != JSON_VALUE(Text, '$.k1'))", {"\2k1"});

            // Flipped operand order - numeric comparisons
            ValidateTokens(db, R"(10 < JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\2k1"});
            ValidateTokens(db, R"(10 > JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\2k1"});
            ValidateTokens(db, R"(10 >= JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\2k1"});
            ValidateTokens(db, R"(10 <= JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\2k1"});
            ValidateTokens(db, R"(10 != JSON_VALUE(Text, '$.k1' RETURNING Int32))", {"\2k1"});

            // STARTS WITH
            ValidateTokens(db, R"(StartsWith(JSON_VALUE(Text, '$.k1'), "prefix"))", {"\2k1"});
            ValidateTokens(db, R"(StartsWith(JSON_VALUE(Text, '$.k1'), "prefix") AND JSON_VALUE(Text, '$.k1') == "a")", {"\2k1", "\2k1" + strSuffix("a")});

            // ENDS WITH: not extractable
            ValidateTokens(db, R"(EndsWith(JSON_VALUE(Text, '$.k1'), "suffix"))", {"\2k1"});

            // LIKE / ILIKE: not extractable
            ValidateTokens(db, R"(JSON_VALUE(Text, '$.k1') LIKE "pattern%")", {"\2k1"});
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1') ILIKE "pattern%")"); // udf

            // REGEXP: not extractable
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1') REGEXP "^pattern$")"); // udf

            // String concatenation (||) in a comparison: not extractable
            ValidateError(db, R"((JSON_VALUE(Text, '$.k1') || "suffix") == "value_suffix")");
            ValidateError(db, R"((JSON_VALUE(Text, '$.k1') || "suffix") == "value_suffix" AND JSON_VALUE(Text, '$.k2') == "b")");
        });
    }
}

}  // namespace NKikimr::NKqp
