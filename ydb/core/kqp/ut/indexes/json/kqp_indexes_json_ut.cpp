#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;

Y_UNIT_TEST_SUITE(KqpJsonIndexes) {

TKikimrRunner Kikimr() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    return TKikimrRunner(settings);
}

void CreateTestTable(NQuery::TQueryClient& db, const char* type = "Json", bool withIndex = false) {
    TString query = std::format(R"sql(
        CREATE TABLE `/Root/TestTable` (
            Key Uint64,
            Text {0},
            Data Utf8,
            PRIMARY KEY (Key)
            {1}
        );
    )sql", type, withIndex ? ", INDEX `json_idx` GLOBAL USING json ON (Text)" : "");
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

TResultSet ReadIndex(NQuery::TQueryClient& db, const char* table = "indexImplTable") {
    TString query = Sprintf(R"sql(
        SELECT * FROM `/Root/TestTable/json_idx/%s`;
    )sql", table);
    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return result.GetResultSet(0);
}

void DoTestAddJsonIndex(const TString& type, bool nullable, bool covered) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    auto columnType = type + (nullable ? "" : " not null");
    CreateTestTable(db, columnType.c_str());
    {
        TString castStart, castEnd;
        if (type == "JsonDocument") {
            castStart = !nullable ? "unwrap(cast(" : "cast(";
            castEnd = !nullable ? " as JsonDocument))" : " as JsonDocument)";
        }
        TString query = Sprintf(R"sql(
            UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                (10, %1$s"\"literal string\""%2$s, "d1"),
                (11, %1$s"0.123"%2$s, "data 2"),
                (12, %1$s"true"%2$s, "very long unit test data 3"),
                (13, %1$s"false"%2$s, "data 4"),
                (14, %1$s"null"%2$s, "data 5"),
                (15, %1$s"[false,\"item 1\",45]"%2$s, "array data 6"),
                (16, %1$s"{\"id\":42042,\"brand\":\"bricks\",\"part_count\":1401,\"price\":null,\"parts\":
                    [{\"id\":32526,\"count\":7,\"name\":\"3x5\"},{\"id\":32523,\"count\":17,\"name\":\"1x3\"}]}"%2$s, "object data 7")
        )sql", castStart.c_str(), castEnd.c_str());
        if (nullable) {
            query += ", (17, NULL, \"null data 8\")";
        }
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    {
        TString query = R"sql(
            ALTER TABLE `/Root/TestTable` ADD INDEX json_idx
                GLOBAL USING json ON (Text)
        )sql";
        if (covered) {
            query += " COVER (Data)";
        }
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }
    auto index = ReadIndex(db);
    if (covered) {
        CompareYson(R"([
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
        ])", NYdb::FormatResultSetYson(index));
    } else {
        CompareYson(R"([
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
        ])", NYdb::FormatResultSetYson(index));
    }
}

void ValidatePredicate(NQuery::TQueryClient& db, const std::string& table, const std::string& indexTable, const std::string& predicate) {
    auto query = [&](const std::string& table, const std::string& indexTable, const std::string& predicate) {
        return std::format(R"sql(
            SELECT k, v FROM {} {} WHERE {} ORDER BY k;
        )sql", table, (indexTable.empty() ? "" : "VIEW  " + indexTable), predicate);
    };

    auto mainResult = db.ExecuteQuery(query(table, "", predicate), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(mainResult.GetStatus(), EStatus::SUCCESS, mainResult.GetIssues().ToString());

    auto indexResult = db.ExecuteQuery(query(table, indexTable, predicate), TTxControl::NoTx()).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(indexResult.GetStatus(), EStatus::SUCCESS, indexResult.GetIssues().ToString());

    // Cerr << "MAIN: " << Endl << NYdb::FormatResultSetYson(mainResult.GetResultSet(0)) << Endl;
    // Cerr << "INDEX: " << Endl << NYdb::FormatResultSetYson(indexResult.GetResultSet(0)) << Endl;

    Cerr << predicate << ", main size: " << mainResult.GetResultSet(0).RowsCount() << ", index size: " << indexResult.GetResultSet(0).RowsCount() << Endl;
    CompareYson(NYdb::FormatResultSetYson(mainResult.GetResultSet(0)), NYdb::FormatResultSetYson(indexResult.GetResultSet(0)));
}

Y_UNIT_TEST(AddJsonIndexJson) {
    DoTestAddJsonIndex("Json", true, false);
}

Y_UNIT_TEST(AddJsonIndexJsonDocument) {
    DoTestAddJsonIndex("JsonDocument", true, false);
}

Y_UNIT_TEST(AddJsonIndexJsonNotNull) {
    DoTestAddJsonIndex("Json", false, false);
}

Y_UNIT_TEST(AddJsonIndexJsonDocumentNotNull) {
    DoTestAddJsonIndex("JsonDocument", false, false);
}

Y_UNIT_TEST(AddJsonIndexCoveringJson) {
    DoTestAddJsonIndex("Json", true, true);
}

Y_UNIT_TEST(AddJsonIndexCoveringJsonDocument) {
    DoTestAddJsonIndex("JsonDocument", true, true);
}

Y_UNIT_TEST(AddJsonIndexCoveringJsonNotNull) {
    DoTestAddJsonIndex("Json", false, true);
}

Y_UNIT_TEST(AddJsonIndexCoveringJsonDocumentNotNull) {
    DoTestAddJsonIndex("JsonDocument", false, true);
}

Y_UNIT_TEST(OnCreate) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTestTable(db, "Json", true);

    // TODO: Test it with update after implementing update
}

Y_UNIT_TEST(UnsupportedType) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    CreateTestTable(db, "Uint64");
    {
        TString query = R"sql(
            ALTER TABLE `/Root/TestTable` ADD INDEX json_idx
                GLOBAL USING json ON (Text)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST(NoMultipleColumns) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TestTable` (
                Key Uint64,
                Field1 Json,
                Field2 Json,
                PRIMARY KEY (Key)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            ALTER TABLE `/Root/TestTable` ADD INDEX json_idx
                GLOBAL USING json ON (Field1, Field2)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST(NonUint64Pk) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TestTable` (
                Key Uint32,
                Field1 Json,
                PRIMARY KEY (Key)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            ALTER TABLE `/Root/TestTable` ADD INDEX json_idx
                GLOBAL USING json ON (Field1)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST(NoCompositePk) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TestTable` (
                Key1 Uint64,
                Key2 Uint64,
                Field1 Json,
                PRIMARY KEY (Key1, Key2)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        TString query = R"sql(
            ALTER TABLE `/Root/TestTable` ADD INDEX json_idx
                GLOBAL USING json ON (Field1)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST(DisabledFlagRejectAlter) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(false);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    auto kikimr = TKikimrRunner(settings);
    auto db = kikimr.GetQueryClient();

    CreateTestTable(db, "Json");
    {
        TString query = R"sql(
            ALTER TABLE `/Root/TestTable` ADD INDEX json_idx
                GLOBAL USING json ON (Text)
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST(DisabledFlagRejectCreate) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(false);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    auto kikimr = TKikimrRunner(settings);
    auto db = kikimr.GetQueryClient();

    {
        TString query = R"sql(
            CREATE TABLE `/Root/TestTable` (
                Key Uint64,
                Text Json,
                Data Utf8,
                PRIMARY KEY (Key),
                INDEX `json_idx` GLOBAL USING json ON (Text)
            );
        )sql";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::GENERIC_ERROR, result.GetIssues().ToString());
    }
}

Y_UNIT_TEST_QUAD(SelectJsonExists, IsJsonDocument, IsStrict) {
    auto kikimr = Kikimr();
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
    kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

    auto db = kikimr.GetQueryClient();

    const std::string jsonType = IsJsonDocument ? "JsonDocument" : "Json";
    const auto jsonExists = [&](const std::string& predicate) {
        return std::format("JSON_EXISTS(v, '{}')", (IsStrict ? "strict " : "lax ") + predicate);
    };

    {
        auto query = std::format(R"(
            CREATE TABLE TestTable (
                k Uint64,
                v {0},
                PRIMARY KEY (k)
            );
        )", jsonType);
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    }

    {
        std::vector<std::string> values = {
            R"(('null'))",
            R"(('1'))",
            R"(('true'))",
            R"(('false'))",
            R"(('"string"'))",
            R"(('[]'))",
            R"(('{}'))",
            R"(('{"k1": null}'))",
            R"(('{"k1": 1}'))",
            R"(('{"k1": true}'))",
            R"(('{"k1": false}'))",
            R"(('{"k1": "string"}'))",
            R"(('{"k1": []}'))",
            R"(('{"k1": {}}'))",
            R"(('{"k1": [1, 2, 3]}'))",
            R"(('{"k1": "string", "k2": "string2"}'))",
            R"(('[{"k1": "string", "k2": "string2"}, {"k1": "string", "k2": "string2"}]'))",
            R"(('{"k1": {"k2": {"k3": {"k4": "string"}}}}'))",
            R"(('{"k1": 0, "k2": -1.5, "k3": "text", "k4": true, "k5": null, "k6": [1, "string", false], "k7": {"k1": "v"}}'))",
            R"(('{"k1": [{"k1": 10}, {"k1": 20}], "k2": {"k1": 2, "k2": true}}'))",
            R"(('{"": null}'))",
            R"(('{"": 1}'))",
            R"(('{"": true}'))",
            R"(('{"": false}'))",
            R"(('{"": "string"}'))",
            R"(('{"": []}'))",
            R"(('{"": {}}'))",
            R"(('{"": [1, 2, 3]}'))",
            R"(('{"": {"": {"": {"": ["", "string", null, 1, {"": ""}]}}}}'))",
            R"(('[{"": ""}, {"": ""}, {"": ""}, {"": ""}]'))",
            R"(('{"k1": [[{"k2": 0}, {"k2": 1}], []]}'))",
            R"(('[1, [2, [3, [4, []]]]]'))",
            R"(('["string", {"k1": 1}, [2, 3], 4, null, false]'))",
            R"(('[[{"k1": 1}], [{"k2": [{"k3": 2}]}]]'))",
            R"(('{"k1": {"k2": {"k3": [{"k1": "a"}, {"k2": "b"}], "k4": [0, 1.5, -2, null]}}}'))",
            "NULL"
        };

        std::string query = R"(
            UPSERT INTO TestTable (k, v) VALUES
        )";

        for (size_t i = 0; i < values.size(); ++i) {
            query += std::format("({}, {}),", i + 1, (values[i] == "NULL" ? "" : jsonType) + values[i]);
        }

        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    {
        auto query = R"(
            ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (v)
        )";
        auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }

    // Any json value existence
    {
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$"));
    }

    // Keys existence (with empty keys)
    {
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}", 1)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}", 2)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}", 3)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}", 4)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}", 5)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}", 6)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}", 7)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}", 8)));

        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k1.k{}", 1)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k1.k{}", 2)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k1.k{}", 3)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k1.k{}", 4)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k1.k{}", 5)));

        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 1, 1)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 1, 2)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 1, 3)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 1, 4)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 1, 5)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 2, 1)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 2, 2)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 2, 3)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 2, 4)));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists(std::format("$.k{}.k{}", 2, 5)));

        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.\"\""));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.\"\".\"\""));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.\"\".\"\".\"\""));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.\"\".\"\".\"\".\"\""));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.\"\".\"\".\"\".\"\".\"\""));

        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.*"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1.*"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k2.*"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1.k1.*"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1.*.k1"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1.*.*"));
    }

    // Array elements existence
    {
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[0]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[0, 3]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[1 to 3]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[last]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[*]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[0].k1"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[0, 3].k1"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[1 to 3].k1"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[last].k1"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[*].k1"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[0].*"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$[*].*"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1[0]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1[0, 3]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1[1 to 3]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1[last]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1[0 to last]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.k1[*]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.*[0]"));
        ValidatePredicate(db, "TestTable", "json_idx", jsonExists("$.*[*]"));
    }
}

}

} // namespace NKikimr::NKqp
