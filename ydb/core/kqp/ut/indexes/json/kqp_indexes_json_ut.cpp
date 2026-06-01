#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_ut_common.h>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

Y_UNIT_TEST_SUITE(KqpJsonIndexes) {
    Y_UNIT_TEST(AddJsonIndexJson) {
        TestAddJsonIndex("Json", true);
    }

    Y_UNIT_TEST(AddJsonIndexJsonDocument) {
        TestAddJsonIndex("JsonDocument", true);
    }

    Y_UNIT_TEST(AddJsonIndexJsonNotNull) {
        TestAddJsonIndex("Json", false);
    }

    Y_UNIT_TEST(AddJsonIndexJsonDocumentNotNull) {
        TestAddJsonIndex("JsonDocument", false);
    }

    Y_UNIT_TEST(CoverColumnsNotAllowed) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE TestTable (
                    Key Uint64,
                    Text Json,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text) COVER (Data)
                );
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "JSON index does not support COVER columns");
        }

        CreateTestTable(db, "Json");

        {
            const std::string query = R"(
                ALTER TABLE TestTable ADD INDEX json_idx
                    GLOBAL USING json ON (Text) COVER (Data)
            )";

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "JSON index does not support COVER columns");
        }

        {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.GetSession().GetValueSync().GetSession();

            auto desc = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("Key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("Text", NYdb::EPrimitiveType::Json)
                .AddNullableColumn("Data", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("Key")
                .AddSecondaryIndex("json_idx", NYdb::NTable::EIndexType::GlobalJson, {"Text"}, {"Data"})
                .Build();

            auto result = session.CreateTable("/Root/TestTableSdkCover", std::move(desc)).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "JSON index does not support COVER columns");
        }

        {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.GetSession().GetValueSync().GetSession();

            NYdb::NTable::TAlterTableSettings alterSettings;
            alterSettings.AppendAddIndexes(NYdb::NTable::TIndexDescription(
                "json_idx_sdk",
                NYdb::NTable::EIndexType::GlobalJson,
                {"Text"},
                {"Data"}
            ));

            auto result = session.AlterTable("/Root/TestTable", alterSettings).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::BAD_REQUEST, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "JSON index does not support COVER columns");
        }
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
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "JSON index requires exactly one key column, but 2 are requested");
        }
    }

    Y_UNIT_TEST(NonIntegerPk) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Utf8,
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
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "Error: JSON index requires primary key column 'Key' to be of type 'Uint64', 'Int64', 'Uint32' or 'Int32' but got Utf8");
        }
    }

    Y_UNIT_TEST(AlterTableJsonIndex_PK_Int32) {
        TestJsonIndexAlterTableWithIntegerPk("Int32");
    }

    Y_UNIT_TEST(AlterTableJsonIndex_PK_Uint32) {
        TestJsonIndexAlterTableWithIntegerPk("Uint32");
    }

    Y_UNIT_TEST(AlterTableJsonIndex_PK_Int64) {
        TestJsonIndexAlterTableWithIntegerPk("Int64");
    }

    Y_UNIT_TEST(AlterTableJsonIndex_PK_Uint64) {
        TestJsonIndexAlterTableWithIntegerPk("Uint64");
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
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "Error: JSON index requires exactly one primary key column of type 'Uint64', 'Int64', 'Uint32' or 'Int32', but table has 2 primary key columns");
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

        {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.GetSession().GetValueSync().GetSession();

            NYdb::NTable::TAlterTableSettings alterSettings;
            alterSettings.AppendAddIndexes(NYdb::NTable::TIndexDescription(
                "json_idx_sdk",
                NYdb::NTable::EIndexType::GlobalJson,
                {"Text"},
                {"Data"}
            ));

            auto result = session.AlterTable("/Root/TestTable", alterSettings).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "JSON index support is disabled");
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

        {
            auto tableClient = kikimr.GetTableClient();
            auto session = tableClient.GetSession().GetValueSync().GetSession();

            auto desc = NYdb::NTable::TTableBuilder()
                .AddNullableColumn("Key", NYdb::EPrimitiveType::Uint64)
                .AddNullableColumn("Text", NYdb::EPrimitiveType::Json)
                .AddNullableColumn("Data", NYdb::EPrimitiveType::Utf8)
                .SetPrimaryKeyColumn("Key")
                .AddSecondaryIndex("json_idx", NYdb::NTable::EIndexType::GlobalJson, {"Text"}, {"Data"})
                .Build();

            auto result = session.CreateTable("/Root/TestTable", std::move(desc)).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "JSON index support is disabled");
        }
    }

    Y_UNIT_TEST(CreateOlap) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true);
        auto db = kikimr.GetQueryClient();

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64 NOT NULL,
                    Text Json,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX `json_idx` GLOBAL USING json ON (Text)
                ) WITH (
                    STORE = COLUMN
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
        }
    }

    Y_UNIT_TEST(AlterOlap) {
        auto kikimr = Kikimr(/* enableJsonIndex */ true);
        auto db = kikimr.GetQueryClient();

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64 NOT NULL,
                    Text Json,
                    Data Utf8,
                    PRIMARY KEY (Key),
                ) WITH (
                    STORE = COLUMN
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
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
                    R"(SELECT Key FROM TestTable VIEW PRIMARY KEY WHERE JSON_VALUE(Text, '$.k1') == "1"u ORDER BY Key;)",
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

    Y_UNIT_TEST(SelectJsonIndex_Top) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            static constexpr const char* where = R"(JSON_EXISTS(Text, '$.k1'))";
            auto empty = TParamsBuilder().Build();

            FillDataColumn(db);

            ValidatePredicate(db, where, empty, "LIMIT 0");
            ValidatePredicate(db, where, empty, "LIMIT 1");
            ValidatePredicate(db, where, empty, "LIMIT 2");
            ValidatePredicate(db, where, empty, "LIMIT 10");
            ValidatePredicate(db, where, empty, "LIMIT 100000");
            ValidatePredicate(db, where, empty, "LIMIT -1");

            ValidatePredicate(db, where, empty, "LIMIT 0 OFFSET 5");
            ValidatePredicate(db, where, empty, "LIMIT 1 OFFSET 5");
            ValidatePredicate(db, where, empty, "LIMIT 2 OFFSET 5");
            ValidatePredicate(db, where, empty, "LIMIT 3 OFFSET 5");
            ValidatePredicate(db, where, empty, "LIMIT 10 OFFSET 5");
            ValidatePredicate(db, where, empty, "LIMIT 100000 OFFSET 5");
            ValidatePredicate(db, where, empty, "LIMIT -1 OFFSET 5");

            {
                ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') LIMIT 5)");

                const std::string query = R"(
                    SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.k1') LIMIT 5;
                )";

                auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 5);
            }

            {
                ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') LIMIT 5 OFFSET 3)");

                const std::string query = R"(
                    SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.k1') LIMIT 5 OFFSET 3;
                )";

                auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 5);
            }
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(SelectJsonIndex_TopSort) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            static constexpr const char* where = R"(JSON_EXISTS(Text, '$.k1'))";
            auto empty = TParamsBuilder().Build();

            FillDataColumn(db);

            ValidatePredicate(db, where, empty, "ORDER BY Data ASC");
            ValidatePredicate(db, where, empty, "ORDER BY Data DESC");
            ValidatePredicate(db, where, empty, "ORDER BY Data ASC LIMIT 5");
            ValidatePredicate(db, where, empty, "ORDER BY Data DESC LIMIT 5");
            ValidatePredicate(db, where, empty, "ORDER BY Data ASC LIMIT 5 OFFSET 3");
            ValidatePredicate(db, where, empty, "ORDER BY Data DESC LIMIT 5 OFFSET 3");

            ValidatePredicate(db, where, empty, "ORDER BY Key ASC");
            ValidatePredicate(db, where, empty, "ORDER BY Key DESC");
            ValidatePredicate(db, where, empty, "ORDER BY Key ASC LIMIT 5");
            ValidatePredicate(db, where, empty, "ORDER BY Key DESC LIMIT 5");
            ValidatePredicate(db, where, empty, "ORDER BY Key ASC LIMIT 5 OFFSET 3");
            ValidatePredicate(db, where, empty, "ORDER BY Key DESC LIMIT 5 OFFSET 3");

            {
                ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') ORDER BY Data LIMIT 5)");

                const std::string query = R"(
                    SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.k1') ORDER BY Data LIMIT 5;
                )";

                auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 5);
            }

            {
                ValidateAutoSelect(db, R"(JSON_EXISTS(Text, '$.k1') ORDER BY Data LIMIT 5 OFFSET 3)");

                const std::string query = R"(
                    SELECT Key FROM TestTable WHERE JSON_EXISTS(Text, '$.k1') ORDER BY Data LIMIT 5 OFFSET 3;
                )";

                auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
                UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 5);
            }
        }, /* enableJsonIndexAutoSelect */ true);
    }

    Y_UNIT_TEST(TruncateTable) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableJsonIndex(true);
        featureFlags.SetEnableTruncateTable(true);

        auto kikimr = TKikimrRunner(TKikimrSettings().SetFeatureFlags(featureFlags));
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, "Json", /* withIndex */ true);

        auto upsertData = [&]() {
            const TString query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                    (1, '{"a":1}', "data1"),
                    (2, '{"b":"hello"}', "data2"),
                    (3, '"scalar"', "data3");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        };

        auto ensureMainTableEmpty = [&]() {
            auto result = db.ExecuteQuery("SELECT * FROM `/Root/TestTable`;", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 0);
        };

        auto ensureIndexEmpty = [&]() {
            auto index = ReadIndex(db);
            UNIT_ASSERT_VALUES_EQUAL(index.RowsCount(), 0);
        };

        auto ensureIndexNonEmpty = [&]() {
            auto index = ReadIndex(db);
            UNIT_ASSERT_GT(index.RowsCount(), 0);
        };

        upsertData();
        ensureIndexNonEmpty();

        for (size_t i = 0; i < 3; ++i) {
            auto result = db.ExecuteQuery("TRUNCATE TABLE `/Root/TestTable`;", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

            ensureMainTableEmpty();
            ensureIndexEmpty();

            upsertData();
            ensureIndexNonEmpty();
        }
    }

    Y_UNIT_TEST(SqlIn_List_Literal) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // List<String?>
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN ['1', '2']");
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN [Just('1'), '2']");
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN [Just('1'), Just('2')]");

            // List<String?>?
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN Just(['1', '2'])");
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN Just([Just('1'), Just('2')])");

            // AsList[Strict]
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN AsList('1', '2')");
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN AsListStrict('1', '2')");
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN Just(AsList('1', '2'))");
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN Just(AsListStrict('1', '2'))");
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN Just(AsList(Just('1'), Just('2')))");
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN Just(AsListStrict(Just('1'), Just('2')))");

            // Empty list -> always false, index not applicable
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN ListCreate(String)");
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN Just(ListCreate(String))");

            // NULL in list -> negation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN [Just('1'), NULL]");
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN [Just('1'), Nothing(Optional<String>)]");

            // Parameters
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN [$p1, $p2]",
                TParamsBuilder()
                    .AddParam("$p1").String("1").Build()
                    .AddParam("$p2").String("2").Build()
                    .Build());

            // Optional parameters -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN [$p1, $p2]",
                TParamsBuilder()
                    .AddParam("$p1").OptionalString("1").Build()
                    .AddParam("$p2").EmptyOptional(TTypeBuilder().Primitive(EPrimitiveType::String).Build()).Build()
                    .Build());

            // Elements longer than 16 bytes
            ValidatePredicate(db,
                std::format("JSON_VALUE(Text, '$.k1' RETURNING String) IN ['{}', '{}']", kFirstLongSqlInValue, kSecondLongSqlInValue));
        });
    }

    Y_UNIT_TEST(SqlIn_List_Parameter) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // List<String>
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p1",
                TParamsBuilder()
                    .AddParam("$p1")
                        .BeginList()
                            .AddListItem().String("1")
                            .AddListItem().String("2")
                            .EndList()
                        .Build()
                    .Build());

            // List<String?> -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p2",
                TParamsBuilder()
                    .AddParam("$p2")
                        .BeginList()
                            .AddListItem().OptionalString("1")
                            .AddListItem().OptionalString("2")
                            .EndList()
                        .Build()
                    .Build());

            // List<String>? -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p3",
                TParamsBuilder()
                    .AddParam("$p3")
                        .BeginOptional()
                            .BeginList()
                                .AddListItem().String("1")
                                .AddListItem().String("2")
                                .EndList()
                            .EndOptional()
                        .Build()
                    .Build());

            // List<String?>? -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p4",
                TParamsBuilder()
                    .AddParam("$p4")
                        .BeginOptional()
                            .BeginList()
                                .AddListItem().OptionalString("1")
                                .AddListItem().OptionalString("2")
                                .EndList()
                            .EndOptional()
                        .Build()
                    .Build());

            // Empty list
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p5",
                TParamsBuilder()
                    .AddParam("$p5")
                        .EmptyList(TTypeBuilder().Primitive(EPrimitiveType::String).Build())
                        .Build()
                    .Build());

            // List parameter with elements longer than 16 bytes
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p6",
                TParamsBuilder()
                    .AddParam("$p6")
                        .BeginList()
                            .AddListItem().String(kFirstLongSqlInValue)
                            .AddListItem().String(kSecondLongSqlInValue)
                            .EndList()
                        .Build()
                    .Build());
        });
    }

    Y_UNIT_TEST(SqlIn_Tuple_Literal) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Tuple<Int32, Int32>
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, 2))");

            // Tuple<Int32?, Int32?>
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (Just(1), Just(2)))");

            // Tuple<Int32?, Int32?>?
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN Just((Just(1), Just(2))))");

            // AsTuple
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsTuple(1, 2))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsTuple(Just(1), Just(2)))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN Just(AsTuple(Just(1), Just(2))))");

            // Different integers
            ValidatePredicate(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsTuple(1t, 2s, 3, 4l, 5u, 6.0f, 7.0))");

            // NULL in tuple -> negation
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (1, NULL))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsTuple(1, NULL))");
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsTuple(1, Nothing(Optional<Int32>)))");

            // Elements longer than 16 bytes
            ValidatePredicate(db,
                std::format(R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN ('{}', '{}'))", kFirstLongSqlInValue, kSecondLongSqlInValue));
        });
    }

    // Tuple<Int32, Int64, Float, Double>
    Y_UNIT_TEST(SqlIn_Tuple_Parameter) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Tuple<String, String>
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p1",
                TParamsBuilder()
                    .AddParam("$p1")
                        .BeginTuple()
                            .AddElement().String("1")
                            .AddElement().String("2")
                        .EndTuple()
                        .Build()
                    .Build());

            // Tuple<Int32, Int32>
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING Int32) IN $p1",
                TParamsBuilder()
                    .AddParam("$p1")
                        .BeginTuple()
                            .AddElement().Int32(1)
                            .AddElement().Int32(2)
                        .EndTuple()
                        .Build()
                    .Build());

            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING Int32) IN $p2",
                TParamsBuilder()
                    .AddParam("$p2")
                        .BeginTuple()
                            .AddElement().Int32(1)
                            .AddElement().Int64(2)
                            .AddElement().Float(3.0f)
                            .AddElement().Double(4.0)
                        .EndTuple()
                        .Build()
                    .Build());

            // Tuple<String?, String?> -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p3",
                TParamsBuilder()
                    .AddParam("$p3")
                        .BeginTuple()
                            .AddElement().OptionalString("1")
                            .AddElement().OptionalString("2")
                        .EndTuple()
                        .Build()
                    .Build());

            // Tuple<String, String>? -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p4",
                TParamsBuilder()
                    .AddParam("$p4")
                        .BeginOptional()
                            .BeginTuple()
                                .AddElement().String("1")
                                .AddElement().String("2")
                            .EndTuple()
                        .EndOptional()
                        .Build()
                    .Build());

            // Tuple<String?, String?>? -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p5",
                TParamsBuilder()
                    .AddParam("$p5")
                        .BeginOptional()
                            .BeginTuple()
                                .AddElement().OptionalString("1")
                                .AddElement().OptionalString("2")
                            .EndTuple()
                        .EndOptional()
                        .Build()
                    .Build());

            // Tuple parameter with elements longer than 16 bytes
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p6",
                TParamsBuilder()
                    .AddParam("$p6")
                        .BeginTuple()
                            .AddElement().String(kFirstLongSqlInValue)
                            .AddElement().String(kSecondLongSqlInValue)
                        .EndTuple()
                        .Build()
                    .Build());
        });
    }

    Y_UNIT_TEST(SqlIn_Dict_Literal) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Dict<String, Int32>
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN {'1': 10, '2': 20})");

            // Dict<Int32, String>
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN {1: 'a', 2: 'b'})");

            // Dict<Int32?, String> -> optional literal keys are OK
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN {Just(1): 'a', Just(2): 'b'})");

            // Just(Dict<...>) -> outer optional unwraps
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN Just({1: 'a', 2: 'b'}))");

            // AsDict
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsDict(AsTuple(1, 'a'), AsTuple(2, 'b')))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsDictStrict(AsTuple(1, 'a'), AsTuple(2, 'b')))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsDict(AsTuple(Just(1), 'a'), AsTuple(Just(2), 'b')))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN Just(AsDict(AsTuple(Just(1), 'a'), AsTuple(Just(2), 'b'))))");

            // Different integer key types
            ValidatePredicate(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsDict(AsTuple(1t, 'a'), AsTuple(2s, 'b'), AsTuple(3, 'c'), AsTuple(4l, 'd')))");

            // Empty dict -> always false, index not applicable
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN DictCreate(String, String)");
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN Just(DictCreate(String, String))");

            // NULL key in dict -> negation
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsDict(AsTuple(1, 'a'), AsTuple(Nothing(Optional<Int32>), 'b')))");

            // NULL value in dict is allowed (we only care about keys)
            ValidatePredicate(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsDict(AsTuple(1, Just('a')), AsTuple(2, Nothing(Optional<String>))))");

            // Parameters as keys
            ValidatePredicate(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN AsDict(AsTuple($p1, 'a'), AsTuple($p2, 'b')))",
                TParamsBuilder()
                    .AddParam("$p1").String("1").Build()
                    .AddParam("$p2").String("2").Build()
                    .Build());

            // Optional parameters as keys -> cannot check nulls during compilation
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN AsDict(AsTuple($p1, 'a'), AsTuple($p2, 'b')))",
                TParamsBuilder()
                    .AddParam("$p1").OptionalString("1").Build()
                    .AddParam("$p2").EmptyOptional(TTypeBuilder().Primitive(EPrimitiveType::String).Build()).Build()
                    .Build());

            // Keys longer than 16 bytes
            ValidatePredicate(db,
                std::format(R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN {{'{}': 1, '{}': 2}})", kFirstLongSqlInValue, kSecondLongSqlInValue));
        });
    }

    Y_UNIT_TEST(SqlIn_Dict_Parameter) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Dict<String, Int32>
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p1",
                TParamsBuilder()
                    .AddParam("$p1")
                        .BeginDict()
                            .AddDictItem().DictKey().String("1").DictPayload().Int32(10)
                            .AddDictItem().DictKey().String("2").DictPayload().Int32(20)
                        .EndDict()
                        .Build()
                    .Build());

            // Dict<Int32, String>
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING Int32) IN $p1",
                TParamsBuilder()
                    .AddParam("$p1")
                        .BeginDict()
                            .AddDictItem().DictKey().Int32(1).DictPayload().String("a")
                            .AddDictItem().DictKey().Int32(2).DictPayload().String("b")
                        .EndDict()
                        .Build()
                    .Build());

            // Dict<String?, Int32> -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p2",
                TParamsBuilder()
                    .AddParam("$p2")
                        .BeginDict()
                            .AddDictItem().DictKey().OptionalString("1").DictPayload().Int32(10)
                            .AddDictItem().DictKey().OptionalString("2").DictPayload().Int32(20)
                        .EndDict()
                        .Build()
                    .Build());

            // Dict<String, Int32>? -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p3",
                TParamsBuilder()
                    .AddParam("$p3")
                        .BeginOptional()
                            .BeginDict()
                                .AddDictItem().DictKey().String("1").DictPayload().Int32(10)
                                .AddDictItem().DictKey().String("2").DictPayload().Int32(20)
                            .EndDict()
                        .EndOptional()
                        .Build()
                    .Build());

            // Dict<String?, Int32>? -> cannot check nulls during compilation
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p4",
                TParamsBuilder()
                    .AddParam("$p4")
                        .BeginOptional()
                            .BeginDict()
                                .AddDictItem().DictKey().OptionalString("1").DictPayload().Int32(10)
                                .AddDictItem().DictKey().OptionalString("2").DictPayload().Int32(20)
                            .EndDict()
                        .EndOptional()
                        .Build()
                    .Build());

            // Empty dict
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p5",
                TParamsBuilder()
                    .AddParam("$p5")
                        .EmptyDict(
                            TTypeBuilder().Primitive(EPrimitiveType::String).Build(),
                            TTypeBuilder().Primitive(EPrimitiveType::Int32).Build())
                        .Build()
                    .Build());

            // Dict parameter with keys longer than 16 bytes
            ValidatePredicate(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN $p6",
                TParamsBuilder()
                    .AddParam("$p6")
                        .BeginDict()
                            .AddDictItem().DictKey().String(kFirstLongSqlInValue).DictPayload().Int32(10)
                            .AddDictItem().DictKey().String(kSecondLongSqlInValue).DictPayload().Int32(20)
                        .EndDict()
                        .Build()
                    .Build());
        });
    }

    Y_UNIT_TEST(SqlIn_Set_Literal) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Set<String> via {} syntax
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN {'1', '2'})");

            // Set<Int32>
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN {1, 2})");

            // Set<Int32?> -> optional literal keys are OK
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN {Just(1), Just(2)})");

            // Just(Set<...>) -> outer optional unwraps
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN Just({1, 2}))");

            // AsSet
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsSet(1, 2))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsSetStrict(1, 2))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsSet(Just(1), Just(2)))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN Just(AsSet(1, 2)))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN Just(AsSet(Just(1), Just(2))))");

            // Empty set -> always false, index not applicable
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN SetCreate(String)");
            ValidateError(db, "JSON_VALUE(Text, '$.k1' RETURNING String) IN Just(SetCreate(String))");

            // NULL in set -> negation
            ValidateError(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsSet(1, NULL))");
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN AsSet(Just(1), Nothing(Optional<Int32>)))");

            // Parameters as keys
            ValidatePredicate(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN AsSet($p1, $p2))",
                TParamsBuilder()
                    .AddParam("$p1").String("1").Build()
                    .AddParam("$p2").String("2").Build()
                    .Build());

            // Optional parameters as keys -> cannot check nulls during compilation
            ValidateError(db,
                R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN AsSet($p1, $p2))",
                TParamsBuilder()
                    .AddParam("$p1").OptionalString("1").Build()
                    .AddParam("$p2").EmptyOptional(TTypeBuilder().Primitive(EPrimitiveType::String).Build()).Build()
                    .Build());

            // Elements longer than 16 bytes
            ValidatePredicate(db,
                std::format(R"(JSON_VALUE(Text, '$.k1' RETURNING String) IN {{'{}', '{}'}})", kFirstLongSqlInValue, kSecondLongSqlInValue));
        });
    }

    Y_UNIT_TEST(SafeCast) {
        TestSelectJsonWithIndex("JsonDocument", std::nullopt, [](TQueryClient& db, const auto&) {
            // Supported literal casts
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == CAST(10 AS Int32))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) IN (CAST(7 AS Int32), 8))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.key' RETURNING String) == CAST(10 AS String))");
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == CAST(2.5f AS Int32))");

            // Supported parameter casts
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == CAST($p AS Int32))",
                TParamsBuilder().AddParam("$p").Int32(10).Build().Build());
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int64) IN (CAST($p AS Int64), CAST($q AS Int32)))",
                TParamsBuilder()
                    .AddParam("$p").Int64(10).Build()
                    .AddParam("$q").Int32(20).Build()
                    .Build());

            // Unsupported parameter casts
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Int32) == CAST($p AS Int32))",
                TParamsBuilder().AddParam("$p").Double(2.5).Build().Build());
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$.k1' RETURNING Utf8) IN (CAST($p AS Utf8), "x"))",
                TParamsBuilder().AddParam("$p").Int32(10).Build().Build());
        });
    }

    Y_UNIT_TEST(ShowCreateTable) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableJsonIndex(true);

        auto kikimr = TKikimrRunner(TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetEnableShowCreate(true));

        auto db = kikimr.GetQueryClient();
        auto session = db.GetSession().GetValueSync().GetSession();

        {
            auto result = session.ExecuteQuery(R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text Json,
                    Data JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx_2 GLOBAL USING json ON (Data);
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto result = session.ExecuteQuery(R"(
                SHOW CREATE TABLE `/Root/TestTable`;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT(!result.GetResultSets().empty());

            auto yson = FormatResultSetYson(result.GetResultSet(0));
            UNIT_ASSERT_STRING_CONTAINS_C(yson, "INDEX `json_idx` GLOBAL USING json ON (`Text`)", yson);
            UNIT_ASSERT_STRING_CONTAINS_C(yson, "INDEX `json_idx_2` GLOBAL USING json ON (`Data`)", yson);
        }
    }

    Y_UNIT_TEST_TWIN(CyrillicIndexImplTable, IsJsonDocument) {
        const auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";

        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, jsonType);

        {
            const auto query = std::format(R"(
                UPSERT INTO TestTable (Key, Text) VALUES (1, {0}({1}));
            )", jsonType, R"('{"ключ": "я mop"}')");

            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = R"(
                ALTER TABLE TestTable ADD INDEX json_idx GLOBAL USING json ON (Text);
            )";

            const auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        CompareYson(R"([
            [[1u];""];
            [[1u];"\x09ключ"];
            [[1u];"\x09ключ\0\3я mop"]
        ])", FormatResultSetYson(ReadIndex(db)));
    }

    Y_UNIT_TEST_TWIN(CyrillicPredicates, IsJsonDocument) {
        const std::string jsonType = IsJsonDocument ? "JsonDocument" : "Json";

        TestSelectJsonWithIndex(jsonType, std::nullopt, [&](TQueryClient& db, const auto&) {
            {
                const auto query = std::format(R"(
                    UPSERT INTO TestTable (Key, Text) VALUES
                        (100, {0}({1})),
                        (101, {0}({2})),
                        (102, {0}({3})),
                        (103, {0}({4}));
                )", jsonType, R"('{"ключ": "Я моп"}')", R"('{"другой ключ": "в стойло!"}')", R"('{"ключ": "Я empty"}')", "'{}'");

                auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
                UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            }

            // JE: Cyrillic key in jsonpath
            ValidatePredicate(db, R"(JSON_EXISTS(Text, '$."ключ"'))");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$."ключ"'))", {"\x09ключ"});

            // JE: Cyrillic key with Cyrillic string value in equality filter
            ValidatePredicate(db, R"(JSON_EXISTS(Text, '$ ? (@."ключ" == "Я моп")'))");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@."ключ" == "Я моп")'))", {std::string("\x09ключ") + strSuffix("Я моп")});

            ValidatePredicate(db, R"(JSON_EXISTS(Text, '$ ? (@."ключ" starts with "Я")'))");
            ValidateTokens(db, R"(JSON_EXISTS(Text, '$ ? (@."ключ" starts with "Я")'))", {"\x09ключ"});

            // JV: Cyrillic key compared to Cyrillic Utf8 literal
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$."ключ"' RETURNING Utf8) == "Я моп"u)");
            ValidateTokens(db, R"(JSON_VALUE(Text, '$."ключ"' RETURNING Utf8) == "Я моп"u)", {std::string("\x09ключ") + strSuffix("Я моп")});

            // JV: Cyrillic key compared to external Utf8 parameter
            auto cyrParam = TParamsBuilder().AddParam("$p").Utf8("я").Build().Build();
            ValidatePredicate(db, R"(JSON_VALUE(Text, '$."ключ"' RETURNING Utf8) == $p)", cyrParam);
            ValidateTokens(db, R"(JSON_VALUE(Text, '$."ключ"' RETURNING Utf8) == $p)", {NJsonIndex::TToken{"\x09ключ", "$p"}}, cyrParam);
        });
    }
}

}  // namespace NKikimr::NKqp
