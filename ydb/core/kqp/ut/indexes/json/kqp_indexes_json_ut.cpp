#include <ydb/core/kqp/ut/indexes/json/common/kqp_indexes_json_ut_common.h>
#include <ydb/core/kqp/ut/indexes/common/kqp_indexes_ttl_ut_common.h>
#include <ydb/core/tx/datashard/const.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/proto/accessor.h>

namespace NKikimr::NKqp {

using namespace NYdb::NQuery;
using namespace NYdb;

namespace {

// A runner for the __ydb_row_id opt-in: JSON indexes plus the unique-index feature, so a JSON index
// over a non-single-integer primary key can use __ydb_row_id as its doc_id and resolve it back to the
// primary key through a unique secondary index (the mechanism shared with fulltext indexes).
TKikimrRunner KikimrJsonRowId() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(true);
    featureFlags.SetEnableAddUniqueIndex(true);
    featureFlags.SetEnableFulltextIndexRowId(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    return TKikimrRunner(settings);
}

// Same as KikimrJsonRowId() plus the compact-index flag, so a JSON index is materialized as a compact
// (delta/posting) index. Combined with __ydb_row_id this exercises the compact-JSON-in-rowid-mode path
// (EIndexTypeGlobalJsonCompact), which must obtain its doc_id from __ydb_row_id just like plain JSON.
TKikimrRunner KikimrJsonRowIdCompact() {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(true);
    featureFlags.SetEnableAddUniqueIndex(true);
    featureFlags.SetEnableFulltextIndexRowId(true);
    featureFlags.SetEnableCompactFulltextIndex(true);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    return TKikimrRunner(settings);
}

TKikimrRunner KikimrJsonPrefixRowId(bool compact) {
    NKikimrConfig::TFeatureFlags featureFlags;
    featureFlags.SetEnableJsonIndex(true);
    featureFlags.SetEnableAddUniqueIndex(true);
    featureFlags.SetEnableFulltextIndexRowId(true);
    featureFlags.SetEnableFulltextIndexPrefix(true);
    featureFlags.SetEnableCompactFulltextIndex(compact);
    auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
    settings.AppConfig.MutableTableServiceConfig()->SetBackportMode(NKikimrConfig::TTableServiceConfig_EBackportMode_All);
    if (compact) {
        settings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
    }
    return TKikimrRunner(settings);
}

void ExecuteJsonStatement(TQueryClient& db, const TString& sql, TParams params = TParamsBuilder().Build()) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
}

TString SelectJsonRows(TQueryClient& db, const TString& sql, TParams params = TParamsBuilder().Build()) {
    auto result = db.ExecuteQuery(sql, TTxControl::NoTx(), params).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
    return FormatResultSetYson(result.GetResultSet(0));
}

TString JsonLiteralToken(TStringBuf json) {
    TString error;
    auto tokens = NJsonIndex::TokenizeJson(json, error);
    UNIT_ASSERT_C(error.empty(), error);
    UNIT_ASSERT_C(!tokens.empty(), "JSON produced no index tokens");
    return tokens.back();
}

TString FormatUint32Keys(TVector<ui32> keys) {
    Sort(keys);
    TStringBuilder yson;
    yson << '[';
    for (size_t i = 0; i < keys.size(); ++i) {
        yson << (i ? ";" : "") << "[[" << keys[i] << "u]]";
    }
    yson << ']';
    return yson;
}

TString MakeScalarJson(TStringBuf marker, size_t scalarSize) {
    return TStringBuilder() << R"({"marker":")" << marker
        << R"(","payload":")" << TString(scalarSize, 'x') << R"("})";
}

TString MakeWhitespaceJson(TStringBuf marker, size_t totalSize) {
    const TString prefix = TStringBuilder() << R"({"marker":")" << marker << '"';
    UNIT_ASSERT_C(totalSize > prefix.size() + 1, totalSize);
    return prefix + TString(totalSize - prefix.size() - 1, ' ') + '}';
}

} // namespace

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

    Y_UNIT_TEST(NoMultipleColumnsWithoutFeatureFlag) {
        // With the prefix feature flag off, multi-column JSON indexes are rejected.
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableJsonIndex(true);
        featureFlags.SetEnableFulltextIndexPrefix(false);
        featureFlags.SetEnableJsonIndexAutoSelect(false);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        auto kikimr = TKikimrRunner(settings);
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
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Prefixed fulltext/json index support is disabled");
        }
    }

    Y_UNIT_TEST(NonIntegerPk) {
        // With the unique-index feature OFF, a JSON index over a non-integer PK can neither
        // use the PK as doc_id nor auto-provision the __ydb_row_id infrastructure, so it is rejected.
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableJsonIndex(true);
        featureFlags.SetEnableAddUniqueIndex(false);
        featureFlags.SetEnableFulltextIndexRowId(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        auto kikimr = TKikimrRunner(settings);
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
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "requires the unique-index feature");
        }
    }

    Y_UNIT_TEST(NonIntegerPkRowId) {
        // End-to-end: a table keyed by a non-integer (Utf8) PK plus a __ydb_row_id Uint64 NOT NULL column
        // and a unique secondary index on __ydb_row_id supports a JSON index. The JSON index uses
        // __ydb_row_id as doc_id; the runtime resolves __ydb_row_id -> PK before reading the main table.
        // Mirrors SelectWithFulltextMatch_RowIdOptIn_Plain in kqp_fulltext_search_ut.cpp.
        auto kikimr = KikimrJsonRowId();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Utf8 NOT NULL,
                    Text Json,
                    Data Utf8,
                    __ydb_row_id Uint64 NOT NULL,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            // __ydb_row_id is generated by YDB; create the JSON index first so its sequence is
            // provisioned, then insert without naming the system column.
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                    ("a"u, Json('{"k1": 1}'),  "d1"u),
                    ("b"u, Json('{"k1": 2}'),  "d2"u),
                    ("c"u, Json('{"k2": 3}'),  "d3"u),
                    ("d"u, Json('{"k1": 10}'), "d4"u);
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            // Row "d" has k1 == 10; doc_id -> PK resolution must return its Utf8 key.
            std::string query = R"(
                SELECT Key FROM `/Root/TestTable` VIEW json_idx
                WHERE JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10
                ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        }

        {
            std::string query = R"(
                SELECT Key FROM `/Root/TestTable` VIEW json_idx
                WHERE JSON_EXISTS(Text, '$.k2')
                ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        }
    }

    Y_UNIT_TEST(NonIntegerPkRowIdCompact) {
        // Same as NonIntegerPkRowId but with the compact-index flag on, so the JSON index is built as a
        // compact index (EIndexTypeGlobalJsonCompact). The compact type must still enter __ydb_row_id
        // doc_id mode over a non-integer PK; before the fix it fell back to requiring a single integer PK
        // and the ADD INDEX below was rejected.
        auto kikimr = KikimrJsonRowIdCompact();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Utf8 NOT NULL,
                    Text Json,
                    Data Utf8,
                    __ydb_row_id Uint64 NOT NULL,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                    ("a"u, Json('{"k1": 1}'),  "d1"u),
                    ("b"u, Json('{"k1": 2}'),  "d2"u),
                    ("c"u, Json('{"k2": 3}'),  "d3"u),
                    ("d"u, Json('{"k1": 10}'), "d4"u);
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            // Row "d" has k1 == 10; doc_id -> PK resolution must return its Utf8 key.
            std::string query = R"(
                SELECT Key FROM `/Root/TestTable` VIEW json_idx
                WHERE JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10
                ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        }

        {
            std::string query = R"(
                SELECT Key FROM `/Root/TestTable` VIEW json_idx
                WHERE JSON_EXISTS(Text, '$.k2')
                ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        }
    }

    Y_UNIT_TEST(CreateTableInlineCompactRowId) {
        // Inline CREATE TABLE with a compact JSON index over a non-integer PK: the KQP DDL layer emits the
        // compact index type (EnableCompactFulltextIndex), and the schemeshard create-table path must
        // auto-provision __ydb_row_id + its unique index and build the compact index in rowid mode.
        auto kikimr = KikimrJsonRowIdCompact();
        auto db = kikimr.GetQueryClient();

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Utf8 NOT NULL,
                    Text Json,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Text) VALUES
                    ("a"u, Json('{"k1": 1}')),
                    ("b"u, Json('{"k1": 2}')),
                    ("c"u, Json('{"k2": 3}')),
                    ("d"u, Json('{"k1": 10}'));
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                SELECT Key FROM `/Root/TestTable` VIEW json_idx
                WHERE JSON_VALUE(Text, '$.k1' RETURNING Int64) == 10
                ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        }

        {
            std::string query = R"(
                SELECT Key FROM `/Root/TestTable` VIEW json_idx
                WHERE JSON_EXISTS(Text, '$.k2')
                ORDER BY Key;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
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
        // With the unique-index feature OFF, a composite-PK table cannot host a JSON index.
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableJsonIndex(true);
        featureFlags.SetEnableAddUniqueIndex(false);
        featureFlags.SetEnableFulltextIndexRowId(true);
        auto settings = TKikimrSettings().SetFeatureFlags(featureFlags);
        auto kikimr = TKikimrRunner(settings);
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
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "requires the unique-index feature");
        }
    }

    Y_UNIT_TEST(CompositePkRowId) {
        // A composite-PK table with an explicit __ydb_row_id Uint64 NOT NULL column and a unique index on
        // it supports a JSON index that resolves the synthetic doc_id back to the (Key1, Key2) primary key.
        auto kikimr = KikimrJsonRowId();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key1 Uint64 NOT NULL,
                    Key2 Uint64 NOT NULL,
                    Text Json,
                    __ydb_row_id Uint64 NOT NULL,
                    PRIMARY KEY (Key1, Key2)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            // __ydb_row_id is generated by YDB; create the JSON index first so its sequence is
            // provisioned, then insert without naming the system column.
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                UPSERT INTO `/Root/TestTable` (Key1, Key2, Text) VALUES
                    (1, 1, Json('{"k1": 1}')),
                    (1, 2, Json('{"k1": 2}')),
                    (2, 1, Json('{"k2": 3}')),
                    (2, 2, Json('{"k1": 10}'));
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            // Three rows have k1; doc_id -> (Key1, Key2) resolution must return all of them.
            std::string query = R"(
                SELECT Key1, Key2 FROM `/Root/TestTable` VIEW json_idx
                WHERE JSON_EXISTS(Text, '$.k1')
                ORDER BY Key1, Key2;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 3);
        }
    }

    Y_UNIT_TEST(CompositePkRowIdCompact) {
        // Same as CompositePkRowId but with the compact-index flag on: a composite-PK table hosts a compact
        // JSON index (EIndexTypeGlobalJsonCompact) that resolves the synthetic __ydb_row_id doc_id back to
        // the (Key1, Key2) primary key.
        auto kikimr = KikimrJsonRowIdCompact();
        auto db = kikimr.GetQueryClient();

        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::BUILD_INDEX, NActors::NLog::PRI_TRACE);
        kikimr.GetTestServer().GetRuntime()->SetLogPriority(NKikimrServices::FLAT_TX_SCHEMESHARD, NActors::NLog::PRI_TRACE);

        {
            std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key1 Uint64 NOT NULL,
                    Key2 Uint64 NOT NULL,
                    Text Json,
                    __ydb_row_id Uint64 NOT NULL,
                    PRIMARY KEY (Key1, Key2)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
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
                UPSERT INTO `/Root/TestTable` (Key1, Key2, Text) VALUES
                    (1, 1, Json('{"k1": 1}')),
                    (1, 2, Json('{"k1": 2}')),
                    (2, 1, Json('{"k2": 3}')),
                    (2, 2, Json('{"k1": 10}'));
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            // Three rows have k1; doc_id -> (Key1, Key2) resolution must return all of them.
            std::string query = R"(
                SELECT Key1, Key2 FROM `/Root/TestTable` VIEW json_idx
                WHERE JSON_EXISTS(Text, '$.k1')
                ORDER BY Key1, Key2;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 3);
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
                    CompareYsonUnordered(R"([
                        [["data 1"];[1u];["{\"k1\":[\"v1\",1,false]}"]];
                        [["data 2"];[2u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[3u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 4"];[4u];["{\"k4\":[\"v4\",4,true]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
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
            CompareYsonUnordered(R"([
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatFulltextIndex(kikimr));
        }

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "UPSERT", "TestTable", jsonType, {{1, 3}, {3, 2}, {5, 5}}, WithReturning);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYsonUnordered(R"([
                        [["data 2"];[3u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[1u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 5"];[5u];["{\"k5\":[\"v5\",5,false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
                        [["data 2"];[3u];["{\"k2\": [\"v2\", 2, true]}"]];
                        [["data 3"];[1u];["{\"k3\": [\"v3\", 3, false]}"]];
                        [["data 5"];[5u];["{\"k5\": [\"v5\", 5, false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                }
            }
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\3k3"];
                [[1u];"\3k3\0\0"];
                [[1u];"\3k3\0\3v3"];
                [[1u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k2"];
                [[3u];"\3k2\0\1"];
                [[3u];"\3k2\0\3v2"];
                [[3u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"];
                [[5u];"\3k5"];
                [[5u];"\3k5\0\0"];
                [[5u];"\3k5\0\3v5"];
                [[5u];"\3k5\0\4\0\0\0\0\0\0\x14@"]
            ])", FormatFulltextIndex(kikimr));
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
                    CompareYsonUnordered(R"([
                        [["data 1"];[1u];["{\"k1\":[\"v1\",1,false]}"]];
                        [["data 2"];[2u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[3u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 4"];[4u];["{\"k4\":[\"v4\",4,true]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
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
            CompareYsonUnordered(R"([
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatFulltextIndex(kikimr));
        }

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "REPLACE", "TestTable", jsonType, {{1, 3}, {3, 2}, {5, 5}}, WithReturning);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYsonUnordered(R"([
                        [["data 2"];[3u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[1u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 5"];[5u];["{\"k5\":[\"v5\",5,false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
                        [["data 2"];[3u];["{\"k2\": [\"v2\", 2, true]}"]];
                        [["data 3"];[1u];["{\"k3\": [\"v3\", 3, false]}"]];
                        [["data 5"];[5u];["{\"k5\": [\"v5\", 5, false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                }
            }
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\3k3"];
                [[1u];"\3k3\0\0"];
                [[1u];"\3k3\0\3v3"];
                [[1u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k2"];
                [[3u];"\3k2\0\1"];
                [[3u];"\3k2\0\3v2"];
                [[3u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"];
                [[5u];"\3k5"];
                [[5u];"\3k5\0\0"];
                [[5u];"\3k5\0\3v5"];
                [[5u];"\3k5\0\4\0\0\0\0\0\0\x14@"]
            ])", FormatFulltextIndex(kikimr));
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
                    CompareYsonUnordered(R"([
                        [["data 1"];[1u];["{\"k1\":[\"v1\",1,false]}"]];
                        [["data 2"];[2u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[3u];["{\"k3\":[\"v3\",3,false]}"]];
                        [["data 4"];[4u];["{\"k4\":[\"v4\",4,true]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
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
            CompareYsonUnordered(R"([
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatFulltextIndex(kikimr));
        }

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "INSERT", "TestTable", jsonType, {{5, 3}, {6, 2}}, WithReturning);
            UNIT_ASSERT_C(writeResult.IsSuccess(), writeResult.GetIssues().ToString());

            if (WithReturning) {
                if (IsJsonDocument) {
                    CompareYsonUnordered(R"([
                        [["data 2"];[6u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 3"];[5u];["{\"k3\":[\"v3\",3,false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
                        [["data 2"];[6u];["{\"k2\": [\"v2\", 2, true]}"]];
                        [["data 3"];[5u];["{\"k3\": [\"v3\", 3, false]}"]]
                    ])", FormatResultSetYson(writeResult.GetResultSet(0)));
                }
            }
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"];
                [[5u];"\3k3"];
                [[5u];"\3k3\0\0"];
                [[5u];"\3k3\0\3v3"];
                [[5u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[6u];"\3k2"];
                [[6u];"\3k2\0\1"];
                [[6u];"\3k2\0\3v2"];
                [[6u];"\3k2\0\4\0\0\0\0\0\0\0@"]
            ])", FormatFulltextIndex(kikimr));
        }

        {
            auto writeResult = WriteJsonIndexWithKeys(db, "INSERT", "TestTable", jsonType, {{1, 1}, {7, 7}}, WithReturning);
            UNIT_ASSERT_C(!writeResult.IsSuccess(), writeResult.GetIssues().ToString());
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[5u];"\3k3"];
                [[5u];"\3k3\0\0"];
                [[5u];"\3k3\0\3v3"];
                [[5u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[6u];"\3k2\0\1"];
                [[6u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[6u];"\3k2"];
                [[6u];"\3k2\0\3v2"]
            ])", FormatFulltextIndex(kikimr));
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
            CompareYsonUnordered(R"([
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];"\3k2"];
                [[2u];"\3k2\0\1"];
                [[2u];"\3k2\0\3v2"];
                [[2u];"\3k2\0\4\0\0\0\0\0\0\0@"];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatFulltextIndex(kikimr));
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
                    CompareYsonUnordered(R"([
                        [["data 10"];[2u];["{\"k10\":[\"v10\",10,true]}"]];
                        [["data 10"];[3u];["{\"k10\":[\"v10\",10,true]}"]]
                    ])", FormatResultSetYson(updateResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
                        [["data 10"];[2u];["{\"k10\": [\"v10\", 10, true]}"]];
                        [["data 10"];[3u];["{\"k10\": [\"v10\", 10, true]}"]]
                    ])", FormatResultSetYson(updateResult.GetResultSet(0)));
                }
            }
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];"\4k10"];
                [[2u];"\4k10\0\1"];
                [[2u];"\4k10\0\3v10"];
                [[2u];"\4k10\0\4\0\0\0\0\0\0$@"];
                [[3u];"\4k10"];
                [[3u];"\4k10\0\1"];
                [[3u];"\4k10\0\3v10"];
                [[3u];"\4k10\0\4\0\0\0\0\0\0$@"];
                [[4u];"\3k4"];
                [[4u];"\3k4\0\1"];
                [[4u];"\3k4\0\3v4"];
                [[4u];"\3k4\0\4\0\0\0\0\0\0\x10@"]
            ])", FormatFulltextIndex(kikimr));
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
                    CompareYsonUnordered(R"([
                        [["data 100"];[1u];["{\"k100\":[\"v100\",100,false]}"]];
                        [["data 100"];[2u];["{\"k100\":[\"v100\",100,false]}"]];
                        [["data 100"];[3u];["{\"k100\":[\"v100\",100,false]}"]];
                        [["data 100"];[4u];["{\"k100\":[\"v100\",100,false]}"]]
                    ])", FormatResultSetYson(updateResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
                        [["data 100"];[1u];["{\"k100\": [\"v100\", 100, false]}"]];
                        [["data 100"];[2u];["{\"k100\": [\"v100\", 100, false]}"]];
                        [["data 100"];[3u];["{\"k100\": [\"v100\", 100, false]}"]];
                        [["data 100"];[4u];["{\"k100\": [\"v100\", 100, false]}"]]
                    ])", FormatResultSetYson(updateResult.GetResultSet(0)));
                }
            }
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\5k100"];
                [[1u];"\5k100\0\0"];
                [[1u];"\5k100\0\3v100"];
                [[1u];"\5k100\0\4\0\0\0\0\0\0Y@"];
                [[2u];"\5k100"];
                [[2u];"\5k100\0\0"];
                [[2u];"\5k100\0\3v100"];
                [[2u];"\5k100\0\4\0\0\0\0\0\0Y@"];
                [[3u];"\5k100"];
                [[3u];"\5k100\0\0"];
                [[3u];"\5k100\0\3v100"];
                [[3u];"\5k100\0\4\0\0\0\0\0\0Y@"];
                [[4u];"\5k100"];
                [[4u];"\5k100\0\0"];
                [[4u];"\5k100\0\3v100"];
                [[4u];"\5k100\0\4\0\0\0\0\0\0Y@"]
            ])", FormatFulltextIndex(kikimr));
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
                    CompareYsonUnordered(R"([
                        [["data 2"];[2u];["{\"k2\":[\"v2\",2,true]}"]];
                        [["data 4"];[4u];["{\"k4\":[\"v4\",4,true]}"]]
                    ])", FormatResultSetYson(deleteResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
                        [["data 2"];[2u];["{\"k2\": [\"v2\", 2, true]}"]];
                        [["data 4"];[4u];["{\"k4\": [\"v4\", 4, true]}"]]
                    ])", FormatResultSetYson(deleteResult.GetResultSet(0)));
                }
            }
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\3k1"];
                [[1u];"\3k1\0\0"];
                [[1u];"\3k1\0\3v1"];
                [[1u];"\3k1\0\4\0\0\0\0\0\0\xF0?"];
                [[3u];"\3k3"];
                [[3u];"\3k3\0\0"];
                [[3u];"\3k3\0\3v3"];
                [[3u];"\3k3\0\4\0\0\0\0\0\0\x08@"]
            ])", FormatFulltextIndex(kikimr));
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
                    CompareYsonUnordered(R"([
                        [["data 1"];[1u];["{\"k1\":[\"v1\",1,false]}"]];
                        [["data 3"];[3u];["{\"k3\":[\"v3\",3,false]}"]]
                    ])", FormatResultSetYson(deleteResult.GetResultSet(0)));
                } else {
                    CompareYsonUnordered(R"([
                        [["data 1"];[1u];["{\"k1\": [\"v1\", 1, false]}"]];
                        [["data 3"];[3u];["{\"k3\": [\"v3\", 3, false]}"]]
                    ])", FormatResultSetYson(deleteResult.GetResultSet(0)));
                }
            }
        }

        {
            CompareYsonUnordered("[]", FormatFulltextIndex(kikimr));
        }
    }

    Y_UNIT_TEST_QUAD(SelectJsonExists_ContextObject, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidateError(db, jsonExists("$"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
        });
    }

    Y_UNIT_TEST_QUAD(ExecutionStatisticsMatchActualSelectivity, IsJsonDocument, Compact) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableJsonIndex(true);
        featureFlags.SetEnableCompactFulltextIndex(Compact);
        auto runnerSettings = TKikimrSettings().SetFeatureFlags(featureFlags);
        if (Compact) {
            runnerSettings.AppConfig.MutableTableServiceConfig()->SetEnableIndexStreamWrite(true);
        }
        auto kikimr = TKikimrRunner(runnerSettings);
        auto db = kikimr.GetQueryClient();
        const std::string jsonType = IsJsonDocument ? "JsonDocument" : "Json";

        CreateTestTable(db, jsonType, /* withIndex */ true);

        {
            TStringBuilder query;
            query << "UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES\n";
            for (ui64 key = 1; key <= 1000; ++key) {
                const char* json = key <= 10
                    ? R"({"segment":"rare","tracked":true})"
                    : key <= 100
                        ? R"({"segment":"common","tracked":true})"
                        : R"({"noise":0})";
                query << "(" << key << ", " << jsonType << "('" << json << "'), \"row_" << key << "\")";
                query << (key == 1000 ? ";" : ",\n");
            }

            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        const auto settings = TExecuteQuerySettings().StatsMode(EStatsMode::Basic);
        const auto hasTableAccess = [](const TExecuteQueryResult& result, TStringBuf table) {
            const auto& stats = TProtoAccessor::GetProto(*result.GetStats());
            for (const auto& phase : stats.query_phases()) {
                for (const auto& access : phase.table_access()) {
                    if (access.name() == table) {
                        return true;
                    }
                }
            }
            return false;
        };
        const auto execute = [&](const std::string& view, const std::string& predicate) {
            const auto query = std::format(R"(
                SELECT Key, Data FROM `/Root/TestTable` VIEW {}
                WHERE {}
                ORDER BY Key;
            )", view, predicate);
            auto result = db.ExecuteQuery(query, TTxControl::NoTx(), settings).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), "Predicate: " + predicate + ", issues: " + result.GetIssues().ToString());
            UNIT_ASSERT_C(result.GetStats(), "Execution statistics are missing for: " + predicate);
            return result;
        };

        const auto validate = [&](const std::string& predicate, ui64 expectedMatches, ui64 expectedIndexReads) {
            const auto scanResult = execute("PRIMARY KEY", predicate);
            const auto indexResult = execute("`json_idx`", predicate);

            UNIT_ASSERT_VALUES_EQUAL_C(scanResult.GetResultSet(0).RowsCount(), expectedMatches, predicate);
            CompareYson(FormatResultSetYson(scanResult.GetResultSet(0)),
                FormatResultSetYson(indexResult.GetResultSet(0)), TString(predicate));

            AssertTableStats(scanResult, "/Root/TestTable", {
                .ExpectedReads = 1000,
            });
            AssertTableStats(indexResult, "/Root/TestTable", {
                .ExpectedReads = expectedMatches,
            });
            AssertTableStats(indexResult, "/Root/TestTable/json_idx/indexImplTable", {
                .ExpectedReads = expectedIndexReads,
            });
            UNIT_ASSERT_C(hasTableAccess(indexResult, "/Root/TestTable/json_idx/indexImplTable"),
                "Execution statistics have no physical JSON index table access for: " + predicate);
        };

        // Plain indexes read one posting row per matched document. Compact indexes read one
        // segment row for the searched token, while the main-table reads still reflect matches
        validate(R"(JSON_VALUE(Text, '$.segment' RETURNING Utf8) == "rare"u)", 10, Compact ? 1 : 10);
        validate(R"(JSON_EXISTS(Text, '$.tracked'))", 100, Compact ? 1 : 100);
        validate(R"(JSON_EXISTS(Text, '$.missing'))", 0, 0);
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

            ValidateError(db, jsonExists("$.*"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidatePredicate(db, jsonExists("$.k1.*"));
            ValidatePredicate(db, jsonExists("$.k2.*"));
            ValidatePredicate(db, jsonExists("$.k1.k1.*"));
            ValidatePredicate(db, jsonExists("$.k1.*.k1"));
            ValidatePredicate(db, jsonExists("$.k1.*.*"));
        });
    }

    Y_UNIT_TEST_QUAD(SelectJsonExists_ArrayAccess, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            ValidateError(db, jsonExists("$[0]"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$[0, 3]"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$[1 to 3]"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$[last]"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$[*]"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$[0][0][0]"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidatePredicate(db, jsonExists("$[0].k1"));
            ValidatePredicate(db, jsonExists("$[0, 3].k1"));
            ValidatePredicate(db, jsonExists("$[1 to 3].k1"));
            ValidatePredicate(db, jsonExists("$[last].k1"));
            ValidatePredicate(db, jsonExists("$[*].k1"));
            ValidateError(db, jsonExists("$[0].*"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$[*].*"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidatePredicate(db, jsonExists("$.k1[0]"));
            ValidatePredicate(db, jsonExists("$.k1[0, 3]"));
            ValidatePredicate(db, jsonExists("$.k1[1 to 3]"));
            ValidatePredicate(db, jsonExists("$.k1[last]"));
            ValidatePredicate(db, jsonExists("$.k1[0 to last]"));
            ValidatePredicate(db, jsonExists("$.k1[*]"));
            ValidateError(db, jsonExists("$.*[0]"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$.*[*]"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
        });
    }

    Y_UNIT_TEST_QUAD(SelectJsonExists_Methods, IsJsonDocument, IsStrict) {
        TestSelectJsonWithIndex(IsJsonDocument ? "JsonDocument" : "Json", std::make_optional(IsStrict), [](TQueryClient& db, const auto& jsonExists) {
            auto validateMethod = [&](const std::string& method) {
                ValidateError(db, jsonExists(std::format("$.{}", method)),
                    "JSON index cannot be used: full-range search cannot be performed using full-text search");
                ValidatePredicate(db, jsonExists(std::format("$.k1.{}", method)));
                ValidateError(db, jsonExists(std::format("$.*.{}", method)),
                    "JSON index cannot be used: full-range search cannot be performed using full-text search");
                ValidateError(db, jsonExists(std::format("$[0].{}", method)),
                    "JSON index cannot be used: full-range search cannot be performed using full-text search");
                ValidateError(db, jsonExists(std::format("$[*].{}", method)),
                    "JSON index cannot be used: full-range search cannot be performed using full-text search");
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

            ValidateError(db, jsonExists("$.* ? (@ == 1)"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$.* ? (@ == \"1\")"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$.* ? (@ == true)"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$.* ? (@ == null)"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");
            ValidateError(db, jsonExists("$.* ? (@ == 42)"),
                "JSON index cannot be used: full-range search cannot be performed using full-text search");

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
            auto index = FormatFulltextIndex(kikimr);
            UNIT_ASSERT_VALUES_EQUAL(index, "[]");
        };

        auto ensureIndexNonEmpty = [&]() {
            auto index = FormatFulltextIndex(kikimr);
            UNIT_ASSERT(index != "[]");
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
            .SetFeatureFlags(featureFlags));

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

        CompareYsonUnordered(R"([
            [[1u];"\x09ключ"];
            [[1u];"\x09ключ\0\3я mop"]
        ])", FormatFulltextIndex(kikimr));
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

    Y_UNIT_TEST(DmlDuringBuild) {
        NKikimrConfig::TFeatureFlags featureFlags;
        featureFlags.SetEnableJsonIndex(true);

        auto kikimr = TKikimrRunner(TKikimrSettings()
            .SetFeatureFlags(featureFlags)
            .SetUseRealThreads(false));

        auto db = kikimr.GetQueryClient();
        auto* runtime = kikimr.GetTestServer().GetRuntime();

        kikimr.RunCall([&] {
            CreateTestTable(db, "Json");

            auto result = db.ExecuteQuery(R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                    (1, '{"a": 1}', 'row1'),
                    (2, '{"b": 2}', 'row2'),
                    (3, '{"c": 3}', 'row3');
            )", TTxControl::NoTx()).ExtractValueSync();

            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return true;
        });

        TVector<TAutoPtr<IEventHandle>> capturedEvents;
        int captured = 0;

        runtime->SetObserverFunc([&](TAutoPtr<IEventHandle>& ev) -> NActors::TTestActorRuntimeBase::EEventAction {
            if (captured < 1 && ev->GetTypeRewrite() == TEvDataShard::TEvBuildFulltextIndexRequest::EventType) {
                captured++;
                capturedEvents.push_back(ev.Release());
                return NActors::TTestActorRuntimeBase::EEventAction::DROP;
            }

            return NActors::TTestActorRuntimeBase::EEventAction::PROCESS;
        });

        NYdb::NQuery::TAsyncExecuteQueryResult addIndexFuture;
        kikimr.RunCall([&] {
            addIndexFuture = db.ExecuteQuery(R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )", TTxControl::NoTx());
            return true;
        });

        runtime->WaitFor("index build paused", [&] { return captured >= 1; });

        kikimr.RunCall([&] {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES (4, '{"a": 4}', 'row4');
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return true;
        });

        kikimr.RunCall([&] {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/TestTable` VIEW json_idx WHERE JSON_EXISTS(Text, '$.a');
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Requested index: json_idx is not ready to use");
            return true;
        });

        kikimr.RunCall([&] {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/TestTable` WHERE JSON_EXISTS(Text, '$.a') ORDER BY Key;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[1u]];[[4u]]])", FormatResultSetYson(result.GetResultSet(0)));
            return true;
        });

        for (auto& ev : capturedEvents) {
            runtime->Send(ev.Release());
        }

        capturedEvents.clear();
        runtime->SetObserverFunc(TTestActorRuntime::DefaultObserverFunc);

        kikimr.RunCall([&] {
            auto result = addIndexFuture.GetValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return true;
        });

        kikimr.RunCall([&] {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES (5, '{"a": 5}', 'row5');
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            return true;
        });

        kikimr.RunCall([&] {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/TestTable` VIEW json_idx WHERE JSON_EXISTS(Text, '$.a') ORDER BY Key;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            CompareYson(R"([[[1u]];[[4u]];[[5u]]])", FormatResultSetYson(result.GetResultSet(0)));
            return true;
        });
    }

    // Overwriting a JSON value with NULL removes all index tokens for that row.
    Y_UNIT_TEST_TWIN(NullUpdate_JsonToNull, IsJsonDocument) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();
        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";
        CreateTestTable(db, jsonType, /* withIndex */ true);

        {
            const auto query = std::format(R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                    (1, {}('{{\"a\": 1}}'), "data1"),
                    (2, {}('{{\"b\": 2}}'), "data2");
            )", jsonType, jsonType);
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\2a"];
                [[1u];"\2a\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];"\2b"];
                [[2u];"\2b\0\4\0\0\0\0\0\0\0@"]
            ])", FormatFulltextIndex(kikimr));
        }

        {
            const auto query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Text) VALUES (1, NULL);
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Key 1 tokens must be gone; key 2 is unchanged.
        {
            CompareYsonUnordered(R"([
                [[2u];"\2b"];
                [[2u];"\2b\0\4\0\0\0\0\0\0\0@"]
            ])", FormatFulltextIndex(kikimr));
        }
    }

    // Overwriting a NULL value with JSON adds the new tokens to the index.
    Y_UNIT_TEST_TWIN(NullUpdate_NullToJson, IsJsonDocument) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();
        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";
        CreateTestTable(db, jsonType, /* withIndex */ true);

        {
            const auto query = std::format(R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                    (1, NULL, "null_data"),
                    (2, {}('{{\"b\": 2}}'), "data2");
            )", jsonType);
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Only key 2 has tokens; key 1 (NULL) has none.
        {
            CompareYsonUnordered(R"([
                [[2u];"\2b"];
                [[2u];"\2b\0\4\0\0\0\0\0\0\0@"]
            ])", FormatFulltextIndex(kikimr));
        }

        {
            const auto query = std::format(R"(
                UPSERT INTO `/Root/TestTable` (Key, Text) VALUES (1, {}('{{\"a\": 1}}'));
            )", jsonType);
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Now both keys have tokens.
        {
            CompareYsonUnordered(R"([
                [[1u];"\2a"];
                [[1u];"\2a\0\4\0\0\0\0\0\0\xF0?"];
                [[2u];"\2b"];
                [[2u];"\2b\0\4\0\0\0\0\0\0\0@"]
            ])", FormatFulltextIndex(kikimr));
        }
    }

    // Inserting a row with NULL Text produces no entries in the index.
    Y_UNIT_TEST_TWIN(NullInsert_NoTokens, IsJsonDocument) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();
        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";
        CreateTestTable(db, jsonType, /* withIndex */ true);

        {
            const auto query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                    (1, NULL, "null_data1"),
                    (2, NULL, "null_data2");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            CompareYsonUnordered("[]", FormatFulltextIndex(kikimr));
        }
    }

    // Deleting a NULL row leaves the index unchanged (still empty).
    Y_UNIT_TEST_TWIN(NullDelete_IndexUnchanged, IsJsonDocument) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();
        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";
        CreateTestTable(db, jsonType, /* withIndex */ true);

        {
            const auto query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES (1, NULL, "null_data");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            CompareYsonUnordered("[]", FormatFulltextIndex(kikimr));
        }

        {
            const auto query = R"(DELETE FROM `/Root/TestTable` WHERE Key = 1;)";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            CompareYsonUnordered("[]", FormatFulltextIndex(kikimr));
        }
    }

    // Delete a JSON row then insert NULL for the same key: tokens are removed and none are added.
    Y_UNIT_TEST_TWIN(NullInsert_AfterJsonDelete, IsJsonDocument) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();
        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";
        CreateTestTable(db, jsonType, /* withIndex */ true);

        {
            const auto query = std::format(R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES
                    (1, {}('{{\"a\": 1}}'), "data1");
            )", jsonType);
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\2a"];
                [[1u];"\2a\0\4\0\0\0\0\0\0\xF0?"]
            ])", FormatFulltextIndex(kikimr));
        }

        {
            const auto query = R"(DELETE FROM `/Root/TestTable` WHERE Key = 1;)";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            CompareYsonUnordered("[]", FormatFulltextIndex(kikimr));
        }

        // Insert NULL for the same key.
        {
            const auto query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Text) VALUES (1, NULL);
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        // Index must stay empty
        {
            CompareYsonUnordered("[]", FormatFulltextIndex(kikimr));
        }
    }

    // Delete a NULL row then insert JSON for the same key: tokens appear in the index.
    Y_UNIT_TEST_TWIN(NullDelete_BeforeJsonInsert, IsJsonDocument) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();
        auto jsonType = IsJsonDocument ? "JsonDocument" : "Json";
        CreateTestTable(db, jsonType, /* withIndex */ true);

        {
            const auto query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES (1, NULL, "null_data");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            CompareYsonUnordered("[]", FormatFulltextIndex(kikimr));
        }

        {
            const auto query = R"(DELETE FROM `/Root/TestTable` WHERE Key = 1;)";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const auto query = std::format(R"(
                UPSERT INTO `/Root/TestTable` (Key, Text, Data) VALUES (1, {}('{{\"a\": 1}}'), "data1");
            )", jsonType);
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            CompareYsonUnordered(R"([
                [[1u];"\2a"];
                [[1u];"\2a\0\4\0\0\0\0\0\0\xF0?"]
            ])", FormatFulltextIndex(kikimr));
        }
    }

    Y_UNIT_TEST(ChangeSchema_DropColumn) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, "Json", /* withIndex */ true);

        {
            const std::string query = R"(
                ALTER TABLE `/Root/TestTable` DROP COLUMN Text
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Impossible drop column because table has an index with that column");
        }
    }

    Y_UNIT_TEST(ChangeSchema_SetDefault) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, "Json", /* withIndex */ true);

        {
            const std::string query = R"(
                ALTER TABLE `/Root/TestTable` ALTER COLUMN Text SET DEFAULT Json('{"default": true}')
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Data) VALUES (1, "data1");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT Text FROM `/Root/TestTable` WHERE Key = 1;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto yson = FormatResultSetYson(result.GetResultSet(0));
            UNIT_ASSERT_STRING_CONTAINS(yson, "default");
        }
    }

    Y_UNIT_TEST(ChangeSchema_DropDefault) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text Json DEFAULT Json('{"default": true}'),
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                ALTER TABLE `/Root/TestTable` ALTER COLUMN Text DROP DEFAULT
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Data) VALUES (1, "data1");
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT Text FROM `/Root/TestTable` WHERE Key = 1;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto yson = FormatResultSetYson(result.GetResultSet(0));
            UNIT_ASSERT_STRING_CONTAINS(yson, "#");
        }
    }

    Y_UNIT_TEST(ChangeSchema_DropNotNull) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        {
            const std::string query = R"(
                CREATE TABLE `/Root/TestTable` (
                    Key Uint64,
                    Text Json NOT NULL,
                    Data Utf8,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                ALTER TABLE `/Root/TestTable` ALTER COLUMN Text DROP NOT NULL
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                UPSERT INTO `/Root/TestTable` (Key, Data, Text) VALUES (1, "data1", NULL);
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            const std::string query = R"(
                SELECT Text FROM `/Root/TestTable` WHERE Key = 1;
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
            auto yson = FormatResultSetYson(result.GetResultSet(0));
            UNIT_ASSERT_STRING_CONTAINS(yson, "#");
        }
    }

    Y_UNIT_TEST(BulkUpsert) {
        auto kikimr = Kikimr();
        auto db = kikimr.GetQueryClient();

        CreateTestTable(db, "Json");

        {
            std::string query = R"(
                ALTER TABLE `/Root/TestTable` ADD INDEX json_idx GLOBAL USING json ON (Text)
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }

        {
            auto tableClient = kikimr.GetTableClient();

            TValueBuilder rows;
            rows.BeginList()
                .BeginStruct()
                    .AddMember("Key").Uint64(1)
                    .AddMember("Text").Json(R"({"k1": ["v1", 1, false]})")
                    .AddMember("Data").Utf8("data 1")
                .EndStruct()
            .EndList();

            auto result = tableClient.BulkUpsert("/Root/TestTable", rows.Build()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SCHEME_ERROR, result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Only async-indexed tables are supported by BulkUpsert");
        }
    }

    TTtlNotAllowedIndexTestConfig MakeJsonTtlNotAllowedConfig(TKikimrRunner& kikimr) {
        const bool compact = kikimr.GetTestServer().GetRuntime()->GetAppData(0).FeatureFlags.GetEnableCompactFulltextIndex();
        const char* enumType = compact ? "EIndexTypeGlobalJsonCompact" : "EIndexTypeGlobalJson";
        return {
            .TextColumnType = "Json",
            .IndexInCreateTable = "INDEX json_idx GLOBAL USING json ON (Text),",
            .AlterAddIndex = R"(
                ALTER TABLE TestTable ADD INDEX json_idx
                    GLOBAL USING json ON (Text);
            )",
            .ExpectedError = std::format("Table with {} index doesn't support TTL", enumType),
        };
    }

    Y_UNIT_TEST(TtlNotAllowed_Both) {
        auto kikimr = Kikimr();
        TestTtlNotAllowedBoth(kikimr.GetQueryClient(), MakeJsonTtlNotAllowedConfig(kikimr));
    }

    Y_UNIT_TEST(TtlNotAllowed_AlterTtl) {
        auto kikimr = Kikimr();
        TestTtlNotAllowedAlterTtl(kikimr.GetQueryClient(), MakeJsonTtlNotAllowedConfig(kikimr));
    }

    Y_UNIT_TEST(TtlNotAllowed_AlterIndex) {
        auto kikimr = Kikimr();
        TestTtlNotAllowedAlterIndex(kikimr.GetQueryClient(), MakeJsonTtlNotAllowedConfig(kikimr));
    }

    Y_UNIT_TEST(TtlNotAllowed_AlterTtlIndex) {
        auto kikimr = Kikimr();
        TestTtlNotAllowedAlterTtlIndex(kikimr.GetQueryClient(), MakeJsonTtlNotAllowedConfig(kikimr));
    }

    Y_UNIT_TEST(TtlNotAllowed_AlterIndexTtl) {
        auto kikimr = Kikimr();
        TestTtlNotAllowedAlterIndexTtl(kikimr.GetQueryClient(), MakeJsonTtlNotAllowedConfig(kikimr));
    }

    Y_UNIT_TEST_TWIN(MultiShardHighFanoutBuildAndDml, Compact) {
        const auto oldMaxDelta = NDataShard::gFulltextMaxDelta;
        const auto oldMaxSegment = NDataShard::gFulltextMaxSegment;
        Y_DEFER {
            NDataShard::gFulltextMaxDelta = oldMaxDelta;
            NDataShard::gFulltextMaxSegment = oldMaxSegment;
        };
        if (Compact) {
            NDataShard::gFulltextMaxDelta = 2;
            NDataShard::gFulltextMaxSegment = 2;
        }

        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ false, Compact);
        auto db = kikimr.GetQueryClient();

        ExecuteJsonStatement(db, R"(
            CREATE TABLE `/Root/Docs` (
                Key Uint32,
                Text JsonDocument,
                Data Utf8,
                PRIMARY KEY (Key)
            ) WITH (
                AUTO_PARTITIONING_BY_SIZE = DISABLED,
                AUTO_PARTITIONING_BY_LOAD = DISABLED,
                UNIFORM_PARTITIONS = 4
            );
        )");

        TStringBuilder upsert;
        upsert << "UPSERT INTO `/Root/Docs` (Key, Text, Data) VALUES\n";
        TVector<ui32> allKeys;
        TVector<ui32> evenGroupKeys;
        for (ui32 i = 1; i <= 128; ++i) {
            const ui32 key = i * 2654435761u;
            const ui32 group = i % 4;
            allKeys.push_back(key);
            if (group == 0) {
                evenGroupKeys.push_back(key);
            }
            upsert << "(" << key << "u, JsonDocument('{\"common\":\"all\",\"group\":\"g"
                   << group << "\",\"tags\":[\"repeat\",\"repeat\"]}'), \"row_" << i << "\"u)"
                   << (i == 128 ? ";" : ",\n");
        }
        ExecuteJsonStatement(db, upsert);
        ExecuteJsonStatement(db, R"(
            ALTER TABLE `/Root/Docs` ADD INDEX json_idx
                GLOBAL USING json ON (Text);
        )");

        auto runtime = kikimr.GetTestServer().GetRuntime();
        auto shards = GetTableShards(&kikimr.GetTestServer(), runtime->AllocateEdgeActor(), "/Root/Docs");
        UNIT_ASSERT_VALUES_EQUAL(shards.size(), 4);

        Sort(allKeys);
        Sort(evenGroupKeys);
        const auto compareViews = [&](const TString& predicate, const TVector<ui32>& expected) {
            for (const TStringBuf view : {TStringBuf("PRIMARY KEY"), TStringBuf("json_idx")}) {
                TStringBuilder query;
                query << "SELECT Key FROM `/Root/Docs` VIEW " << view << '\n'
                      << "WHERE " << predicate << '\n'
                      << "ORDER BY Key;";
                CompareYson(FormatUint32Keys(expected), SelectJsonRows(db, query));
            }
        };

        const TString commonPredicate = R"(JSON_VALUE(Text, '$.common' RETURNING Utf8) = "all"u)";
        const TString groupPredicate = R"(JSON_VALUE(Text, '$.group' RETURNING Utf8) = "g0"u)";
        const TString repeatPredicate = R"(JSON_EXISTS(Text, '$.tags ? (@ == "repeat")'))";
        compareViews(commonPredicate, allKeys);
        compareViews(groupPredicate, evenGroupKeys);
        compareViews(repeatPredicate, allKeys);

        ExecuteJsonStatement(db, R"(
            INSERT INTO `/Root/Docs` (Key, Text, Data) VALUES
                (268435456u,
                 JsonDocument('{"common":"all","group":"g0","tags":["repeat","repeat"]}'),
                 "inserted"u);
        )");
        ExecuteJsonStatement(db, R"(
            UPDATE `/Root/Docs`
            SET Text = JsonDocument('{"common":"all","group":"changed","tags":["repeat","repeat"]}')
            WHERE Key = 2027808452u;
        )");
        ExecuteJsonStatement(db, R"(
            DELETE FROM `/Root/Docs` WHERE Key = 3668339987u;
        )");

        const ui32 insertedKey = 0x10000000u;
        const ui32 updatedKey = 4u * 2654435761u;
        const ui32 deletedKey = 3u * 2654435761u;
        const auto eraseKey = [](TVector<ui32>& keys, ui32 key) {
            const auto it = Find(keys, key);
            UNIT_ASSERT_C(it != keys.end(), "Expected key is missing");
            keys.erase(it);
        };
        eraseKey(allKeys, deletedKey);
        allKeys.push_back(insertedKey);
        eraseKey(evenGroupKeys, updatedKey);
        evenGroupKeys.push_back(insertedKey);
        Sort(allKeys);
        Sort(evenGroupKeys);

        compareViews(commonPredicate, allKeys);
        compareViews(groupPredicate, evenGroupKeys);
        compareViews(repeatPredicate, allKeys);

        const TString commonToken = JsonLiteralToken(R"({"common":"all"})");
        auto params = TParamsBuilder()
            .AddParam("$token").String(commonToken).Build()
            .Build();
        auto result = db.ExecuteQuery(R"(
            DECLARE $token AS String;
            SELECT COUNT(*) AS Rows
            FROM `/Root/Docs/json_idx/indexImplTable`
            WHERE __ydb_token = $token;
        )", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        TResultSetParser parser(result.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        const ui64 physicalRows = parser.ColumnParser("Rows").GetUint64();
        UNIT_ASSERT(!parser.TryNextRow());
        if (Compact) {
            UNIT_ASSERT_C(physicalRows > 1, "Compact posting table did not split the common token");
        } else {
            UNIT_ASSERT_VALUES_EQUAL(physicalRows, allKeys.size());
        }
    }

    Y_UNIT_TEST_TWIN(TextJsonDuplicateKeys, Compact) {
        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ true, Compact);
        auto db = kikimr.GetQueryClient();

        ExecuteJsonStatement(db, R"(
            CREATE TABLE `/Root/DuplicateDocs` (
                Key Uint64,
                Text Json,
                PRIMARY KEY (Key),
                INDEX json_idx GLOBAL USING json ON (Text)
            );
        )");

        const TVector<std::pair<ui64, TString>> rows = {
            {1, R"({ "dup" : "same", "dup" : "same", "nested" : { "n" : 1, "n" : 1 } })"},
            {2, R"({"dup":"first","dup":"second"})"},
            {3, R"({"dup":1,"dup":"one"})"},
            {4, R"({"other":true})"},
        };
        for (const auto& [key, text] : rows) {
            auto params = TParamsBuilder()
                .AddParam("$key").Uint64(key).Build()
                .AddParam("$text").Json(text).Build()
                .Build();
            ExecuteJsonStatement(db, R"(
                DECLARE $key AS Uint64;
                DECLARE $text AS Json;
                UPSERT INTO `/Root/DuplicateDocs` (Key, Text) VALUES ($key, $text);
            )", params);
        }

        auto textResult = db.ExecuteQuery(R"(
            SELECT Text FROM `/Root/DuplicateDocs` VIEW PRIMARY KEY WHERE Key = 1;
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(textResult.GetStatus(), EStatus::SUCCESS, textResult.GetIssues().ToString());
        TResultSetParser parser(textResult.GetResultSet(0));
        UNIT_ASSERT(parser.TryNextRow());
        UNIT_ASSERT_VALUES_EQUAL(parser.ColumnParser("Text").GetOptionalJson().value(), rows[0].second);
        UNIT_ASSERT(!parser.TryNextRow());

        const auto assertViews = [&](const TString& predicate, const TString& expected) {
            TStringBuilder primaryQuery;
            primaryQuery << "SELECT Key FROM `/Root/DuplicateDocs` VIEW PRIMARY KEY WHERE "
                         << predicate << " ORDER BY Key;";
            const TString primaryRows = SelectJsonRows(db, primaryQuery);

            TStringBuilder indexQuery;
            indexQuery << "SELECT Key FROM `/Root/DuplicateDocs` VIEW json_idx WHERE "
                       << predicate << " ORDER BY Key;";
            const TString indexRows = SelectJsonRows(db, indexQuery);

            CompareYson(expected, primaryRows);
            CompareYson(expected, indexRows);
            CompareYson(primaryRows, indexRows);
        };

        const TString samePredicate = R"(JSON_VALUE(Text, '$.dup' RETURNING Utf8) = "same"u)";
        const TString firstPredicate = R"(JSON_VALUE(Text, '$.dup' RETURNING Utf8) = "first"u)";
        const TString numericPredicate = R"(JSON_VALUE(Text, '$.dup' RETURNING Int64) = 1)";
        const TString nestedPredicate = R"(JSON_VALUE(Text, '$.nested.n' RETURNING Int64) = 1)";

        assertViews(samePredicate, "[[[1u]]]");
        assertViews(firstPredicate, "[[[2u]]]");
        assertViews(R"(JSON_VALUE(Text, '$.dup' RETURNING Utf8) = "second"u)", "[]");
        assertViews(numericPredicate, "[[[3u]]]");
        assertViews(R"(JSON_VALUE(Text, '$.dup' RETURNING Utf8) = "one"u)", "[]");
        assertViews(nestedPredicate, "[[[1u]]]");

        const auto assertAutoSelect = [&](const TString& predicate, const TString& expected) {
            ValidateAutoSelect(db, predicate, "json_idx", "DuplicateDocs");
            TStringBuilder query;
            query << "SELECT Key FROM `/Root/DuplicateDocs` WHERE " << predicate << " ORDER BY Key;";
            CompareYson(expected, SelectJsonRows(db, query));
        };
        assertAutoSelect(samePredicate, "[[[1u]]]");
        assertAutoSelect(firstPredicate, "[[[2u]]]");
        assertAutoSelect(numericPredicate, "[[[3u]]]");
        assertAutoSelect(nestedPredicate, "[[[1u]]]");
    }

    Y_UNIT_TEST_TWIN(TextJsonPostingKeySizeLimit, Compact) {
        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ false, Compact);
        auto db = kikimr.GetQueryClient();

        ExecuteJsonStatement(db, R"(
            CREATE TABLE `/Root/SizeDocs` (
                Key Uint64,
                Text Json,
                PRIMARY KEY (Key),
                INDEX json_idx GLOBAL USING json ON (Text)
            );
        )");

        const size_t margin = 64_KB;
        const size_t below = NDataShard::NLimits::MaxWriteKeySize - margin;
        const size_t above = NDataShard::NLimits::MaxWriteKeySize + margin;
        const TString acceptedPayload(below, 'x');
        const TString rejectedPayload(above, 'x');
        const TString accepted = MakeScalarJson("posting-accepted", acceptedPayload.size());
        const TString rejected = MakeScalarJson("posting-too-large", rejectedPayload.size());
        const TString upsert = R"(
            DECLARE $key AS Uint64;
            DECLARE $text AS Json;
            UPSERT INTO `/Root/SizeDocs` (Key, Text) VALUES ($key, $text);
        )";

        auto acceptedParams = TParamsBuilder()
            .AddParam("$key").Uint64(1).Build()
            .AddParam("$text").Json(accepted).Build()
            .Build();
        auto acceptedResult = db.ExecuteQuery(upsert, TTxControl::NoTx(), acceptedParams).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(
            acceptedResult.GetStatus(), EStatus::SUCCESS, acceptedResult.GetIssues().ToString());

        auto payloadParams = TParamsBuilder()
            .AddParam("$payload").Utf8(acceptedPayload).Build()
            .Build();
        for (const TStringBuf view : {TStringBuf("PRIMARY KEY"), TStringBuf("json_idx")}) {
            TStringBuilder query;
            query << "DECLARE $payload AS Utf8;\n"
                  << "SELECT Key FROM `/Root/SizeDocs` VIEW " << view << '\n'
                  << "WHERE JSON_VALUE(Text, '$.payload' RETURNING Utf8) = $payload;";
            CompareYson("[[[1u]]]", SelectJsonRows(db, query, payloadParams));
        }

        auto rejectedParams = TParamsBuilder()
            .AddParam("$key").Uint64(2).Build()
            .AddParam("$text").Json(rejected).Build()
            .Build();
        auto rejectedResult = db.ExecuteQuery(upsert, TTxControl::NoTx(), rejectedParams).ExtractValueSync();
        UNIT_ASSERT_C(!rejectedResult.IsSuccess(), "Oversized JSON posting key unexpectedly succeeded");
        const TString issues = rejectedResult.GetIssues().ToString();

        auto keyParams = TParamsBuilder()
            .AddParam("$key").Uint64(2).Build()
            .Build();
        CompareYson("[]", SelectJsonRows(db, R"(
            DECLARE $key AS Uint64;
            SELECT Key FROM `/Root/SizeDocs` VIEW PRIMARY KEY WHERE Key = $key;
        )", keyParams));

        auto markerParams = TParamsBuilder()
            .AddParam("$marker").Utf8("posting-too-large").Build()
            .Build();
        CompareYson("[]", SelectJsonRows(db, R"(
            DECLARE $marker AS Utf8;
            SELECT Key FROM `/Root/SizeDocs` VIEW json_idx
            WHERE JSON_VALUE(Text, '$.marker' RETURNING Utf8) = $marker;
        )", markerParams));

        UNIT_ASSERT_C(issues.Contains("Row key size"), issues);
        UNIT_ASSERT_C(issues.Contains("larger than the allowed threshold"), issues);
    }

    Y_UNIT_TEST_TWIN(TextJsonValueSizeLimit, Compact) {
        auto kikimr = KikimrJson(/* enableJsonIndexAutoSelect */ false, Compact);
        auto db = kikimr.GetQueryClient();

        ExecuteJsonStatement(db, R"(
            CREATE TABLE `/Root/ValueSizeDocs` (
                Key Uint64,
                Text Json,
                PRIMARY KEY (Key),
                INDEX json_idx GLOBAL USING json ON (Text)
            );
        )");

        const size_t margin = 64_KB;
        const TString accepted = MakeWhitespaceJson(
            "value-accepted", NDataShard::NLimits::MaxWriteValueSize - margin);
        const TString rejected = MakeWhitespaceJson(
            "value-too-large", NDataShard::NLimits::MaxWriteValueSize + margin);
        const TString upsert = R"(
            DECLARE $key AS Uint64;
            DECLARE $text AS Json;
            UPSERT INTO `/Root/ValueSizeDocs` (Key, Text) VALUES ($key, $text);
        )";
        const auto querySettings = TExecuteQuerySettings().ClientTimeout(TDuration::Minutes(2));
        const auto write = [&](ui64 key, const TString& text) {
            auto params = TParamsBuilder()
                .AddParam("$key").Uint64(key).Build()
                .AddParam("$text").Json(text).Build()
                .Build();
            return db.ExecuteQuery(upsert, TTxControl::NoTx(), params, querySettings).ExtractValueSync();
        };

        auto acceptedResult = write(1, accepted);
        UNIT_ASSERT_VALUES_EQUAL_C(
            acceptedResult.GetStatus(), EStatus::SUCCESS, acceptedResult.GetIssues().ToString());

        auto acceptedMarkerParams = TParamsBuilder()
            .AddParam("$marker").Utf8("value-accepted").Build()
            .Build();
        for (const TStringBuf view : {TStringBuf("PRIMARY KEY"), TStringBuf("json_idx")}) {
            TStringBuilder query;
            query << "DECLARE $marker AS Utf8;\n"
                  << "SELECT Key FROM `/Root/ValueSizeDocs` VIEW " << view << '\n'
                  << "WHERE JSON_VALUE(Text, '$.marker' RETURNING Utf8) = $marker;";
            CompareYson("[[[1u]]]", SelectJsonRows(db, query, acceptedMarkerParams));
        }

        auto rejectedResult = write(2, rejected);
        UNIT_ASSERT_C(!rejectedResult.IsSuccess(), "Oversized JSON cell unexpectedly succeeded");
        const TString issues = rejectedResult.GetIssues().ToString();

        auto keyParams = TParamsBuilder()
            .AddParam("$key").Uint64(2).Build()
            .Build();
        CompareYson("[]", SelectJsonRows(db, R"(
            DECLARE $key AS Uint64;
            SELECT Key FROM `/Root/ValueSizeDocs` VIEW PRIMARY KEY WHERE Key = $key;
        )", keyParams));

        auto rejectedMarkerParams = TParamsBuilder()
            .AddParam("$marker").Utf8("value-too-large").Build()
            .Build();
        CompareYson("[]", SelectJsonRows(db, R"(
            DECLARE $marker AS Utf8;
            SELECT Key FROM `/Root/ValueSizeDocs` VIEW json_idx
            WHERE JSON_VALUE(Text, '$.marker' RETURNING Utf8) = $marker;
        )", rejectedMarkerParams));

        UNIT_ASSERT_C(issues.Contains("Row cell size"), issues);
        UNIT_ASSERT_C(issues.Contains("larger than the allowed threshold"), issues);
    }

    Y_UNIT_TEST_QUAD(PrefixedJsonSinglePrefixMatrix, IsJsonDocument, Compact) {
        const std::string jsonType = IsJsonDocument ? "JsonDocument" : "Json";
        // These four variants form a pairwise matrix for JSON type, index format and build path.
        const bool useAlter = IsJsonDocument != Compact;
        auto kikimr = KikimrJsonPrefix(/* enableJsonIndexAutoSelect */ false, Compact);
        auto db = kikimr.GetQueryClient();

        auto exec = [&](const std::string& query) {
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        };
        auto select = [&](const std::string& query, TParams params = TParamsBuilder().Build()) {
            auto result = db.ExecuteQuery(query, TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            return FormatResultSetYson(result.GetResultSet(0));
        };

        exec(std::format(R"(
            CREATE TABLE `/Root/Docs` (
                Key Uint64,
                UserId Uint64,
                Text {},
                PRIMARY KEY (Key)
                {}
            );
        )", jsonType, useAlter ? "" : ", INDEX json_idx GLOBAL USING json ON (UserId, Text)"));

        const auto json = [&](const std::string& value) {
            return std::format("{}('{}')", jsonType, value);
        };
        exec(std::format(R"(
            UPSERT INTO `/Root/Docs` (Key, UserId, Text) VALUES
                (1, 100, {}),
                (2, 100, {}),
                (3, 200, {}),
                (4, 200, {});
        )",
            json(R"({"kind":"shared","score":10})"),
            json(R"({"kind":"own","score":20})"),
            json(R"({"kind":"shared","score":20})"),
            json(R"({"other":true,"score":30})")));

        if (useAlter) {
            exec(R"(
                ALTER TABLE `/Root/Docs` ADD INDEX json_idx
                    GLOBAL USING json ON (UserId, Text);
            )");
        }

        CompareYson("[[[1u]];[[2u]]]", select(R"(
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE UserId = 100 AND JSON_EXISTS(Text, '$.kind')
            ORDER BY Key;
        )"));

        CompareYson("[[[3u]]]", select(R"(
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE 200 = UserId AND JSON_VALUE(Text, '$.score' RETURNING Int64) = 20
            ORDER BY Key;
        )"));

        auto params = TParamsBuilder().AddParam("$uid").Uint64(200).Build().Build();
        CompareYson("[[[3u]]]", select(R"(
            DECLARE $uid AS Uint64;
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE UserId = $uid AND JSON_EXISTS(Text, '$.kind')
            ORDER BY Key;
        )", params));
    }

    Y_UNIT_TEST_TWIN(PrefixedJsonMultiPrefixMatrix, Compact) {
        // Pair storage type with the opposite format here; the full type/format cross is covered above.
        const std::string jsonType = Compact ? "Json" : "JsonDocument";
        auto kikimr = KikimrJsonPrefix(/* enableJsonIndexAutoSelect */ false, Compact);
        auto db = kikimr.GetQueryClient();

        auto exec = [&](const std::string& query) {
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        };
        auto select = [&](const std::string& query, TParams params = TParamsBuilder().Build()) {
            auto result = db.ExecuteQuery(query, TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            return FormatResultSetYson(result.GetResultSet(0));
        };
        auto expectPrefixError = [&](const std::string& query) {
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "Prefixed JSON index requires an equality predicate");
        };

        exec(std::format(R"(
            CREATE TABLE `/Root/Docs` (
                Key Uint64,
                Tenant Utf8,
                UserId Uint64,
                Text {},
                Data Utf8,
                PRIMARY KEY (Key)
            );
        )", jsonType));

        const auto json = [&](const std::string& value) {
            return std::format("{}('{}')", jsonType, value);
        };
        exec(std::format(R"(
            UPSERT INTO `/Root/Docs` (Key, Tenant, UserId, Text, Data) VALUES
                (1, "acme"u,   100, {}, "data1"u),
                (2, "acme"u,   100, {}, "data2"u),
                (3, "acme"u,   200, {}, "data3"u),
                (4, "globex"u, 100, {}, "data4"u),
                (6, "sentinel"u, 999, {}, "stable"u);
        )",
            json(R"({"kind":"cats","score":10})"),
            json(R"({"kind":"dogs","score":20})"),
            json(R"({"kind":"cats","score":30})"),
            json(R"({"kind":"cats","score":40})"),
            json(R"({"kind":"stable","score":999})")));

        exec(R"(
            ALTER TABLE `/Root/Docs` ADD INDEX json_idx
                GLOBAL USING json ON (Tenant, UserId, Text);
        )");

        auto searchKind = [&](const std::string& tenant, ui64 userId, const std::string& kind) {
            return select(std::format(R"(
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE Tenant = "{}"u AND UserId = {}
                    AND JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "{}"u
                ORDER BY Key;
            )", tenant, userId, kind));
        };
        auto searchScore = [&](const std::string& tenant, ui64 userId, i64 score) {
            return select(std::format(R"(
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE Tenant = "{}"u AND UserId = {}
                    AND JSON_VALUE(Text, '$.score' RETURNING Int64) = {}
                ORDER BY Key;
            )", tenant, userId, score));
        };
        auto assertSentinel = [&] {
            CompareYson("[[[6u]]]", searchKind("sentinel", 999, "stable"));
            CompareYson("[[[6u]]]", searchScore("sentinel", 999, 999));
        };

        CompareYson("[[[1u]]]", searchKind("acme", 100, "cats"));
        CompareYson("[[[1u]]]", searchScore("acme", 100, 10));
        assertSentinel();
        CompareYson("[[[3u]]]", select(R"(
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cats"u
                AND UserId = 200 AND Tenant = "acme"u
            ORDER BY Key;
        )"));
        CompareYson("[[[4u]]]", select(R"(
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE "globex"u = Tenant AND 100 = UserId AND JSON_EXISTS(Text, '$.kind')
            ORDER BY Key;
        )"));

        auto params = TParamsBuilder()
            .AddParam("$tenant").Utf8("acme").Build()
            .AddParam("$uid").Uint64(100).Build()
            .Build();
        CompareYson("[[[1u]];[[2u]]]", select(R"(
            DECLARE $tenant AS Utf8;
            DECLARE $uid AS Uint64;
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE UserId = $uid AND JSON_EXISTS(Text, '$.kind') AND Tenant = $tenant
            ORDER BY Key;
        )", params));

        expectPrefixError(R"(
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE UserId = 100 AND JSON_EXISTS(Text, '$.kind');
        )");
        expectPrefixError(R"(
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE Tenant = "acme"u AND JSON_EXISTS(Text, '$.kind');
        )");
        expectPrefixError(R"(
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE (Tenant = "acme"u OR Tenant = "globex"u)
                AND UserId = 100 AND JSON_EXISTS(Text, '$.kind');
        )");
        expectPrefixError(R"(
            SELECT Key FROM `/Root/Docs` VIEW json_idx
            WHERE Tenant = "acme"u AND UserId > 0 AND JSON_EXISTS(Text, '$.kind');
        )");

        if (Compact) {
            // The pairwise matrix assigns prefix-changing JSON DML to the plain twin; the compact
            // twin covers the same typed multi-prefix build/read and predicate-validation paths
            return;
        }

        exec(std::format(R"(
            INSERT INTO `/Root/Docs` (Key, Tenant, UserId, Text, Data) VALUES
                (5, "acme"u, 100, {}, "inserted"u);
        )", json(R"({"kind":"cats","score":50})")));
        CompareYson("[[[1u]];[[5u]]]", searchKind("acme", 100, "cats"));
        CompareYson("[[[5u]]]", searchScore("acme", 100, 50));
        CompareYson("[[[1u]]]", searchScore("acme", 100, 10));
        CompareYson("[[[3u]]]", searchKind("acme", 200, "cats"));
        assertSentinel();

        exec(std::format(R"(
            UPSERT INTO `/Root/Docs` (Key, Tenant, UserId, Text, Data) VALUES
                (1, "globex"u, 200, {}, "upserted"u);
        )", json(R"({"kind":"owls","score":60})")));
        CompareYson("[[[5u]]]", searchKind("acme", 100, "cats"));
        CompareYson("[]", searchScore("acme", 100, 10));
        CompareYson("[[[1u]]]", searchKind("globex", 200, "owls"));
        CompareYson("[[[1u]]]", searchScore("globex", 200, 60));
        assertSentinel();

        exec(std::format(R"(
            UPDATE `/Root/Docs`
            SET Tenant = "globex"u, UserId = 100, Text = {}, Data = "updated"u
            WHERE Key = 2;
        )", json(R"({"kind":"birds","score":70})")));
        CompareYson("[]", searchKind("acme", 100, "dogs"));
        CompareYson("[]", searchScore("acme", 100, 20));
        CompareYson("[[[2u]]]", searchKind("globex", 100, "birds"));
        CompareYson("[[[2u]]]", searchScore("globex", 100, 70));
        assertSentinel();

        exec(std::format(R"(
            REPLACE INTO `/Root/Docs` (Key, Tenant, UserId, Text, Data) VALUES
                (3, "acme"u, 100, {}, "replaced"u);
        )", json(R"({"kind":"cats","score":80})")));
        CompareYson("[]", searchKind("acme", 200, "cats"));
        CompareYson("[]", searchScore("acme", 200, 30));
        CompareYson("[[[3u]];[[5u]]]", searchKind("acme", 100, "cats"));
        CompareYson("[[[3u]]]", searchScore("acme", 100, 80));
        assertSentinel();

        exec(R"(DELETE FROM `/Root/Docs` WHERE Key = 4;)");
        CompareYson("[]", searchKind("globex", 100, "cats"));
        CompareYson("[]", searchScore("globex", 100, 40));
        CompareYson("[[[2u]]]", searchKind("globex", 100, "birds"));
        CompareYson("[[[1u]]]", searchKind("globex", 200, "owls"));
        assertSentinel();
    }

    Y_UNIT_TEST_TWIN(PrefixedJsonRowIdComplexPk, Compact) {
        // __ydb_row_id is the posting doc-id; index reads must resolve it back to the composite PK
        auto kikimr = KikimrJsonPrefixRowId(Compact);
        auto db = kikimr.GetQueryClient();

        auto exec = [&](const std::string& query) {
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        };
        auto select = [&](const std::string& query) {
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            return FormatResultSetYson(result.GetResultSet(0));
        };

        exec(R"(
            CREATE TABLE `/Root/Docs` (
                Org Utf8 NOT NULL,
                Pk Utf8 NOT NULL,
                Tenant Utf8,
                Text JsonDocument,
                __ydb_row_id Uint64 NOT NULL,
                PRIMARY KEY (Org, Pk)
            );
        )");
        exec(R"(
            ALTER TABLE `/Root/Docs`
                ADD INDEX uniq_rowid GLOBAL UNIQUE ON (__ydb_row_id);
        )");
        exec(R"(
            ALTER TABLE `/Root/Docs` ADD INDEX json_idx
                GLOBAL USING json ON (Tenant, Text);
        )");
        exec(R"(
            UPSERT INTO `/Root/Docs` (Org, Pk, Tenant, Text) VALUES
                ("acme"u,   "a1"u, "red"u,  JsonDocument('{"kind":"cats","score":10}')),
                ("acme"u,   "a2"u, "red"u,  JsonDocument('{"kind":"dogs","score":20}')),
                ("acme"u,   "a3"u, "blue"u, JsonDocument('{"kind":"cats","score":30}')),
                ("globex"u, "a1"u, "red"u,  JsonDocument('{"kind":"cats","score":40}'));
        )");

        CompareYson(R"([["acme";"a1"];["globex";"a1"]])", select(R"(
            SELECT Org, Pk FROM `/Root/Docs` VIEW json_idx
            WHERE Tenant = "red"u
                AND JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "cats"u
            ORDER BY Org, Pk;
        )"));
        CompareYson(R"([["acme";"a3"]])", select(R"(
            SELECT Org, Pk FROM `/Root/Docs` VIEW json_idx
            WHERE Tenant = "blue"u AND JSON_EXISTS(Text, '$.kind')
            ORDER BY Org, Pk;
        )"));
        CompareYson(R"([["acme";"a2"]])", select(R"(
            SELECT Org, Pk FROM `/Root/Docs` VIEW json_idx
            WHERE "red"u = Tenant
                AND JSON_VALUE(Text, '$.kind' RETURNING Utf8) = "dogs"u
            ORDER BY Org, Pk;
        )"));
    }

    Y_UNIT_TEST(PrefixedJsonDdlValidation) {
        auto kikimr = KikimrJsonPrefix();
        auto db = kikimr.GetQueryClient();

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE `/Root/PrefixOnPk` (
                    Key Uint64,
                    Text Json,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (Key, Text)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "JSON index prefix column 'Key' must not be a primary key column");
        }

        {
            auto result = db.ExecuteQuery(R"(
                CREATE TABLE `/Root/InvalidPrefixType` (
                    Key Uint64,
                    BadPrefix Json,
                    Text JsonDocument,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (BadPrefix, Text)
                );
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "Column BadPrefix has wrong key type Json");
        }
    }

    Y_UNIT_TEST(PrefixedJsonCreate) {
        auto kikimr = KikimrJsonPrefix();
        auto db = kikimr.GetQueryClient();

        {
            std::string query = R"(
                CREATE TABLE `/Root/Docs` (
                    Key Uint64,
                    UserId Uint64,
                    Text Json,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (UserId, Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                UPSERT INTO `/Root/Docs` (Key, UserId, Text) VALUES
                    (1, 100, Json('{"k1": "v1"}')),
                    (2, 100, Json('{"k2": "v2"}')),
                    (3, 200, Json('{"k1": "v1"}')),
                    (4, 200, Json('{"k3": "v3"}'));
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Query with prefix equality + JSON predicate: user 100 sees only its own docs
        {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE UserId = 100 AND JSON_EXISTS(Text, '$.k1')
                ORDER BY Key;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[[[1u]]]", FormatResultSetYson(result.GetResultSet(0)));
        }

        // User 200 sees only its own docs with k1
        {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE UserId = 200 AND JSON_EXISTS(Text, '$.k1')
                ORDER BY Key;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[[[3u]]]", FormatResultSetYson(result.GetResultSet(0)));
        }

        // Prefix value passed as a parameter
        {
            auto params = TParamsBuilder().AddParam("$uid").Uint64(200).Build().Build();
            auto result = db.ExecuteQuery(R"(
                DECLARE $uid AS Uint64;
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE UserId = $uid AND JSON_EXISTS(Text, '$.k1')
                ORDER BY Key;
            )", TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[[[3u]]]", FormatResultSetYson(result.GetResultSet(0)));
        }
    }

    Y_UNIT_TEST(PrefixedJsonQueryMissingPrefix) {
        auto kikimr = KikimrJsonPrefix();
        auto db = kikimr.GetQueryClient();

        {
            std::string query = R"(
                CREATE TABLE `/Root/Docs` (
                    Key Uint64,
                    UserId Uint64,
                    Text Json,
                    PRIMARY KEY (Key),
                    INDEX json_idx GLOBAL USING json ON (UserId, Text)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Missing equality on the prefix column => error
        {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE JSON_EXISTS(Text, '$.k1')
                ORDER BY Key;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "Prefixed JSON index requires an equality predicate");
        }

        // More complex expression (OR) => error
        {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE (UserId = 100 OR UserId = 200) AND JSON_EXISTS(Text, '$.k1')
                ORDER BY Key;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "Prefixed JSON index requires an equality predicate");
        }

        // More complex expression (sqrt :D) => error
        {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE (UserId * UserId) = 100 AND JSON_EXISTS(Text, '$.k1')
                ORDER BY Key;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
                "Prefixed JSON index requires an equality predicate");
        }
    }

    Y_UNIT_TEST(PrefixedJsonAlterAdd) {
        auto kikimr = KikimrJsonPrefix();
        auto db = kikimr.GetQueryClient();

        {
            std::string query = R"(
                CREATE TABLE `/Root/Docs` (
                    Key Uint64,
                    UserId Uint64,
                    Text Json,
                    PRIMARY KEY (Key)
                );
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        {
            std::string query = R"(
                UPSERT INTO `/Root/Docs` (Key, UserId, Text) VALUES
                    (1, 100, Json('{"k1": "v1"}')),
                    (2, 100, Json('{"k2": "v2"}')),
                    (3, 200, Json('{"k1": "v1"}')),
                    (4, 200, Json('{"k3": "v3"}'));
            )";
            auto result = db.ExecuteQuery(query, TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // ALTER TABLE ADD INDEX with prefix columns
        {
            auto result = db.ExecuteQuery(R"(
                ALTER TABLE `/Root/Docs` ADD INDEX json_idx GLOBAL USING json ON (UserId, Text)
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Query with prefix equality after ALTER
        {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE UserId = 100 AND JSON_EXISTS(Text, '$.k1')
                ORDER BY Key;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[[[1u]]]", FormatResultSetYson(result.GetResultSet(0)));
        }

        // Insert more data after the index is built
        {
            auto result = db.ExecuteQuery(R"(
                UPSERT INTO `/Root/Docs` (Key, UserId, Text) VALUES
                    (5, 100, Json('{"k1": "v5"}'));
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
        }

        // Verify the new row is returned
        {
            auto result = db.ExecuteQuery(R"(
                SELECT Key FROM `/Root/Docs` VIEW json_idx
                WHERE UserId = 100 AND JSON_EXISTS(Text, '$.k1')
                ORDER BY Key;
            )", TTxControl::NoTx()).ExtractValueSync();
            UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::SUCCESS, result.GetIssues().ToString());
            CompareYson("[[[1u]];[[5u]]]", FormatResultSetYson(result.GetResultSet(0)));
        }
    }
}

}  // namespace NKikimr::NKqp
