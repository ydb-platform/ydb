#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NYdb::NTable;

namespace {

TKikimrSettings DefaultsSettings() {
    auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
    settings.AppConfig.MutableFeatureFlags()->SetEnableColumnTableDefaults(true);
    return settings;
}

void ExecScheme(TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void ExecDml(TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
}

void ExpectSchemeError(TQueryClient& db, const TString& query, const TString& message) {
    auto result = db.ExecuteQuery(query, NQuery::TTxControl::NoTx()).GetValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), message);
}

void ExpectDmlError(TQueryClient& db, const TString& query, const TString& message) {
    auto result = db.ExecuteQuery(query, NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
    UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), message);
}

TString SelectYson(TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    return FormatResultSetYson(result.GetResultSet(0));
}

} // namespace

Y_UNIT_TEST_SUITE(KqpOlapColumnDefaults) {

    Y_UNIT_TEST(InsertOmitAndExplicitNull) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefIns` (
                Key Uint32 NOT NULL,
                Value Utf8 DEFAULT 'aba',
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecDml(db, "INSERT INTO `/Root/DefIns` (Key) VALUES (1);");
        ExecDml(db, "INSERT INTO `/Root/DefIns` (Key, Value) VALUES (2, NULL);");
        CompareYson(R"([[["aba"]]])", SelectYson(db, "SELECT Value FROM `/Root/DefIns` WHERE Key = 1;"));
        CompareYson(R"([[#]])", SelectYson(db, "SELECT Value FROM `/Root/DefIns` WHERE Key = 2;"));
    }

    Y_UNIT_TEST(UpsertNewAndExisting) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefUpsert` (
                Key Uint32 NOT NULL,
                Value Utf8 DEFAULT 'born',
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecDml(db, "UPSERT INTO `/Root/DefUpsert` (Key) VALUES (1);");
        ExecDml(db, "UPSERT INTO `/Root/DefUpsert` (Key, Value) VALUES (1, 'kept');");
        ExecDml(db, "UPSERT INTO `/Root/DefUpsert` (Key) VALUES (1);");
        ExecDml(db, "UPSERT INTO `/Root/DefUpsert` (Key) VALUES (2);");
        CompareYson(R"([[["kept"]]])", SelectYson(db, "SELECT Value FROM `/Root/DefUpsert` WHERE Key = 1;"));
        CompareYson(R"([[["born"]]])", SelectYson(db, "SELECT Value FROM `/Root/DefUpsert` WHERE Key = 2;"));
    }

    Y_UNIT_TEST(ReplaceOmit) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefReplace` (
                Key Uint32 NOT NULL,
                Value Utf8 DEFAULT 'cur',
                Other Utf8,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecDml(db, "INSERT INTO `/Root/DefReplace` (Key, Value, Other) VALUES (1, 'old', 'keep');");
        ExecDml(db, "REPLACE INTO `/Root/DefReplace` (Key) VALUES (1);");
        CompareYson(R"([[["cur"];#]])", SelectYson(db, "SELECT Value, Other FROM `/Root/DefReplace` WHERE Key = 1;"));
    }

    Y_UNIT_TEST(AddColumnDefaultOnExisting) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefAdd` (
                Key Uint32 NOT NULL,
                Value Utf8,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecDml(db, "INSERT INTO `/Root/DefAdd` (Key, Value) VALUES (1, 'row');");
        ExecScheme(db, "ALTER TABLE `/Root/DefAdd` ADD COLUMN Extra Utf8 DEFAULT 'born';");
        CompareYson(R"([[["born"]]])", SelectYson(db, "SELECT Extra FROM `/Root/DefAdd` WHERE Key = 1;"));
        ExecDml(db, "INSERT INTO `/Root/DefAdd` (Key, Value) VALUES (2, 'new');");
        CompareYson(R"([[["born"]]])", SelectYson(db, "SELECT Extra FROM `/Root/DefAdd` WHERE Key = 2;"));
    }

    Y_UNIT_TEST(AddThenSetDefault) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefAddSet` (
                Key Uint32 NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecScheme(db, "ALTER TABLE `/Root/DefAddSet` ADD COLUMN Extra Utf8;");
        ExecDml(db, "INSERT INTO `/Root/DefAddSet` (Key) VALUES (1);");
        ExecScheme(db, "ALTER TABLE `/Root/DefAddSet` ALTER COLUMN Extra SET DEFAULT 'later';");
        ExecDml(db, "INSERT INTO `/Root/DefAddSet` (Key) VALUES (2);");
        CompareYson(R"([[#]])", SelectYson(db, "SELECT Extra FROM `/Root/DefAddSet` WHERE Key = 1;"));
        CompareYson(R"([[["later"]]])", SelectYson(db, "SELECT Extra FROM `/Root/DefAddSet` WHERE Key = 2;"));
    }

    Y_UNIT_TEST(SetDefaultMultipleTimes) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefSets` (
                Key Uint32 NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecScheme(db, "ALTER TABLE `/Root/DefSets` ADD COLUMN Extra Utf8 DEFAULT 'x';");
        ExecDml(db, "INSERT INTO `/Root/DefSets` (Key) VALUES (1);");
        ExecScheme(db, "ALTER TABLE `/Root/DefSets` ALTER COLUMN Extra SET DEFAULT 'y';");
        ExecDml(db, "INSERT INTO `/Root/DefSets` (Key) VALUES (2);");
        ExecScheme(db, "ALTER TABLE `/Root/DefSets` ALTER COLUMN Extra SET DEFAULT 'z';");
        ExecDml(db, "INSERT INTO `/Root/DefSets` (Key) VALUES (3);");
        CompareYson(R"([[["x"]]])", SelectYson(db, "SELECT Extra FROM `/Root/DefSets` WHERE Key = 1;"));
        CompareYson(R"([[["y"]]])", SelectYson(db, "SELECT Extra FROM `/Root/DefSets` WHERE Key = 2;"));
        CompareYson(R"([[["z"]]])", SelectYson(db, "SELECT Extra FROM `/Root/DefSets` WHERE Key = 3;"));
    }

    Y_UNIT_TEST(DropDefaultNullable) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefDrop` (
                Key Uint32 NOT NULL,
                Value Utf8 DEFAULT 'a',
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecDml(db, "INSERT INTO `/Root/DefDrop` (Key) VALUES (1);");
        ExecScheme(db, "ALTER TABLE `/Root/DefDrop` ALTER COLUMN Value DROP DEFAULT;");
        ExecDml(db, "INSERT INTO `/Root/DefDrop` (Key) VALUES (2);");
        CompareYson(R"([[["a"]]])", SelectYson(db, "SELECT Value FROM `/Root/DefDrop` WHERE Key = 1;"));
        CompareYson(R"([[#]])", SelectYson(db, "SELECT Value FROM `/Root/DefDrop` WHERE Key = 2;"));
    }

    Y_UNIT_TEST(DropDefaultNotNullRejectsOmit) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefDropNn` (
                Key Uint32 NOT NULL,
                Value Uint32 NOT NULL DEFAULT 1,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecScheme(db, "ALTER TABLE `/Root/DefDropNn` ALTER COLUMN Value DROP DEFAULT;");
        ExpectDmlError(db, "INSERT INTO `/Root/DefDropNn` (Key) VALUES (1);", "Missing not null column");
    }

    Y_UNIT_TEST(DropAddSameName) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefReadd` (
                Key Uint32 NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecScheme(db, "ALTER TABLE `/Root/DefReadd` ADD COLUMN Name Utf8 DEFAULT 'old';");
        ExecDml(db, "INSERT INTO `/Root/DefReadd` (Key) VALUES (1);");
        ExecScheme(db, "ALTER TABLE `/Root/DefReadd` DROP COLUMN Name;");
        ExecScheme(db, "ALTER TABLE `/Root/DefReadd` ADD COLUMN Name Utf8 DEFAULT 'new';");
        CompareYson(R"([[["new"]]])", SelectYson(db, "SELECT Name FROM `/Root/DefReadd` WHERE Key = 1;"));
    }

    Y_UNIT_TEST(BulkUpsertOmittedAndExplicitNull) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        auto table = kikimr.GetTableClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefBulk` (
                Key Uint32 NOT NULL,
                Value Utf8 DEFAULT 'bulk',
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        {
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).EndStruct().EndList();
            auto result = table.BulkUpsert("/Root/DefBulk", rows.Build()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        {
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(2).AddMember("Value").EmptyOptional(EPrimitiveType::Utf8).EndStruct().EndList();
            auto result = table.BulkUpsert("/Root/DefBulk", rows.Build()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        CompareYson(R"([[["bulk"]]])", SelectYson(db, "SELECT Value FROM `/Root/DefBulk` WHERE Key = 1;"));
        CompareYson(R"([[#]])", SelectYson(db, "SELECT Value FROM `/Root/DefBulk` WHERE Key = 2;"));
    }

    Y_UNIT_TEST(PrimaryKeyDefaultRejected) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExpectSchemeError(db, R"(
            CREATE TABLE `/Root/DefPk` (
                Key Uint32 NOT NULL DEFAULT 7,
                Value Utf8,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )", "Default value is not supported for primary key columns in column tables");
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefPk2` (
                Key Uint32 NOT NULL,
                Value Utf8 DEFAULT 'ok',
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExpectSchemeError(db, "ALTER TABLE `/Root/DefPk2` ALTER COLUMN Key SET DEFAULT 1;",
            "Default value is not supported for primary key columns in column tables");
        ExpectSchemeError(db,
            "ALTER OBJECT `/Root/DefPk2` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Key, `DEFAULT_VALUE`=`1`);",
            "Default value is not supported for primary key columns in column tables");
    }

    Y_UNIT_TEST(BoolAndStringDefaultsAndUnsupportedType) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefTypes` (
                Key Uint32 NOT NULL,
                Flag Bool DEFAULT true,
                Payload String DEFAULT 'xy',
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecDml(db, "INSERT INTO `/Root/DefTypes` (Key) VALUES (1);");
        CompareYson(R"([[[%true];["xy"]]])", SelectYson(db, "SELECT Flag, Payload FROM `/Root/DefTypes` WHERE Key = 1;"));
        ExpectSchemeError(db, R"(
            CREATE TABLE `/Root/DefBad` (
                Key Uint32 NOT NULL,
                Doc Json DEFAULT '{}',
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )", "Default value is not supported for type");
    }

    Y_UNIT_TEST(SparsedSetDefaultRejected) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefSparse` (
                Key Uint32 NOT NULL,
                Value Uint32,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecScheme(db,
            "ALTER OBJECT `/Root/DefSparse` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Value, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SPARSED`);");
        ExpectSchemeError(db, "ALTER TABLE `/Root/DefSparse` ALTER COLUMN Value SET DEFAULT 1;",
            "Changing default value of a sparsed column is not supported");
    }

}

} // namespace NKikimr::NKqp
