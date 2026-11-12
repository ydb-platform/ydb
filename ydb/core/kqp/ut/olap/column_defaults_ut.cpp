#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/string/subst.h>

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

    Y_UNIT_TEST(BoolStringAndJsonDefaults) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefTypes` (
                Key Uint32 NOT NULL,
                Flag Bool DEFAULT true,
                Payload String DEFAULT 'xy',
                Doc Json DEFAULT '{}',
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecDml(db, "INSERT INTO `/Root/DefTypes` (Key) VALUES (1);");
        CompareYson(R"([[[%true];["xy"];["{}"]]])", SelectYson(db, "SELECT Flag, Payload, Doc FROM `/Root/DefTypes` WHERE Key = 1;"));
    }

    Y_UNIT_TEST(NotNullDefaultOmitMatchesRowTable) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        for (const char* side : {"Ds", "Cs"}) {
            const TString store = TString(side) == "Cs" ? "WITH (STORE = COLUMN)" : "";
            ExecScheme(db, TStringBuilder() << R"(
                CREATE TABLE `/Root/)" << side << R"(Nn` (
                    Key Uint32 NOT NULL,
                    Name Utf8,
                    Value Uint32 NOT NULL DEFAULT 1,
                    Note Utf8 DEFAULT 'n',
                    PRIMARY KEY (Key)
                ) )" << store << ";");
        }
        const auto runBoth = [&](const TString& sql) {
            for (const char* side : {"Ds", "Cs"}) {
                TString query = sql;
                SubstGlobal(query, "$T", TStringBuilder() << "`/Root/" << side << "Nn`");
                ExecDml(db, query);
            }
        };
        const auto selectBoth = [&](const TString& sql) {
            TString ds = sql;
            TString cs = sql;
            SubstGlobal(ds, "$T", "`/Root/DsNn`");
            SubstGlobal(cs, "$T", "`/Root/CsNn`");
            CompareYson(SelectYson(db, ds), SelectYson(db, cs));
        };
        runBoth("INSERT INTO $T (Key, Name) VALUES (1, 'a');");
        selectBoth("SELECT Key, Name, Value, Note FROM $T WHERE Key = 1;");

        runBoth("INSERT INTO $T (Key, Name, Note) VALUES (2, 'b', NULL);");
        selectBoth("SELECT Key, Name, Value, Note FROM $T WHERE Key = 2;");

        runBoth("UPSERT INTO $T (Key, Name, Value) VALUES (3, 'c', 9);");
        runBoth("UPSERT INTO $T (Key, Name) VALUES (3, 'd');");
        selectBoth("SELECT Key, Name, Value, Note FROM $T WHERE Key = 3;");

        runBoth("REPLACE INTO $T (Key, Name) VALUES (3, 'e');");
        selectBoth("SELECT Key, Name, Value, Note FROM $T WHERE Key = 3;");

        runBoth("UPDATE $T SET Name = 'f' WHERE Key = 1;");
        selectBoth("SELECT Key, Name, Value, Note FROM $T WHERE Key = 1;");

        runBoth("DELETE FROM $T WHERE Key = 3;");
        runBoth("UPSERT INTO $T (Key, Name) VALUES (3, 'g');");
        selectBoth("SELECT Key, Name, Value, Note FROM $T WHERE Key = 3;");

        for (const char* side : {"Ds", "Cs"}) {
            ExecScheme(db, TStringBuilder() << "ALTER TABLE `/Root/" << side << "Nn` ALTER COLUMN Value SET DEFAULT 5;");
        }
        runBoth("INSERT INTO $T (Key, Name) VALUES (4, 'h');");
        selectBoth("SELECT Key, Value FROM $T WHERE Key IN (1, 4) ORDER BY Key;");

        for (const char* side : {"Ds", "Cs"}) {
            ExecScheme(db, TStringBuilder() << "ALTER TABLE `/Root/" << side << "Nn` ALTER COLUMN Value DROP DEFAULT;");
            ExpectDmlError(db, TStringBuilder() << "INSERT INTO `/Root/" << side << "Nn` (Key, Name) VALUES (5, 'i');",
                "Missing not null column");
        }
    }

    Y_UNIT_TEST(LiteralTypesMatchRowTable) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        const char* columns = R"(
            k Uint32 NOT NULL,
            d Date DEFAULT Date('2021-01-01'),
            dt Datetime DEFAULT Datetime('2021-01-01T00:00:00Z'),
            ts Timestamp DEFAULT Timestamp('2021-01-01T00:00:00.000000Z'),
            iv Interval DEFAULT Interval('P1D'),
            d32 Date32 DEFAULT Date32('2021-01-01'),
            dt64 Datetime64 DEFAULT Datetime64('2021-01-01T00:00:00Z'),
            ts64 Timestamp64 DEFAULT Timestamp64('2021-01-01T00:00:00.000000Z'),
            iv64 Interval64 DEFAULT Interval64('P1D'),
            js Json DEFAULT Json('[0]'),
            ys Yson DEFAULT Yson('[1]'),
            uid Uuid DEFAULT Uuid('65df9ecc-a97d-47b2-ae56-3c023da6ee8c'),
            dec Decimal(22, 9) DEFAULT Decimal('1.11', 22, 9),
            PRIMARY KEY (k)
        )";
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/DsTypes` (" << columns << ");");
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/CsTypes` (" << columns << ") WITH (STORE = COLUMN);");
        ExecDml(db, "UPSERT INTO `/Root/DsTypes` (k) VALUES (1);");
        ExecDml(db, "UPSERT INTO `/Root/CsTypes` (k) VALUES (1);");
        const TString select = "SELECT d, dt, ts, iv, d32, dt64, ts64, iv64, js, ys, uid, dec FROM ";
        CompareYson(SelectYson(db, select + "`/Root/DsTypes`"), SelectYson(db, select + "`/Root/CsTypes`"));
        ExpectSchemeError(db, R"(
            CREATE TABLE `/Root/CsJsonDoc` (
                k Uint32 NOT NULL,
                jd JsonDocument DEFAULT JsonDocument('[2]'),
                PRIMARY KEY (k)
            ) WITH (STORE = COLUMN);
        )", "Default value is not supported for type");
        ExpectSchemeError(db, R"(
            CREATE TABLE `/Root/CsDyNumber` (
                k Uint32 NOT NULL,
                dn DyNumber DEFAULT DyNumber('3.14'),
                PRIMARY KEY (k)
            ) WITH (STORE = COLUMN);
        )", "Default value is not supported for type");
    }

    Y_UNIT_TEST(BulkUpsertNotNullDefault) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        auto table = kikimr.GetTableClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DsBulkNn` (
                Key Uint32 NOT NULL,
                Value Uint32 NOT NULL DEFAULT 4,
                PRIMARY KEY (Key)
            );
        )");
        ExecScheme(db, R"(
            CREATE TABLE `/Root/CsBulkNn` (
                Key Uint32 NOT NULL,
                Value Uint32 NOT NULL DEFAULT 4,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        const auto omittedKey = []() {
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(1).EndStruct().EndList();
            return rows.Build();
        };
        {
            auto result = table.BulkUpsert("/Root/DsBulkNn", omittedKey()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing not null columns");
        }
        {
            auto result = table.BulkUpsert("/Root/CsBulkNn", omittedKey()).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        }
        CompareYson(R"([[4u]])", SelectYson(db, "SELECT Value FROM `/Root/CsBulkNn` WHERE Key = 1;"));
    }

    Y_UNIT_TEST(ShowCreatePrintsSqlDefault) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/CsShow` (
                Key Uint32 NOT NULL,
                Name Utf8 DEFAULT 'aba',
                Value Uint32 NOT NULL DEFAULT 5,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        auto result = db.GetSession().GetValueSync().GetSession().ExecuteQuery(
            "SHOW CREATE TABLE `/Root/CsShow`;", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto yson = FormatResultSetYson(result.GetResultSet(0));
        UNIT_ASSERT_STRING_CONTAINS(yson, "DEFAULT 'aba'");
        UNIT_ASSERT_STRING_CONTAINS(yson, "DEFAULT 5");
        UNIT_ASSERT(!yson.Contains("DEFAULT_VALUE"));
        UNIT_ASSERT(!yson.Contains("ALTER OBJECT"));
    }

    Y_UNIT_TEST(AddColumnNotNullDefault) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DsAddNn` (
                Key Uint32 NOT NULL,
                PRIMARY KEY (Key)
            );
        )");
        ExecScheme(db, R"(
            CREATE TABLE `/Root/CsAddNn` (
                Key Uint32 NOT NULL,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecDml(db, "INSERT INTO `/Root/DsAddNn` (Key) VALUES (1);");
        ExecDml(db, "INSERT INTO `/Root/CsAddNn` (Key) VALUES (1);");
        ExpectSchemeError(db, "ALTER TABLE `/Root/CsAddNn` ADD COLUMN Extra Utf8 NOT NULL;",
            "Cannot add not null column without default value");
        ExecScheme(db, "ALTER TABLE `/Root/DsAddNn` ADD COLUMN Extra Utf8 NOT NULL DEFAULT 'born';");
        ExecScheme(db, "ALTER TABLE `/Root/CsAddNn` ADD COLUMN Extra Utf8 NOT NULL DEFAULT 'born';");
        CompareYson(SelectYson(db, "SELECT Extra FROM `/Root/DsAddNn` WHERE Key = 1;"),
            SelectYson(db, "SELECT Extra FROM `/Root/CsAddNn` WHERE Key = 1;"));
        ExecDml(db, "INSERT INTO `/Root/DsAddNn` (Key) VALUES (2);");
        ExecDml(db, "INSERT INTO `/Root/CsAddNn` (Key) VALUES (2);");
        CompareYson(SelectYson(db, "SELECT Key, Extra FROM `/Root/DsAddNn` ORDER BY Key;"),
            SelectYson(db, "SELECT Key, Extra FROM `/Root/CsAddNn` ORDER BY Key;"));
    }

    Y_UNIT_TEST(CompactionAndActualizationKeepFill) {
        auto cs = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/CsKeep` (
                Key Uint32 NOT NULL,
                Name Utf8,
                Value Utf8 DEFAULT 'born',
                Note Utf8,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecDml(db, "INSERT INTO `/Root/CsKeep` (Key, Name) VALUES (1, 'omit');");
        ExecDml(db, "INSERT INTO `/Root/CsKeep` (Key, Name, Value, Note) VALUES (2, 'explicit', 'stored', NULL);");
        ExecDml(db, "UPSERT INTO `/Root/CsKeep` (Key, Name) VALUES (3, 'new');");
        ExecDml(db, "UPSERT INTO `/Root/CsKeep` (Key, Name, Value) VALUES (1, 'omit', 'kept');");
        ExecDml(db, "UPSERT INTO `/Root/CsKeep` (Key, Name) VALUES (1, 'renamed');");
        UNIT_ASSERT(cs->WaitCompactions(TDuration::Seconds(20)));
        {
            const auto row = SelectYson(db, "SELECT Name, Value, Note FROM `/Root/CsKeep` WHERE Key = 1;");
            UNIT_ASSERT_STRING_CONTAINS(row, "renamed");
            UNIT_ASSERT_STRING_CONTAINS(row, "kept");
        }
        {
            const auto row = SelectYson(db, "SELECT Name, Value, Note FROM `/Root/CsKeep` WHERE Key = 2;");
            UNIT_ASSERT_STRING_CONTAINS(row, "stored");
            UNIT_ASSERT_STRING_CONTAINS(row, "#");
        }
        {
            const auto row = SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 3;");
            UNIT_ASSERT_STRING_CONTAINS(row, "born");
        }

        ExecDml(db, "REPLACE INTO `/Root/CsKeep` (Key, Name) VALUES (3, 'replaced');");
        ExecDml(db, "UPDATE `/Root/CsKeep` SET Name = 'updated' WHERE Key = 2;");
        ExecDml(db, "DELETE FROM `/Root/CsKeep` WHERE Key = 3;");
        ExecDml(db, "UPSERT INTO `/Root/CsKeep` (Key, Name) VALUES (3, 'again');");
        UNIT_ASSERT(cs->WaitCompactions(TDuration::Seconds(20)));
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 3;"), "born");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Name, Value, Note FROM `/Root/CsKeep` WHERE Key = 2;"), "updated");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 2;"), "stored");

        ExecScheme(db, "ALTER TABLE `/Root/CsKeep` ADD COLUMN Extra Utf8 DEFAULT 'added';");
        ExecScheme(db, "ALTER TABLE `/Root/CsKeep` ADD COLUMN Required Utf8 NOT NULL DEFAULT 'nn';");
        ExecScheme(db, "ALTER OBJECT `/Root/CsKeep` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
        const auto actualizedAfterAdd = cs->GetActualizationsCount().Val();
        cs->WaitActualization(TDuration::Seconds(30), true);
        UNIT_ASSERT_C(cs->GetActualizationsCount().Val() > actualizedAfterAdd, "scheme actualization did not rewrite portions");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Extra, Required FROM `/Root/CsKeep` WHERE Key = 1;"), "added");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Extra, Required FROM `/Root/CsKeep` WHERE Key = 1;"), "nn");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 1;"), "kept");

        ExecScheme(db, "ALTER TABLE `/Root/CsKeep` ALTER COLUMN Value SET DEFAULT 'later';");
        ExecScheme(db, "ALTER OBJECT `/Root/CsKeep` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
        const auto actualizedAfterSet = cs->GetActualizationsCount().Val();
        cs->WaitActualization(TDuration::Seconds(30), true);
        UNIT_ASSERT_C(cs->GetActualizationsCount().Val() > actualizedAfterSet, "SET DEFAULT did not actualize portions");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 1;"), "kept");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 2;"), "stored");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 3;"), "born");
        ExecDml(db, "INSERT INTO `/Root/CsKeep` (Key, Name) VALUES (4, 'after-set');");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value, Extra, Required FROM `/Root/CsKeep` WHERE Key = 4;"), "later");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Extra, Required FROM `/Root/CsKeep` WHERE Key = 4;"), "added");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Required FROM `/Root/CsKeep` WHERE Key = 4;"), "nn");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 1;"), "kept");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 4;"), "later");

        ExecScheme(db, "ALTER TABLE `/Root/CsKeep` ALTER COLUMN Value DROP DEFAULT;");
        ExecScheme(db, "ALTER OBJECT `/Root/CsKeep` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
        const auto actualizedAfterDrop = cs->GetActualizationsCount().Val();
        cs->WaitActualization(TDuration::Seconds(30), true);
        UNIT_ASSERT_C(cs->GetActualizationsCount().Val() > actualizedAfterDrop, "DROP DEFAULT did not actualize portions");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 1;"), "kept");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 3;"), "born");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 4;"), "later");
        ExecDml(db, "INSERT INTO `/Root/CsKeep` (Key, Name) VALUES (5, 'after-drop');");
        {
            const auto row = SelectYson(db, "SELECT Value FROM `/Root/CsKeep` WHERE Key = 5;");
            UNIT_ASSERT_C(row.Contains("#"), row);
        }
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Required FROM `/Root/CsKeep` WHERE Key = 5;"), "nn");
        ExecScheme(db, "ALTER TABLE `/Root/CsKeep` ALTER COLUMN Required DROP DEFAULT;");
        ExpectDmlError(db, "INSERT INTO `/Root/CsKeep` (Key, Name, Value) VALUES (6, 'missing-nn', 'x');",
            "Missing not null column");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Required FROM `/Root/CsKeep` WHERE Key = 1;"), "nn");
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
