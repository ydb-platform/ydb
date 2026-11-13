#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/ut/olap/combinatory/compaction.h>
#include <ydb/core/tx/columnshard/hooks/testing/controller.h>
#include <ydb/core/tx/columnshard/test_helper/controllers.h>
#include <ydb/public/lib/ydb_cli/common/format.h>

#include <library/cpp/testing/unittest/registar.h>
#include <util/generic/size_literals.h>
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

void ExpectDmlFailure(TQueryClient& db, const TString& query) {
    auto result = db.ExecuteQuery(query, NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
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

    Y_UNIT_TEST(PrimaryKeyStringDefaultRejected) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DefPk2` (
                Key Uint32 NOT NULL,
                Value Utf8 DEFAULT 'ok',
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
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
            jd JsonDocument DEFAULT JsonDocument('[2]'),
            dn DyNumber DEFAULT DyNumber('3.14'),
            PRIMARY KEY (k)
        )";
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/DsTypes` (" << columns << ");");
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/CsTypes` (" << columns << ") WITH (STORE = COLUMN);");
        ExecDml(db, "UPSERT INTO `/Root/DsTypes` (k) VALUES (1);");
        ExecDml(db, "UPSERT INTO `/Root/CsTypes` (k) VALUES (1);");
        const TString select = "SELECT d, dt, ts, iv, d32, dt64, ts64, iv64, js, ys, uid, dec, jd, dn FROM ";
        CompareYson(SelectYson(db, select + "`/Root/DsTypes`"), SelectYson(db, select + "`/Root/CsTypes`"));
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

    Y_UNIT_TEST(ShowCreateKeepsStringDefaultWhenFlagOff) {
        auto settings = TKikimrSettings().SetWithSampleTables(false).SetColumnShardAlterObjectEnabled(true);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/CsLegacy` (
                Key Uint32 NOT NULL,
                Value Uint32,
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )");
        ExecScheme(db,
            "ALTER OBJECT `/Root/CsLegacy` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Value, `DEFAULT_VALUE`=`5`);");
        auto result = db.GetSession().GetValueSync().GetSession().ExecuteQuery(
            "SHOW CREATE TABLE `/Root/CsLegacy`;", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        const auto yson = FormatResultSetYson(result.GetResultSet(0));
        UNIT_ASSERT_STRING_CONTAINS(yson, "DEFAULT_VALUE");
        UNIT_ASSERT(!yson.Contains("DEFAULT 5"));
    }

    Y_UNIT_TEST(InvalidJsonDefaultRejected) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        ExpectSchemeError(db, R"(
            CREATE TABLE `/Root/BadJson` (
                Key Uint32 NOT NULL,
                Doc Json DEFAULT Json('not json'),
                PRIMARY KEY (Key)
            ) WITH (STORE = COLUMN);
        )", "Invalid value \"not json\" for type Json");
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

    Y_UNIT_TEST(DictionaryCompactionKeepsFill) {
        auto cs = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        cs->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        cs->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        cs->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        auto settings = DefaultsSettings();
        settings.AppConfig.MutableFeatureFlags()->SetEnableCsDictionaryEncoding(true);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DsDict` (
                Key Uint64 NOT NULL,
                Name Utf8,
                Value Utf8 DEFAULT 'born',
                PRIMARY KEY (Key)
            );
        )");
        ExecScheme(db, R"(
            CREATE TABLE `/Root/CsDict` (
                Key Uint64 NOT NULL,
                Name Utf8,
                Value Utf8 DEFAULT 'born' ENCODING(DICT),
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        )");
        ExecScheme(db,
            "ALTER OBJECT `/Root/CsDict` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`);");
        const auto insertBoth = [&](const TString& sql) {
            ExecDml(db, "INSERT INTO `/Root/DsDict` " + sql);
            ExecDml(db, "INSERT INTO `/Root/CsDict` " + sql);
        };
        insertBoth("(Key, Name) VALUES (1, 'a'), (2, 'b'), (3, 'c')");
        insertBoth("(Key, Name, Value) VALUES (4, 'd', 'x'), (5, 'e', 'x'), (6, 'f', 'x')");
        insertBoth("(Key, Name, Value) VALUES (7, 'g', NULL)");
        const auto compareAll = [&]() {
            for (const char* query : {
                     "SELECT Key, Value FROM $T ORDER BY Key",
                     "SELECT Value, COUNT(*) FROM $T GROUP BY Value ORDER BY Value",
                     "SELECT DISTINCT Value FROM $T ORDER BY Value",
                 }) {
                TString ds = query;
                TString csQuery = query;
                SubstGlobal(ds, "$T", "`/Root/DsDict`");
                SubstGlobal(csQuery, "$T", "`/Root/CsDict`");
                CompareYson(SelectYson(db, ds), SelectYson(db, csQuery));
            }
        };
        compareAll();
        UNIT_ASSERT(TOneCompactionCommand().Execute(kikimr).IsSuccess());
        compareAll();
        UNIT_ASSERT_STRING_CONTAINS(
            SelectYson(db, "SELECT SUM(Rows) FROM `/Root/CsDict/.sys/primary_index_stats` WHERE Activity == 1 AND EntityName = 'Value'"),
            "7u");
    }

    Y_UNIT_TEST(JsonDocumentAndDyNumberDefaults) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        const char* columns = R"(
            k Uint32 NOT NULL,
            jd JsonDocument DEFAULT JsonDocument('[2]'),
            dn DyNumber DEFAULT DyNumber('3.14'),
            PRIMARY KEY (k)
        )";
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/DsBin` (" << columns << ");");
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/CsBin` (" << columns << ") WITH (STORE = COLUMN);");
        const auto runBoth = [&](const TString& sql) {
            for (const char* side : {"Ds", "Cs"}) {
                TString query = sql;
                SubstGlobal(query, "$T", TStringBuilder() << "`/Root/" << side << "Bin`");
                ExecDml(db, query);
            }
        };
        const auto selectBoth = [&](const TString& sql) {
            TString ds = sql;
            TString cs = sql;
            SubstGlobal(ds, "$T", "`/Root/DsBin`");
            SubstGlobal(cs, "$T", "`/Root/CsBin`");
            CompareYson(SelectYson(db, ds), SelectYson(db, cs));
        };
        runBoth("UPSERT INTO $T (k) VALUES (1);");
        selectBoth("SELECT jd, dn FROM $T;");
        selectBoth("SELECT JSON_VALUE(jd, '$[0]') FROM $T;");
        for (const char* side : {"Ds", "Cs"}) {
            ExecScheme(db, TStringBuilder() << "ALTER TABLE `/Root/" << side
                << "Bin` ADD COLUMN jd2 JsonDocument DEFAULT JsonDocument('{\"a\":1}');");
        }
        selectBoth("SELECT jd2 FROM $T WHERE k = 1;");
        for (const char* side : {"Ds", "Cs"}) {
            ExecScheme(db, TStringBuilder() << "ALTER TABLE `/Root/" << side << "Bin` ALTER COLUMN dn SET DEFAULT DyNumber('7');");
        }
        runBoth("UPSERT INTO $T (k) VALUES (2);");
        selectBoth("SELECT k, dn FROM $T ORDER BY k;");
        auto shown = db.GetSession().GetValueSync().GetSession().ExecuteQuery(
            "SHOW CREATE TABLE `/Root/CsBin`;", NQuery::TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(shown.IsSuccess(), shown.GetIssues().ToString());
        const auto yson = FormatResultSetYson(shown.GetResultSet(0));
        UNIT_ASSERT_STRING_CONTAINS(yson, "JsonDocument(");
        UNIT_ASSERT_STRING_CONTAINS(yson, "DyNumber(");
    }

    void CreateDocTables(TQueryClient& db) {
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DsDoc` (
                Key Uint64 NOT NULL,
                Doc JsonDocument DEFAULT JsonDocument('{"a":"born","b":1}'),
                PRIMARY KEY (Key)
            );
        )");
        ExecScheme(db, R"(
            CREATE TABLE `/Root/CsDoc` (
                Key Uint64 NOT NULL,
                Doc JsonDocument DEFAULT JsonDocument('{"a":"born","b":1}'),
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        )");
        ExecScheme(db,
            "ALTER OBJECT `/Root/CsDoc` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`);");
        ExecScheme(db,
            "ALTER OBJECT `/Root/CsDoc` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `SCAN_READER_POLICY_NAME`=`SIMPLE`);");
        ExecScheme(db,
            "ALTER OBJECT `/Root/CsDoc` (TYPE TABLE) SET (ACTION=ALTER_COLUMN, NAME=Doc, `DATA_ACCESSOR_CONSTRUCTOR.CLASS_NAME`=`SUB_COLUMNS`);");
    }

    void CompareDoc(TQueryClient& db) {
        for (const char* query : {
                 "SELECT Key, Doc FROM $T ORDER BY Key",
                 "SELECT Key, JSON_VALUE(Doc, \"$.a\") FROM $T ORDER BY Key",
                 "SELECT Key FROM $T WHERE JSON_VALUE(Doc, \"$.a\") = \"born\" ORDER BY Key",
                 "SELECT Key FROM $T WHERE JSON_VALUE(Doc, \"$.a\") = \"later\" ORDER BY Key",
                 "SELECT Key FROM $T WHERE JSON_EXISTS(Doc, \"$.b\") ORDER BY Key",
             }) {
            TString ds = query;
            TString cs = query;
            SubstGlobal(ds, "$T", "`/Root/DsDoc`");
            SubstGlobal(cs, "$T", "`/Root/CsDoc`");
            CompareYson(SelectYson(db, ds), SelectYson(db, cs));
        }
    }

    Y_UNIT_TEST(SubColumnsDefaultMatchesRowTable) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        controller->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        controller->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        CreateDocTables(db);
        const auto insertBoth = [&](const TString& sql) {
            ExecDml(db, "INSERT INTO `/Root/DsDoc` " + sql);
            ExecDml(db, "INSERT INTO `/Root/CsDoc` " + sql);
        };
        insertBoth("(Key) VALUES (1), (2), (3)");
        insertBoth("(Key, Doc) VALUES (4, JsonDocument('{\"a\":\"x\"}')), (5, NULL)");
        CompareDoc(db);
        UNIT_ASSERT(TOneCompactionCommand().Execute(kikimr).IsSuccess());
        CompareDoc(db);
        UNIT_ASSERT_STRING_CONTAINS(
            SelectYson(db, "SELECT SUM(Rows) FROM `/Root/CsDoc/.sys/primary_index_stats` WHERE Activity == 1 AND EntityName = 'Doc'"),
            "5u");
    }

    Y_UNIT_TEST(SubColumnsSetDefaultActualization) {
        auto cs = NYDBTest::TControllers::RegisterCSControllerGuard<NOlap::TWaitCompactionController>();
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        CreateDocTables(db);
        // Separate inserts, so tiling++ has several portions to compact. One statement is one portion
        // and WaitCompactions never sees a compaction start.
        for (ui64 key = 1; key <= 3; ++key) {
            ExecDml(db, TStringBuilder() << "INSERT INTO `/Root/DsDoc` (Key) VALUES (" << key << ");");
            ExecDml(db, TStringBuilder() << "INSERT INTO `/Root/CsDoc` (Key) VALUES (" << key << ");");
        }
        UNIT_ASSERT(cs->WaitCompactions(TDuration::Seconds(20)));
        ExecScheme(db, "ALTER TABLE `/Root/DsDoc` ALTER COLUMN Doc SET DEFAULT JsonDocument('{\"a\":\"later\"}');");
        ExecScheme(db, "ALTER TABLE `/Root/CsDoc` ALTER COLUMN Doc SET DEFAULT JsonDocument('{\"a\":\"later\"}');");
        ExecScheme(db, "ALTER OBJECT `/Root/CsDoc` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, SCHEME_NEED_ACTUALIZATION=`true`);");
        const auto actualized = cs->GetActualizationsCount().Val();
        cs->WaitActualization(TDuration::Seconds(30), true);
        UNIT_ASSERT_C(cs->GetActualizationsCount().Val() > actualized, "SET DEFAULT did not actualize portions");
        ExecDml(db, "INSERT INTO `/Root/DsDoc` (Key) VALUES (6);");
        ExecDml(db, "INSERT INTO `/Root/CsDoc` (Key) VALUES (6);");
        CompareDoc(db);
    }

    Y_UNIT_TEST_TWIN(PrimaryKeyDefaultMatchesRowTable, CompileTimeDefaults) {
        auto settings = DefaultsSettings();
        settings.AppConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(CompileTimeDefaults);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        for (const char* side : {"Ds", "Cs"}) {
            const TString store = TString(side) == "Cs" ? "WITH (STORE = COLUMN)" : "";
            ExecScheme(db, TStringBuilder() << R"(
                CREATE TABLE `/Root/)" << side << R"(Pk` (
                    Key Int32 NOT NULL DEFAULT 42,
                    Name Utf8,
                    PRIMARY KEY (Key)
                ) )" << store << ";");
        }
        const auto runBoth = [&](const TString& sql) {
            for (const char* side : {"Ds", "Cs"}) {
                TString query = sql;
                SubstGlobal(query, "$T", TStringBuilder() << "`/Root/" << side << "Pk`");
                ExecDml(db, query);
            }
        };
        const auto selectBoth = [&](const TString& sql) {
            TString ds = sql;
            TString cs = sql;
            SubstGlobal(ds, "$T", "`/Root/DsPk`");
            SubstGlobal(cs, "$T", "`/Root/CsPk`");
            CompareYson(SelectYson(db, ds), SelectYson(db, cs));
        };
        const auto expectBoth = [&](const TString& message) {
            for (const char* side : {"Ds", "Cs"}) {
                ExpectDmlError(db, TStringBuilder() << "UPSERT INTO `/Root/" << side << "Pk` (Name) VALUES ('f');", message);
            }
        };
        runBoth("UPSERT INTO $T (Name) VALUES ('a');");
        runBoth("UPSERT INTO $T (Name) VALUES ('b');");
        selectBoth("SELECT Key, Name FROM $T ORDER BY Key;");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Name FROM `/Root/CsPk`"), "b");
        for (const char* side : {"Ds", "Cs"}) {
            ExpectDmlFailure(db, TStringBuilder() << "INSERT INTO `/Root/" << side << "Pk` (Name) VALUES ('c');");
        }
        selectBoth("SELECT Key, Name FROM $T ORDER BY Key;");
        runBoth("REPLACE INTO $T (Name) VALUES ('d');");
        selectBoth("SELECT Key, Name FROM $T ORDER BY Key;");
        runBoth("UPSERT INTO $T (Key, Name) VALUES (1, 'x');");
        selectBoth("SELECT Key, Name FROM $T ORDER BY Key;");
        for (const char* side : {"Ds", "Cs"}) {
            ExecScheme(db, TStringBuilder() << "ALTER TABLE `/Root/" << side << "Pk` ALTER COLUMN Key SET DEFAULT 43;");
        }
        runBoth("UPSERT INTO $T (Name) VALUES ('e');");
        selectBoth("SELECT Key, Name FROM $T ORDER BY Key;");
        UNIT_ASSERT_STRING_CONTAINS(SelectYson(db, "SELECT Key, Name FROM `/Root/CsPk` ORDER BY Key"), "e");
        for (const char* side : {"Ds", "Cs"}) {
            ExecScheme(db, TStringBuilder() << "ALTER TABLE `/Root/" << side << "Pk` ALTER COLUMN Key DROP DEFAULT;");
        }
        expectBoth("Missing key column");
        selectBoth("SELECT Key, Name FROM $T ORDER BY Key;");
    }

    Y_UNIT_TEST(CompositePrimaryKeyDefault) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        const char* columns = R"(
            A Uint32 NOT NULL,
            B Uint32 NOT NULL DEFAULT 5,
            Name Utf8,
            PRIMARY KEY (A, B)
        )";
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/DsPk2` (" << columns << ");");
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/CsPk2` (" << columns << ") WITH (STORE = COLUMN);");
        for (const char* side : {"Ds", "Cs"}) {
            ExecDml(db, TStringBuilder() << "UPSERT INTO `/Root/" << side << "Pk2` (A, Name) VALUES (1, 'p');");
            ExecDml(db, TStringBuilder() << "UPSERT INTO `/Root/" << side << "Pk2` (A, Name) VALUES (2, 'q');");
        }
        CompareYson(SelectYson(db, "SELECT A, B, Name FROM `/Root/DsPk2` ORDER BY A;"),
            SelectYson(db, "SELECT A, B, Name FROM `/Root/CsPk2` ORDER BY A;"));
    }

    Y_UNIT_TEST(BulkUpsertRequiresKeyWithDefault) {
        TKikimrRunner kikimr(DefaultsSettings());
        auto db = kikimr.GetQueryClient();
        auto table = kikimr.GetTableClient();
        const char* columns = R"(
            Key Uint32 NOT NULL DEFAULT 4,
            Value Utf8,
            PRIMARY KEY (Key)
        )";
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/DsBulkKey` (" << columns << ");");
        ExecScheme(db, TStringBuilder() << "CREATE TABLE `/Root/CsBulkKey` (" << columns << ") WITH (STORE = COLUMN);");
        const auto omittedKey = []() {
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Value").Utf8("x").EndStruct().EndList();
            return rows.Build();
        };
        for (const char* path : {"/Root/DsBulkKey", "/Root/CsBulkKey"}) {
            auto result = table.BulkUpsert(path, omittedKey()).ExtractValueSync();
            UNIT_ASSERT_C(!result.IsSuccess(), result.GetIssues().ToString());
            UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Missing key columns");
        }
    }

    Y_UNIT_TEST(IndexOnAbsentDefaultColumn) {
        auto controller = NYDBTest::TControllers::RegisterCSControllerGuard<NYDBTest::NColumnShard::TController>();
        controller->SetOverridePeriodicWakeupActivationPeriod(TDuration::Seconds(1));
        controller->SetOverrideLagForCompactionBeforeTierings(TDuration::Seconds(1));
        controller->DisableBackground(NYDBTest::ICSController::EBackground::Compaction);
        auto settings = DefaultsSettings();
        settings.AppConfig.MutableFeatureFlags()->SetEnableCsDictionaryEncoding(true);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        ExecScheme(db, R"(
            CREATE TABLE `/Root/DsIdx` (
                Key Uint64 NOT NULL,
                Extra Uint32 DEFAULT 5,
                PRIMARY KEY (Key)
            );
        )");
        ExecScheme(db, R"(
            CREATE TABLE `/Root/CsIdx` (
                Key Uint64 NOT NULL,
                Extra Uint32 DEFAULT 5,
                PRIMARY KEY (Key)
            )
            PARTITION BY HASH(Key)
            WITH (STORE = COLUMN, PARTITION_COUNT = 1);
        )");
        ExecScheme(db,
            "ALTER OBJECT `/Root/CsIdx` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `COMPACTION_PLANNER.CLASS_NAME`=`tiling++`);");
        ExecScheme(db,
            "ALTER OBJECT `/Root/CsIdx` (TYPE TABLE) SET (ACTION=UPSERT_OPTIONS, `INSERT_OPTIONS.BUILD_INDEXES_ENABLED`=`true`);");
        ExecScheme(db, R"(
            ALTER OBJECT `/Root/CsIdx` (TYPE TABLE) SET (ACTION=UPSERT_INDEX, NAME=extra_bloom, TYPE=BLOOM_FILTER,
                FEATURES=`{"column_name" : "Extra", "false_positive_probability" : 0.01}`);
        )");
        const auto replaceBoth = [&](const TString& sql) {
            ExecDml(db, "REPLACE INTO `/Root/DsIdx` " + sql);
            ExecDml(db, "REPLACE INTO `/Root/CsIdx` " + sql);
        };
        replaceBoth("(Key) VALUES (1), (2)");
        replaceBoth("(Key, Extra) VALUES (3, 7)");
        const auto compareAll = [&]() {
            for (const char* query : {
                     "SELECT Key FROM $T WHERE Extra = 5 ORDER BY Key",
                     "SELECT Key FROM $T WHERE Extra = 7 ORDER BY Key",
                     "SELECT Key, Extra FROM $T ORDER BY Key",
                 }) {
                TString ds = query;
                TString cs = query;
                SubstGlobal(ds, "$T", "`/Root/DsIdx`");
                SubstGlobal(cs, "$T", "`/Root/CsIdx`");
                CompareYson(SelectYson(db, ds), SelectYson(db, cs));
            }
        };
        compareAll();
        const auto indexStats = SelectYson(db,
            "SELECT COUNT(*) FROM `/Root/CsIdx/.sys/primary_index_stats` WHERE EntityName = 'extra_bloom'");
        UNIT_ASSERT_C(!indexStats.Contains("[[0u]]"), indexStats);
        UNIT_ASSERT(TOneCompactionCommand().Execute(kikimr).IsSuccess());
        compareAll();
    }

    Y_UNIT_TEST(BufferedBulkUpsertDifferentColumnSets) {
        auto settings = DefaultsSettings();
        settings.AppConfig.MutableColumnShardConfig()->SetWritingBufferDurationMs(3000);
        settings.AppConfig.MutableColumnShardConfig()->SetWritingBufferVolumeBytes(32_MB);
        TKikimrRunner kikimr(settings);
        auto db = kikimr.GetQueryClient();
        auto table = kikimr.GetTableClient();
        for (const char* name : {"CsBufSeq", "CsBufPar"}) {
            ExecScheme(db, TStringBuilder() << R"(
                CREATE TABLE `/Root/)" << name << R"(` (
                    Key Uint32 NOT NULL,
                    Value Utf8 DEFAULT 'bulk',
                    PRIMARY KEY (Key)
                )
                PARTITION BY HASH(Key)
                WITH (STORE = COLUMN, PARTITION_COUNT = 1);
            )");
            ExecDml(db, TStringBuilder() << "UPSERT INTO `/Root/" << name << "` (Key, Value) VALUES (1, 'nine');");
        }
        const auto batchA = []() {
            TValueBuilder rows;
            rows.BeginList().AddListItem().BeginStruct().AddMember("Key").Uint32(2).AddMember("Value").Utf8("x").EndStruct().EndList();
            return rows.Build();
        };
        const auto batchB = []() {
            TValueBuilder rows;
            rows.BeginList()
                .AddListItem().BeginStruct().AddMember("Key").Uint32(1).EndStruct()
                .AddListItem().BeginStruct().AddMember("Key").Uint32(3).EndStruct()
                .EndList();
            return rows.Build();
        };
        {
            auto written = table.BulkUpsert("/Root/CsBufSeq", batchA()).ExtractValueSync();
            UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
            written = table.BulkUpsert("/Root/CsBufSeq", batchB()).ExtractValueSync();
            UNIT_ASSERT_C(written.IsSuccess(), written.GetIssues().ToString());
        }
        {
            auto first = table.BulkUpsert("/Root/CsBufPar", batchA());
            auto second = table.BulkUpsert("/Root/CsBufPar", batchB());
            auto firstResult = first.ExtractValueSync();
            auto secondResult = second.ExtractValueSync();
            UNIT_ASSERT_C(firstResult.IsSuccess(), firstResult.GetIssues().ToString());
            UNIT_ASSERT_C(secondResult.IsSuccess(), secondResult.GetIssues().ToString());
        }
        const TString query = "SELECT Key, Value FROM $T ORDER BY Key;";
        TString seq = query;
        TString par = query;
        SubstGlobal(seq, "$T", "`/Root/CsBufSeq`");
        SubstGlobal(par, "$T", "`/Root/CsBufPar`");
        const auto seqRows = SelectYson(db, seq);
        CompareYson(seqRows, SelectYson(db, par));
        UNIT_ASSERT_STRING_CONTAINS(seqRows, "[\"x\"]");
        UNIT_ASSERT_STRING_CONTAINS(seqRows, "[\"bulk\"]");
        CompareYson(R"([[2u;["x"]]])", SelectYson(db, "SELECT Key, Value FROM `/Root/CsBufSeq` WHERE Key = 2;"));
        CompareYson(R"([[3u;["bulk"]]])", SelectYson(db, "SELECT Key, Value FROM `/Root/CsBufSeq` WHERE Key = 3;"));
    }

}

} // namespace NKikimr::NKqp
