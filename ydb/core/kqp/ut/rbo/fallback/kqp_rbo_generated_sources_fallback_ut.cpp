#include <ydb/core/kqp/ut/rbo/effects/kqp_rbo_generated_helpers.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NGeneratedEffects;

Y_UNIT_TEST_SUITE(KqpRboGeneratedSourcesFallback) {
    Y_UNIT_TEST_TWIN(StoredSourceSelectInsertUpsertReplace, NewRbo) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo);
        TTestFixture fixture(false, R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (2, 2, 7), (4, 4, 9);", appConfig);
        const auto failuresBefore = fixture.GetRboCompileFailures();
        fixture.Exec(R"(
            CREATE TABLE SourceRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec(R"(
            UPSERT INTO SourceRows (k, a, b) VALUES
                (1, 1, 2), (2, 5, 90), (3, 6, 80), (4, 8, 70);
        )");

        fixture.Exec(R"(
            INSERT INTO TargetRows (k, a, b)
            SELECT k, a, b FROM SourceRows WHERE k = 1;
        )");
        fixture.Exec(R"(
            UPSERT INTO TargetRows (k, a)
            SELECT k, a FROM SourceRows WHERE k IN (2, 3);
        )");
        fixture.Exec(R"(
            REPLACE INTO TargetRows (k, a)
            SELECT k, a FROM SourceRows WHERE k = 4;
        )");

        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;", "[[1;[1];[2];12];[2;[5];[7];57];[3;[6];#;60];[4;[8];#;80]]");
        if constexpr (NewRbo) {
            UNIT_ASSERT_C(fixture.GetRboCompileFailures() > failuresBefore, "expected a legacy optimizer retry");
        }
    }

    Y_UNIT_TEST_TWIN(StoredSourceAsTableInsertUpsertReplaceAndMixedRows, NewRbo) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo);
        TTestFixture fixture(false, R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (11, 2, 7), (13, 4, 9);", appConfig);
        const auto failuresBefore = fixture.GetRboCompileFailures();
        auto& session = fixture.QuerySession();

        const auto params = TParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(10)
                        .AddMember("a").Int32(1)
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(11)
                        .AddMember("a").Int32(5)
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(12)
                        .AddMember("a").Int32(6)
                    .EndStruct()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(13)
                        .AddMember("a").Int32(8)
                    .EndStruct()
                .EndList()
            .Build()
            .Build();

        auto insert = session.ExecuteQuery(R"(
            DECLARE $rows AS List<Struct<k:Int32,a:Int32>>;
            INSERT INTO TargetRows (k, a)
            SELECT k, a FROM AS_TABLE($rows) WHERE k = 10;
        )", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());

        auto upsert = session.ExecuteQuery(R"(
            DECLARE $rows AS List<Struct<k:Int32,a:Int32>>;
            UPSERT INTO TargetRows (k, a)
            SELECT k, a FROM AS_TABLE($rows) WHERE k IN (11, 12);
        )", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());
        if constexpr (NewRbo) {
            UNIT_ASSERT_STRING_CONTAINS(upsert.GetIssues().ToString(), "Compilation with the new RBO failed");
        }

        auto replace = session.ExecuteQuery(R"(
            DECLARE $rows AS List<Struct<k:Int32,a:Int32>>;
            REPLACE INTO TargetRows (k, a)
            SELECT k, a FROM AS_TABLE($rows) WHERE k = 13;
        )", TTxControl::NoTx(), params).ExtractValueSync();
        UNIT_ASSERT_C(replace.IsSuccess(), replace.GetIssues().ToString());

        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;",
            "[[10;[1];#;10];[11;[5];[7];57];[12;[6];#;60];[13;[8];#;80]]");
        if constexpr (NewRbo) {
            UNIT_ASSERT_C(fixture.GetRboCompileFailures() > failuresBefore, "expected a legacy optimizer retry");
        }
    }

    Y_UNIT_TEST_TWIN(StoredSourceScalarParameters, NewRbo) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo);
        TTestFixture fixture(false, R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (2, 2, 7), (3, 3, 9);", appConfig);
        const auto failuresBefore = fixture.GetRboCompileFailures();
        auto& session = fixture.QuerySession();

        auto insertParams = TParamsBuilder()
            .AddParam("$k").Int32(1).Build()
            .AddParam("$a").Int32(1).Build()
            .AddParam("$b").Int32(2).Build()
            .Build();
        auto insert = session.ExecuteQuery(R"(
            DECLARE $k AS Int32;
            DECLARE $a AS Int32;
            DECLARE $b AS Int32;
            INSERT INTO TargetRows (k, a, b) VALUES ($k, $a, $b);
        )", TTxControl::NoTx(), insertParams).ExtractValueSync();
        UNIT_ASSERT_C(insert.IsSuccess(), insert.GetIssues().ToString());

        auto upsertParams = TParamsBuilder()
            .AddParam("$k").Int32(2).Build()
            .AddParam("$a").Int32(5).Build()
            .Build();
        auto upsert = session.ExecuteQuery(R"(
            DECLARE $k AS Int32;
            DECLARE $a AS Int32;
            UPSERT INTO TargetRows (k, a) VALUES ($k, $a);
        )", TTxControl::NoTx(), upsertParams).ExtractValueSync();
        UNIT_ASSERT_C(upsert.IsSuccess(), upsert.GetIssues().ToString());

        auto replaceParams = TParamsBuilder()
            .AddParam("$k").Int32(3).Build()
            .AddParam("$a").Int32(8).Build()
            .Build();
        auto replace = session.ExecuteQuery(R"(
            DECLARE $k AS Int32;
            DECLARE $a AS Int32;
            REPLACE INTO TargetRows (k, a) VALUES ($k, $a);
        )", TTxControl::NoTx(), replaceParams).ExtractValueSync();
        UNIT_ASSERT_C(replace.IsSuccess(), replace.GetIssues().ToString());

        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;", "[[1;[1];[2];12];[2;[5];[7];57];[3;[8];#;80]]");
        if constexpr (NewRbo) {
            UNIT_ASSERT_C(fixture.GetRboCompileFailures() > failuresBefore, "expected a legacy optimizer retry");
        }
    }

    Y_UNIT_TEST_TWIN(StoredSourceEmptyInputsAreNoOp, NewRbo) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo);
        TTestFixture fixture(false, R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (1, 1, 2);", appConfig);
        const auto failuresBefore = fixture.GetRboCompileFailures();
        fixture.Exec(R"(
            CREATE TABLE SourceRows (
                k Int32 NOT NULL,
                a Int32,
                PRIMARY KEY (k)
            );
        )");
        fixture.Exec("UPSERT INTO SourceRows (k, a) VALUES (2, 2);");

        fixture.Exec(R"(
            INSERT INTO TargetRows (k, a) SELECT k, a FROM SourceRows WHERE k < 0;
        )");
        fixture.Exec(R"(
            UPSERT INTO TargetRows (k, a) SELECT k, a FROM SourceRows WHERE k < 0;
        )");
        fixture.Exec(R"(
            REPLACE INTO TargetRows (k, a) SELECT k, a FROM SourceRows WHERE k < 0;
        )");

        auto& session = fixture.QuerySession();
        const auto params = TParamsBuilder()
            .AddParam("$rows")
                .BeginList()
                    .AddListItem().BeginStruct()
                        .AddMember("k").Int32(3)
                        .AddMember("a").Int32(3)
                    .EndStruct()
                .EndList()
            .Build()
            .Build();
        for (const TStringBuf operation : {"INSERT", "UPSERT", "REPLACE"}) {
            const TString query = TStringBuilder() << R"(
                DECLARE $rows AS List<Struct<k:Int32,a:Int32>>;
            )" << operation << R"( INTO TargetRows (k, a)
                SELECT k, a FROM AS_TABLE($rows) WHERE k < 0;
            )";
            auto result = session.ExecuteQuery(query, TTxControl::NoTx(), params).ExtractValueSync();
            UNIT_ASSERT_C(result.IsSuccess(), operation << ": " << result.GetIssues().ToString());
        }

        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;", "[[1;[1];[2];12]]");
        if constexpr (NewRbo) {
            UNIT_ASSERT_C(fixture.GetRboCompileFailures() > failuresBefore, "expected a legacy optimizer retry");
        }
    }

    Y_UNIT_TEST_TWIN(StoredSourceDuplicateKeysAndInsertConflictAreAtomic, NewRbo) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo);
        TTestFixture fixture(false, R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (1, 1, 2);", appConfig);
        const auto failuresBefore = fixture.GetRboCompileFailures();
        auto& session = fixture.QuerySession();

        auto duplicateInsert = session.ExecuteQuery(R"(
            INSERT INTO TargetRows (k, a) VALUES (2, 2), (2, 3);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(duplicateInsert.GetStatus(), EStatus::PRECONDITION_FAILED, duplicateInsert.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;", "[[1;[1];[2];12]]");

        fixture.Exec("UPSERT INTO TargetRows (k, a) VALUES (2, 3), (2, 4);");
        fixture.Check("SELECT k, a, b, g FROM TargetRows WHERE k = 2;", "[[2;[4];#;40]]");

        auto existingConflict = session.ExecuteQuery(R"(
            INSERT INTO TargetRows (k, a, b) VALUES (3, 3, 4), (1, 9, 9);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(existingConflict.GetStatus(), EStatus::PRECONDITION_FAILED, existingConflict.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TargetRows ORDER BY k;",
            "[[1;[1];[2];12];[2;[4];#;40]]");
        if constexpr (NewRbo) {
            UNIT_ASSERT_C(fixture.GetRboCompileFailures() > failuresBefore, "expected a legacy optimizer retry");
        }
    }

    Y_UNIT_TEST_TWIN(CompileTimeDefaultsPartialUpsertAndReplace, NewRbo) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(true);

        TTestFixture fixture(false, R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                dep Int32 DEFAULT 7,
                g Int32 NOT NULL GENERATED ALWAYS AS (k * 100 + COALESCE(dep, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TestTable (k, dep) VALUES (1, 5);", appConfig);
        const auto failuresBefore = fixture.GetRboCompileFailures();

        // The existing row preserves dep=5 while the new row receives DEFAULT 7 in one request.
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (1), (2);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;[5];105];[2;[7];207]]");

        // REPLACE constructs the complete row, so an omitted dependency receives its default
        // for both an existing and a new row.
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1), (3);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;[7];107];[2;[7];207];[3;[7];307]]");
        if constexpr (NewRbo) {
            UNIT_ASSERT_C(fixture.GetRboCompileFailures() > failuresBefore, "expected a legacy optimizer retry");
        }
    }

    Y_UNIT_TEST_TWIN(CompileTimeDefaultsAlterDependencySetAndDrop, NewRbo) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(true);

        TTestFixture fixture(false, R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                dep Int32,
                g Int32 NOT NULL
                    GENERATED ALWAYS AS (COALESCE(dep, 0) + 1) STORED,
                PRIMARY KEY (k)
            );
        )", "", appConfig);
        const auto failuresBefore = fixture.GetRboCompileFailures();

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN dep SET DEFAULT 40;");
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1);");
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (2);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;[40];41];[2;[40];41]]");

        fixture.Exec("ALTER TABLE TestTable ALTER COLUMN dep DROP DEFAULT;");
        fixture.Exec("REPLACE INTO TestTable (k) VALUES (1);");
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (3);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;#;1];[2;[40];41];[3;#;1]]");
        if constexpr (NewRbo) {
            UNIT_ASSERT_C(fixture.GetRboCompileFailures() > failuresBefore, "expected a legacy optimizer retry");
        }
    }

    Y_UNIT_TEST_TWIN(CompileTimeDefaultsNonKeySerialDependency, NewRbo) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableTableServiceConfig()->SetEnableNewRBO(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableFallbackToYqlOptimizer(NewRbo);
        appConfig.MutableTableServiceConfig()->SetEnableCompileTimeDefaults(true);

        TTestFixture fixture(false, R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                dep Serial,
                g Int32 NOT NULL GENERATED ALWAYS AS (dep * 10) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TestTable (k, dep) VALUES (1, 50);", appConfig);
        const auto failuresBefore = fixture.GetRboCompileFailures();

        // A generated sequence value is consumed for each input row. The existing row keeps dep=50,
        // while the new row receives the second sequence value.
        fixture.Exec("UPSERT INTO TestTable (k) VALUES (1), (2);");
        fixture.Exec("INSERT INTO TestTable (k) VALUES (3);");
        fixture.Check("SELECT k, dep, g FROM TestTable ORDER BY k;", "[[1;50;500];[2;2;20];[3;3;30]]");
        if constexpr (NewRbo) {
            UNIT_ASSERT_C(fixture.GetRboCompileFailures() > failuresBefore, "expected a legacy optimizer retry");
        }
    }
}

} // namespace NKikimr::NKqp
