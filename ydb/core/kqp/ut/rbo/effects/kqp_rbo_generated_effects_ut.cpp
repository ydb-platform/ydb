#include "kqp_rbo_generated_helpers.h"

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NGeneratedEffects;

// Ported from ut/scheme/kqp_generated_columns_ut.cpp.
Y_UNIT_TEST_SUITE(KqpRboGeneratedEffects) {

    Y_UNIT_TEST_TWIN(RandomGeneratedConsistentWithIndex, NewRbo) {
        TTestFixture fixture(NewRbo, R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32,
                r Uint64 GENERATED ALWAYS AS (RandomNumber(1)) STORED,
                PRIMARY KEY (k),
                INDEX idx_a GLOBAL SYNC ON (a) COVER (r)
            );
        )");

        fixture.Exec("INSERT INTO TestTable (k, a) VALUES (1, 10), (2, 20), (3, 30);");

        const TString fromTable = fixture.QueryYson("SELECT a, r FROM TestTable ORDER BY a;");
        const TString fromIndex = fixture.QueryYson("SELECT a, r FROM TestTable VIEW idx_a ORDER BY a;");

        UNIT_ASSERT_C(!fromTable.Contains("#"), "random generated column is NULL in base table: " << fromTable);
        UNIT_ASSERT_VALUES_EQUAL_C(fromIndex, fromTable,
            "non-deterministic generated value diverged between the base table and the covering index");
    }

    Y_UNIT_TEST_TWIN(UpdateExpressionsUseOldValuesAndRecomputeStoredColumn, NewRbo) {
        TTestFixture fixture(NewRbo, R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                b Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a * 10 + b) STORED,
                PRIMARY KEY (k)
            );
        )", R"(
            INSERT INTO TestTable (k, a, b) VALUES
                (1, 1, 2),
                (2, 3, 4),
                (3, 5, 6);
        )");

        fixture.Exec("UPDATE TestTable SET a = a + 1 WHERE k = 1;");
        fixture.Exec("UPDATE TestTable SET a = b, b = a WHERE k = 2;");
        fixture.Exec("UPDATE TestTable SET a = g WHERE k = 3;");

        // Every RHS uses the old row. g is then evaluated once from the final dependency values.
        fixture.Check("SELECT k, a, b, g FROM TestTable ORDER BY k;", "[[1;2;2;22];[2;4;3;43];[3;56;6;566]]");
    }

    Y_UNIT_TEST_TWIN(UniqueIndexOnGeneratedColumnRejectsCollisionsAtomically, NewRbo) {
        auto appConfig = GeneratedColumnsAppConfig();
        appConfig.MutableFeatureFlags()->SetEnableAddUniqueIndex(true);

        TTestFixture fixture(NewRbo, R"(
            CREATE TABLE TestTable (
                k Int32 NOT NULL,
                a Int32 NOT NULL,
                g Int32 NOT NULL GENERATED ALWAYS AS (a) STORED,
                PRIMARY KEY (k),
                INDEX idx_g GLOBAL UNIQUE SYNC ON (g)
            );
        )", R"(
            INSERT INTO TestTable (k, a) VALUES (1, 10), (2, 20), (3, 30);
        )", appConfig);

        const auto checkUnchanged = [&] {
            fixture.Check("SELECT k, a, g FROM TestTable ORDER BY k;", "[[1;10;10];[2;20;20];[3;30;30]]");
            fixture.Check("SELECT k, a, g FROM TestTable VIEW idx_g ORDER BY g;", "[[1;10;10];[2;20;20];[3;30;30]]");
        };
        checkUnchanged();

        // One row is valid and one collides with an existing generated key. Neither may persist.
        auto result = fixture.QuerySession().ExecuteQuery(R"(
            INSERT INTO TestTable (k, a) VALUES (4, 40), (5, 10);
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        checkUnchanged();

        // A collision produced entirely inside one input batch must also revert the full batch.
        result = fixture.QuerySession().ExecuteQuery(R"(
            INSERT INTO TestTable (k, a) VALUES (4, 40), (5, 40);
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        checkUnchanged();

        // k=2 collides with k=1 while k=3 would move to a free key. The whole UPDATE rolls back.
        result = fixture.QuerySession().ExecuteQuery(R"(
            UPDATE TestTable
            SET a = CASE k WHEN 2 THEN 10 ELSE 40 END
            WHERE k IN (2, 3);
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(result.GetStatus(), EStatus::PRECONDITION_FAILED, result.GetIssues().ToString());
        checkUnchanged();
    }

    Y_UNIT_TEST_TWIN(MultipleDmlStatementsCommitAndAbortAtomically, NewRbo) {
        TTestFixture fixture(NewRbo, R"(
            CREATE TABLE TxStored (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )");
        auto& session = fixture.QuerySession();

        auto success = session.ExecuteQuery(R"(
            INSERT INTO TxStored (k, a, b) VALUES (1, 1, 2), (2, 2, 3);
            UPDATE TxStored SET a = 5 WHERE k = 1;
            DELETE FROM TxStored WHERE g = 23;
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_C(success.IsSuccess(), success.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TxStored ORDER BY k;", "[[1;[5];[2];52]]");

        auto failure = session.ExecuteQuery(R"(
            INSERT INTO TxStored (k, a, b) VALUES (3, 3, 4);
            INSERT INTO TxStored (k, a, b) VALUES (1, 9, 9);
        )", TTxControl::BeginTx(TTxSettings::SerializableRW()).CommitTx()).ExtractValueSync();
        UNIT_ASSERT_VALUES_EQUAL_C(failure.GetStatus(), EStatus::PRECONDITION_FAILED, failure.GetIssues().ToString());
        UNIT_ASSERT(!failure.GetTransaction() || !failure.GetTransaction()->IsActive());
        fixture.Check("SELECT k, a, b, g FROM TxStored ORDER BY k;", "[[1;[5];[2];52]]");
    }

} // suite

} // namespace NKikimr::NKqp
