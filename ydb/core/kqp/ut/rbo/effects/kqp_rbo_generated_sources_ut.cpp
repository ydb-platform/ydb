#include <ydb/core/kqp/ut/rbo/effects/kqp_rbo_generated_helpers.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NGeneratedEffects;

Y_UNIT_TEST_SUITE(KqpRboGeneratedSources) {
    Y_UNIT_TEST_TWIN(StoredSourceInsertOrRevertSuccessAndConflicts, NewRbo) {
        TTestFixture fixture(NewRbo, R"(
            CREATE TABLE TargetRows (
                k Int32 NOT NULL,
                a Int32,
                b Int32,
                g Int32 NOT NULL GENERATED ALWAYS AS (COALESCE(a, 0) * 10 + COALESCE(b, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", "UPSERT INTO TargetRows (k, a, b) VALUES (1, 1, 2);");
        auto& session = fixture.QuerySession();

        auto success = session.ExecuteQuery(R"(
            INSERT OR REVERT INTO TargetRows (k, a, b) VALUES (2, 2, 3), (3, 3, 4);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(success.IsSuccess(), success.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TargetRows WHERE k IN (2, 3) ORDER BY k;", "[[2;[2];[3];23];[3;[3];[4];34]]");

        auto duplicate = session.ExecuteQuery(R"(
            INSERT OR REVERT INTO TargetRows (k, a) VALUES (4, 4), (5, 5), (4, 6);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(duplicate.IsSuccess(), duplicate.GetIssues().ToString());
        fixture.Check("SELECT k, g FROM TargetRows WHERE k IN (4, 5) ORDER BY k;", "[]");

        auto conflict = session.ExecuteQuery(R"(
            INSERT OR REVERT INTO TargetRows (k, a) VALUES (1, 9), (6, 6);
        )", TTxControl::NoTx()).ExtractValueSync();
        UNIT_ASSERT_C(conflict.IsSuccess(), conflict.GetIssues().ToString());
        fixture.Check("SELECT k, a, b, g FROM TargetRows WHERE k IN (1, 6) ORDER BY k;", "[[1;[1];[2];12]]");
    }
}

} // namespace NKikimr::NKqp
