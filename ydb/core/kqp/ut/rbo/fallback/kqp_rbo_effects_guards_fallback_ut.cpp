#include <ydb/core/kqp/ut/rbo/effects/kqp_effects_guard_helpers.h>

namespace NKikimr::NKqp {

using namespace NYdb;
using namespace NYdb::NQuery;
using namespace NEffectsGuardTests;

Y_UNIT_TEST_SUITE(KqpRboEffectsGuardsFallback) {
    Y_UNIT_TEST(SelectAndWriteSharedRandomInput) {
        // A result and an effect consume the same random row producer. The guard
        // must detect sharing across these root kinds and route to legacy planning.
        TKikimrRunner kikimr(TKikimrSettings(GuardConfig()).SetWithSampleTables(false));
        CreateTables(kikimr);
        auto client = kikimr.GetQueryClient();
        auto result = client.ExecuteQuery(R"(
            $rows = SELECT 1ul AS k, RandomNumber(1) AS v;
            SELECT k, v FROM $rows;
            UPSERT INTO A SELECT k, v FROM $rows;
        )", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "shared non-repeatable input");
        auto stored = client.ExecuteQuery("SELECT k, Unwrap(v) AS v FROM A;", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(stored.IsSuccess(), stored.GetIssues().ToString());
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Compilation with the new RBO failed");
        UNIT_ASSERT_VALUES_EQUAL(stored.GetResultSet(0).RowsCount(), 1);
        // Legacy reads and writes can evaluate the input in separate phases.
        // Check routing and both results without requiring equal random values.
        UNIT_ASSERT_VALUES_EQUAL(result.GetResultSet(0).RowsCount(), 1);
        UNIT_ASSERT(!FormatResultSetYson(result.GetResultSet(0)).Contains("#"));
        UNIT_ASSERT(!FormatResultSetYson(stored.GetResultSet(0)).Contains("#"));
    }

    Y_UNIT_TEST(GeneratedPartialUpsert) {
        auto config = GuardConfig();
        config.MutableFeatureFlags()->SetEnableGeneratedStored(true);
        TKikimrRunner kikimr(TKikimrSettings(config).SetWithSampleTables(false));
        auto client = kikimr.GetQueryClient();
        auto schema = client.ExecuteQuery(R"(
            CREATE TABLE G (
                k Int32 NOT NULL,
                dep Int32 DEFAULT 7,
                g Int32 NOT NULL GENERATED ALWAYS AS (k * 100 + COALESCE(dep, 0)) STORED,
                PRIMARY KEY (k)
            );
        )", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(schema.IsSuccess(), schema.GetIssues().ToString());
        auto seed = client.ExecuteQuery("UPSERT INTO G (k, dep) VALUES (1, 5);", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(seed.IsSuccess(), seed.GetIssues().ToString());
        auto result = client.ExecuteQuery("UPSERT INTO G (k) VALUES (1), (2);", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(),
            "New RBO cannot import a table-effect Map whose body is not AsStruct");
        auto stored = client.ExecuteQuery("SELECT k, dep, g FROM G ORDER BY k;", TTxControl::NoTx()).GetValueSync();
        UNIT_ASSERT_C(stored.IsSuccess(), stored.GetIssues().ToString());
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Compilation with the new RBO failed");
        CompareYson("[[1;[5];105];[2;[7];207]]", FormatResultSetYson(stored.GetResultSet(0)));
    }

    Y_UNIT_TEST(SingleReturningWithoutStreamIndex) {
        TKikimrRunner kikimr(TKikimrSettings(GuardConfig(true, false)).SetWithSampleTables(false));
        CreateTables(kikimr);
        auto client = kikimr.GetQueryClient();
        auto seed = client.ExecuteQuery("UPSERT INTO A (k, v) VALUES (1ul, 10ul);", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(seed.IsSuccess(), seed.GetIssues().ToString());
        auto result = client.ExecuteQuery("DELETE FROM A ON (k) VALUES (1ul) RETURNING k, v;",
            TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Returning table sink requires returning-column metadata");
        auto stored = client.ExecuteQuery("SELECT k, v FROM A;", TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(stored.IsSuccess(), stored.GetIssues().ToString());
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
        UNIT_ASSERT_STRING_CONTAINS(result.GetIssues().ToString(), "Compilation with the new RBO failed");
        CompareYson("[[1u;[10u]]]", FormatResultSetYson(result.GetResultSet(0)));
        UNIT_ASSERT_VALUES_EQUAL(stored.GetResultSet(0).RowsCount(), 0);
    }
}

} // namespace NKikimr::NKqp
