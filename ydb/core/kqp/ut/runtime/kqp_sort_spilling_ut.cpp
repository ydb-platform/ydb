#include <ydb/core/kqp/ut/common/kqp_ut_common.h>
#include <ydb/core/kqp/counters/kqp_counters.h>

#include <ydb/library/yql/dq/actors/compute/dq_compute_actor.h>

#include <util/system/fs.h>

namespace NKikimr {
namespace NKqp {

using namespace NYdb;
using namespace NYdb::NTable;

namespace {

NKikimrConfig::TAppConfig AppCfgWithSortSpilling(double spillingPercent = 0.01) {
    NKikimrConfig::TAppConfig appCfg;

    auto* ts = appCfg.MutableTableServiceConfig();
    ts->SetEnableQueryServiceSpilling(true);

    auto* rm = ts->MutableResourceManager();
    rm->SetMkqlLightProgramMemoryLimit(100);
    rm->SetMkqlHeavyProgramMemoryLimit(300);
    rm->SetSpillingPercent(spillingPercent);

    auto* spilling = ts->MutableSpillingServiceConfig()->MutableLocalFileConfig();
    spilling->SetEnable(true);
    spilling->SetRoot("./spilling/");

    return appCfg;
}

void CreateSortTestTable(NQuery::TQueryClient& db) {
    auto status = db.ExecuteQuery(
        R"(
            CREATE TABLE `/Root/sort_test` (
                Key Uint64 NOT NULL,
                SortKey String NOT NULL,
                Payload String NOT NULL,
                PRIMARY KEY (Key)
            );
        )", NYdb::NQuery::TTxControl::NoTx()
    ).GetValueSync();
    UNIT_ASSERT_C(status.IsSuccess(), status.GetIssues().ToString());
}

void FillSortTestTable(NQuery::TQueryClient& db, ui64 numRows = 200) {
    for (ui64 i = 0; i < numRows; ++i) {
        // Create large payload to trigger memory pressure and spilling
        auto sortKey = Sprintf("sort_key_%06lu", (unsigned long)(numRows - i));
        auto payload = TString(100000 + (i % 100) * 1000, 'a' + (i % 26));
        auto result = db.ExecuteQuery(Sprintf(R"(
            --!syntax_v1
            REPLACE INTO `/Root/sort_test` (Key, SortKey, Payload) VALUES (%lu, "%s", "%s")
        )", (unsigned long)i, sortKey.data(), payload.data()), NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
        UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());
    }
}

} // anonymous namespace

Y_UNIT_TEST_SUITE(KqpSortSpilling) {

Y_UNIT_TEST(WideSortWithSpilling) {
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;
    TKikimrRunner kikimr(AppCfgWithSortSpilling());

    auto db = kikimr.GetQueryClient();

    CreateSortTestTable(db);
    FillSortTestTable(db);

    // Simple ORDER BY that triggers WideSort
    auto query = R"(
        --!syntax_v1
        SELECT Key, SortKey, Payload
        FROM `/Root/sort_test`
        ORDER BY SortKey ASC
    )";

    auto explainMode = NYdb::NQuery::TExecuteQuerySettings().ExecMode(NYdb::NQuery::EExecMode::Explain);
    auto planres = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::NoTx(), explainMode).ExtractValueSync();
    UNIT_ASSERT_VALUES_EQUAL_C(planres.GetStatus(), EStatus::SUCCESS, planres.GetIssues().ToString());
    Cerr << "AST: " << planres.GetStats()->GetAst() << Endl;

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto resultSet = result.GetResultSets()[0];
    UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 200);

    // Verify sort order: SortKey should be ascending
    NYdb::TResultSetParser parser(resultSet);
    TString prevSortKey;
    while (parser.TryNextRow()) {
        auto sortKey = TString(*parser.ColumnParser("SortKey").GetOptionalString());
        if (!prevSortKey.empty()) {
            UNIT_ASSERT_C(sortKey >= prevSortKey,
                TStringBuilder() << "Sort order violated: " << sortKey << " < " << prevSortKey);
        }
        prevSortKey = sortKey;
    }

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    Cerr << "ComputeSpilling.WriteBlobs: " << counters.ComputeSpilling.WriteBlobs->Val() << Endl;
    Cerr << "ComputeSpilling.ReadBlobs: " << counters.ComputeSpilling.ReadBlobs->Val() << Endl;
    UNIT_ASSERT_C(counters.ComputeSpilling.WriteBlobs->Val() > 0,
        "Expected spilling to occur during sort (WriteBlobs should be > 0)");
    UNIT_ASSERT_C(counters.ComputeSpilling.ReadBlobs->Val() > 0,
        "Expected spilling to occur during sort (ReadBlobs should be > 0)");
}

Y_UNIT_TEST(WideSortDescWithSpilling) {
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;
    TKikimrRunner kikimr(AppCfgWithSortSpilling());

    auto db = kikimr.GetQueryClient();

    CreateSortTestTable(db);
    FillSortTestTable(db);

    // ORDER BY DESC
    auto query = R"(
        --!syntax_v1
        SELECT Key, SortKey, Payload
        FROM `/Root/sort_test`
        ORDER BY SortKey DESC
    )";

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto resultSet = result.GetResultSets()[0];
    UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 200);

    // Verify sort order: SortKey should be descending
    NYdb::TResultSetParser parser(resultSet);
    TString prevSortKey;
    while (parser.TryNextRow()) {
        auto sortKey = TString(*parser.ColumnParser("SortKey").GetOptionalString());
        if (!prevSortKey.empty()) {
            UNIT_ASSERT_C(sortKey <= prevSortKey,
                TStringBuilder() << "Sort order violated: " << sortKey << " > " << prevSortKey);
        }
        prevSortKey = sortKey;
    }

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    UNIT_ASSERT_C(counters.ComputeSpilling.WriteBlobs->Val() > 0,
        "Expected spilling to occur during sort (WriteBlobs should be > 0)");
    UNIT_ASSERT_C(counters.ComputeSpilling.ReadBlobs->Val() > 0,
        "Expected spilling to occur during sort (ReadBlobs should be > 0)");
}

Y_UNIT_TEST(WideSortMultiKeyWithSpilling) {
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;
    TKikimrRunner kikimr(AppCfgWithSortSpilling());

    auto db = kikimr.GetQueryClient();

    CreateSortTestTable(db);
    FillSortTestTable(db);

    // Multi-key ORDER BY
    auto query = R"(
        --!syntax_v1
        SELECT Key, SortKey, Payload
        FROM `/Root/sort_test`
        ORDER BY SortKey ASC, Key DESC
    )";

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto resultSet = result.GetResultSets()[0];
    UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 200);

    // Verify sort order
    NYdb::TResultSetParser parser(resultSet);
    TString prevSortKey;
    std::optional<ui64> prevKey;
    while (parser.TryNextRow()) {
        auto sortKey = TString(*parser.ColumnParser("SortKey").GetOptionalString());
        auto key = parser.ColumnParser("Key").GetOptionalUint64().value();
        if (!prevSortKey.empty()) {
            if (sortKey == prevSortKey) {
                // Same sort key: Key should be descending
                UNIT_ASSERT_C(key <= *prevKey,
                    TStringBuilder() << "Secondary sort order violated: key " << key << " > " << *prevKey
                    << " for SortKey=" << sortKey);
            } else {
                UNIT_ASSERT_C(sortKey > prevSortKey,
                    TStringBuilder() << "Primary sort order violated: " << sortKey << " < " << prevSortKey);
            }
        }
        prevSortKey = sortKey;
        prevKey = key;
    }

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    UNIT_ASSERT_C(counters.ComputeSpilling.WriteBlobs->Val() > 0,
        "Expected spilling to occur during sort (WriteBlobs should be > 0)");
    UNIT_ASSERT_C(counters.ComputeSpilling.ReadBlobs->Val() > 0,
        "Expected spilling to occur during sort (ReadBlobs should be > 0)");
}

Y_UNIT_TEST(WideSortNoSpillingWhenDisabled) {
    // Verify that spilling does NOT happen when it's disabled
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;
    // Use high threshold so yellow zone is never reached
    TKikimrRunner kikimr(AppCfgWithSortSpilling(100.0));

    auto db = kikimr.GetQueryClient();

    CreateSortTestTable(db);
    FillSortTestTable(db, 10); // Small dataset

    auto query = R"(
        --!syntax_v1
        SELECT Key, SortKey
        FROM `/Root/sort_test`
        ORDER BY SortKey ASC
    )";

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto resultSet = result.GetResultSets()[0];
    UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 10);

    // Verify sort order
    NYdb::TResultSetParser parser(resultSet);
    TString prevSortKey;
    while (parser.TryNextRow()) {
        auto sortKey = TString(*parser.ColumnParser("SortKey").GetOptionalString());
        if (!prevSortKey.empty()) {
            UNIT_ASSERT_C(sortKey >= prevSortKey,
                TStringBuilder() << "Sort order violated: " << sortKey << " < " << prevSortKey);
        }
        prevSortKey = sortKey;
    }

    TKqpCounters counters(kikimr.GetTestServer().GetRuntime()->GetAppData().Counters);
    UNIT_ASSERT_VALUES_EQUAL(counters.ComputeSpilling.WriteBlobs->Val(), 0);
    UNIT_ASSERT_VALUES_EQUAL(counters.ComputeSpilling.ReadBlobs->Val(), 0);
}

Y_UNIT_TEST(WideSortWithTopAndSpilling) {
    // Test ORDER BY ... LIMIT which uses WideTopSort
    Cerr << "cwd: " << NFs::CurrentWorkingDirectory() << Endl;
    TKikimrRunner kikimr(AppCfgWithSortSpilling());

    auto db = kikimr.GetQueryClient();

    CreateSortTestTable(db);
    FillSortTestTable(db);

    auto query = R"(
        --!syntax_v1
        SELECT Key, SortKey, Payload
        FROM `/Root/sort_test`
        ORDER BY SortKey ASC
        LIMIT 50
    )";

    auto result = db.ExecuteQuery(query, NYdb::NQuery::TTxControl::BeginTx().CommitTx()).GetValueSync();
    UNIT_ASSERT_C(result.IsSuccess(), result.GetIssues().ToString());

    auto resultSet = result.GetResultSets()[0];
    UNIT_ASSERT_VALUES_EQUAL(resultSet.RowsCount(), 50);

    // Verify sort order
    NYdb::TResultSetParser parser(resultSet);
    TString prevSortKey;
    while (parser.TryNextRow()) {
        auto sortKey = TString(*parser.ColumnParser("SortKey").GetOptionalString());
        if (!prevSortKey.empty()) {
            UNIT_ASSERT_C(sortKey >= prevSortKey,
                TStringBuilder() << "Sort order violated: " << sortKey << " < " << prevSortKey);
        }
        prevSortKey = sortKey;
    }
}

} // Y_UNIT_TEST_SUITE(KqpSortSpilling)

} // namespace NKqp
} // namespace NKikimr
