#include <ydb/core/base/path.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_query_classifier_ut_common.h>
#include <ydb/core/kqp/workload_service/ut/common/kqp_workload_service_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NKqp {

using namespace NWorkload;
using namespace NYdb;


namespace {

TString GetPostPoolId(const IQueryClassifier::TPostCompileClassifyResult& result) {
    UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TResolvedPoolId>(result),
        TStringBuilder() << "Expected TResolvedPoolId, got variant index " << result.index());
    return std::get<IQueryClassifier::TResolvedPoolId>(result).PoolId;
}

}  // anonymous namespace


Y_UNIT_TEST_SUITE(TQueryClassifierHasFullScan) {

    Y_UNIT_TEST(ShouldTriggerPendingCompilation) {
        TClassifyTestCase tc;
        tc.ClassifierHasFullScan = "/Root/testdb/my_table";
        auto result = tc.RunPreClassify();
        UNIT_ASSERT_C(std::holds_alternative<IQueryClassifier::TPendingCompilation>(result),
            TStringBuilder() << "Expected TPendingCompilation, got variant index " << result.index());
    }

    Y_UNIT_TEST(ShouldMatchFullScan) {
        TClassifyTestCase tc;
        tc.ClassifierHasFullScan = "/Root/testdb/my_table";
        auto result = tc.RunPostClassify("/Root/testdb/my_table", /*isFullScan=*/true);
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "pool_target");
    }

    Y_UNIT_TEST(ShouldNotMatchPartialScan) {
        TClassifyTestCase tc;
        tc.ClassifierHasFullScan = "/Root/testdb/my_table";
        auto result = tc.RunPostClassify("/Root/testdb/my_table", /*isFullScan=*/false);
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "default");
    }

    Y_UNIT_TEST(ShouldNotMatchFullScanOnDifferentTable) {
        TClassifyTestCase tc;
        tc.ClassifierHasFullScan = "/Root/testdb/my_table";
        auto result = tc.RunPostClassify("/Root/testdb/other_table", /*isFullScan=*/true);
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "default");
    }

    Y_UNIT_TEST(ShouldMatchGlobPattern) {
        TClassifyTestCase tc;
        tc.ClassifierHasFullScan = "/Root/testdb/orders_*";
        auto result = tc.RunPostClassify("/Root/testdb/orders_2024", /*isFullScan=*/true);
        UNIT_ASSERT_VALUES_EQUAL(GetPostPoolId(result), "pool_target");
    }
}

namespace {

// Creates `orders(Id, Status)` with a global secondary index on Status,
// grants the given user full access, and inserts a couple of rows so the planner
// doesn't take an empty-table shortcut.
void SetupOrdersWithIndex(TIntrusivePtr<NWorkload::IYdbSetup> ydb, const TString& userSID) {
    ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
        GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
        CREATE TABLE orders (
            Id Uint64 NOT NULL,
            Status Utf8,
            PRIMARY KEY (Id),
            INDEX by_status GLOBAL ON (Status)
        );
    )");

    auto insertSettings = NWorkload::TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID);
    auto insertResult = ydb->ExecuteQuery(R"(
        UPSERT INTO orders (Id, Status) VALUES
            (1u, "a"),
            (2u, "b");
    )", insertSettings);
    UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), NYdb::EStatus::SUCCESS,
        insertResult.GetIssues().ToOneLineString());
}

// Creates a column-store (OLAP) table `olap_events(Id, Payload)` with a HAS_FULL_SCAN
// classifier targeting it, and inserts a couple of rows. The classifier's target pool
// has CONCURRENT_QUERY_LIMIT=0 so any match rejects with PRECONDITION_FAILED.
void SetupOlapEventsAndClassifier(
    TIntrusivePtr<NWorkload::IYdbSetup> ydb, const TString& userSID, const TString& poolId)
{
    ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
        GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
        CREATE TABLE olap_events (
            Id Uint64 NOT NULL,
            Payload Utf8,
            PRIMARY KEY (Id)
        ) WITH (
            STORE = COLUMN
        );
        CREATE RESOURCE POOL )" << poolId << R"( WITH (
            CONCURRENT_QUERY_LIMIT=0
        );
        CREATE RESOURCE POOL CLASSIFIER olap_classifier WITH (
            RESOURCE_POOL=")" << poolId << R"(",
            HAS_FULL_SCAN="/Root/olap_events"
        );
    )");

    auto insertSettings = NWorkload::TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID);
    auto insertResult = ydb->ExecuteQuery(R"(
        UPSERT INTO olap_events (Id, Payload) VALUES (1u, "a"), (2u, "b");
    )", insertSettings);
    UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), NYdb::EStatus::SUCCESS,
        insertResult.GetIssues().ToOneLineString());
}

}  // anonymous namespace


Y_UNIT_TEST_SUITE(HasFullScanSecondaryIndexOltpDdl) {

    // Covering read on the secondary index: reads impl table only (Status is the indexed column),
    // main table is not accessed. A classifier targeting only `/Root/orders` (not the impl path)
    // must NOT fire — the impl full scan happens on a path outside the classifier's regex.
    Y_UNIT_TEST(TestClassifierOnMainTableIgnoresIndexScan) {
        auto ydb = NWorkload::TYdbSetupSettings().Create();

        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupOrdersWithIndex(ydb, userSID);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=0
            );
            CREATE RESOURCE POOL CLASSIFIER main_only WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                HAS_FULL_SCAN="/Root/orders"
            );
        )");

        const TString query = "SELECT * FROM orders VIEW by_status;";
        auto settings = NWorkload::TQueryRunnerSettings().PoolId("").UserSID(userSID);
        NWorkload::WaitForClassifierSuccess(ydb, query, settings);
    }

    // Same query, but the classifier targets the index impl table path directly.
    // The impl full scan matches — classifier must fire and reject the query.
    //
    // Covering scan: reading only `Status` (an indexed column) forces the planner to
    // scan the impl table without falling back to the main table.
    // `SELECT * FROM orders VIEW by_status` (no predicate) would trigger the planner
    // warning "Given predicate is not suitable for used index" and fall back to a main-table scan.
    Y_UNIT_TEST(TestClassifierOnIndexImplTableDetectsFullScan) {
        auto ydb = NWorkload::TYdbSetupSettings().Create();

        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupOrdersWithIndex(ydb, userSID);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=0
            );
            CREATE RESOURCE POOL CLASSIFIER impl_only WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                HAS_FULL_SCAN="/Root/orders/by_status/indexImplTable"
            );
        )");

        const TString query = "SELECT * FROM orders VIEW by_status;";
        auto settings = NWorkload::TQueryRunnerSettings().PoolId("").UserSID(userSID);
        NWorkload::WaitForClassifierFail(ydb, query, settings, poolId);
    }
}

Y_UNIT_TEST_SUITE(HasFullScanIgnoresLimitDdl) {

    // LIMIT does not bypass full-scan detection. `SELECT * FROM orders LIMIT 1` still has an
    // unbounded ReadRange on /Root/orders — the classifier fires and rejects into the target pool.
    Y_UNIT_TEST(TestLimitDoesNotBypassClassifier) {
        auto ydb = NWorkload::TYdbSetupSettings().Create();

        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupOrdersWithIndex(ydb, userSID);

        ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
            CREATE RESOURCE POOL )" << poolId << R"( WITH (
                CONCURRENT_QUERY_LIMIT=0
            );
            CREATE RESOURCE POOL CLASSIFIER main_only WITH (
                RESOURCE_POOL=")" << poolId << R"(",
                HAS_FULL_SCAN="/Root/orders"
            );
        )");

        const TString query = "SELECT * FROM orders LIMIT 1;";
        auto settings = NWorkload::TQueryRunnerSettings().PoolId("").UserSID(userSID);
        NWorkload::WaitForClassifierFail(ydb, query, settings, poolId);
    }

    // Same LIMIT semantic applied to OLAP: `ReadOlapRange` with an unbounded param name and
    // HasItemsLimit() set still counts as a full scan.
    Y_UNIT_TEST(TestLimitDoesNotBypassClassifierOnOlapTable) {
        auto ydb = NWorkload::TYdbSetupSettings().Create();

        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupOlapEventsAndClassifier(ydb, userSID, poolId);

        const TString query = "SELECT * FROM olap_events LIMIT 1;";
        auto settings = NWorkload::TQueryRunnerSettings().PoolId("").UserSID(userSID);
        NWorkload::WaitForClassifierFail(ydb, query, settings, poolId);
    }

    // Zero-selectivity filter behind a LIMIT: at runtime the executor scans the whole table
    // looking for a non-existent value. The residual predicate lives in `OlapProgram` (opaque
    // to the matcher) while `KeyRanges.ParamName` stays empty and `HasItemsLimit()` is set —
    // exactly the runtime full-scan the LIMIT rule used to miss. OLAP is used because there is
    // no secondary index to route the filter through, so the plan shape is unambiguous.
    Y_UNIT_TEST(TestFilterWithLimitDoesNotBypassClassifierOnOlapTable) {
        auto ydb = NWorkload::TYdbSetupSettings().Create();

        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupOlapEventsAndClassifier(ydb, userSID, poolId);

        const TString query = "SELECT * FROM olap_events WHERE Payload = 'nonexistent' LIMIT 1;";
        auto settings = NWorkload::TQueryRunnerSettings().PoolId("").UserSID(userSID);
        NWorkload::WaitForClassifierFail(ydb, query, settings, poolId);
    }
}

Y_UNIT_TEST_SUITE(HasFullScanOlapDdl) {

    // Column-store (OLAP) tables emit `TKqpPhyOpReadOlapRanges` (TableOps branch) instead of
    // row-store's `ReadRange`/`ReadRangesSource`. This test proves the matcher's OLAP branch
    // is invoked end-to-end by the real planner, complementing the ReadOlapRange matcher UT.
    Y_UNIT_TEST(TestClassifierDetectsFullScanOnOlapTable) {
        auto ydb = NWorkload::TYdbSetupSettings().Create();

        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupOlapEventsAndClassifier(ydb, userSID, poolId);

        const TString query = "SELECT * FROM olap_events;";
        auto settings = NWorkload::TQueryRunnerSettings().PoolId("").UserSID(userSID);
        NWorkload::WaitForClassifierFail(ydb, query, settings, poolId);
    }

    // PK point lookup on OLAP compiles to a parametrized ReadOlapRange with a non-empty
    // KeyRanges.ParamName. Matcher must not flag it — proves the OLAP negative branch e2e.
    Y_UNIT_TEST(TestClassifierIgnoresPkPointLookupOnOlapTable) {
        auto ydb = NWorkload::TYdbSetupSettings().Create();

        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupOlapEventsAndClassifier(ydb, userSID, poolId);

        const TString query = "SELECT * FROM olap_events WHERE Id = 1u;";
        auto settings = NWorkload::TQueryRunnerSettings().PoolId("").UserSID(userSID);
        NWorkload::WaitForClassifierSuccess(ydb, query, settings);
    }
}

}  // namespace NKikimr::NKqp
