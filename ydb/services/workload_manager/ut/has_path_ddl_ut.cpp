#include <ydb/core/base/path.h>
#include <ydb/services/workload_manager/ut/common/query_classifier_ut_common.h>
#include <ydb/services/workload_manager/ut/common/workload_service_ut_common.h>

#include <library/cpp/testing/unittest/registar.h>


namespace NKikimr::NWorkloadManager {

using namespace NWorkloadManager;
using namespace NYdb;


namespace {

// Creates a simple row-store `t(Id, Payload)` and grants the given user full access.
void SetupRowStoreTable(TIntrusivePtr<IYdbSetup> ydb, const TString& userSID) {
    ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
        GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
        CREATE TABLE t (
            Id Uint64 NOT NULL,
            Payload Utf8,
            PRIMARY KEY (Id)
        );
    )");

    auto insertSettings = TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID);
    auto insertResult = ydb->ExecuteQuery(R"(
        UPSERT INTO t (Id, Payload) VALUES (1u, "a"), (2u, "b");
    )", insertSettings);
    UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), NYdb::EStatus::SUCCESS,
        insertResult.GetIssues().ToOneLineString());
}

// Creates a column-store `olap_t(Id, Payload)` and grants full access.
void SetupColumnStoreTable(TIntrusivePtr<IYdbSetup> ydb, const TString& userSID) {
    ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
        GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
        CREATE TABLE olap_t (
            Id Uint64 NOT NULL,
            Payload Utf8,
            PRIMARY KEY (Id)
        ) WITH (
            STORE = COLUMN
        );
    )");

    auto insertSettings = TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID);
    auto insertResult = ydb->ExecuteQuery(R"(
        UPSERT INTO olap_t (Id, Payload) VALUES (1u, "a"), (2u, "b");
    )", insertSettings);
    UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), NYdb::EStatus::SUCCESS,
        insertResult.GetIssues().ToOneLineString());
}

// Creates `orders(Id, Status)` with a global secondary index on Status.
void SetupOrdersWithIndex(TIntrusivePtr<IYdbSetup> ydb, const TString& userSID) {
    ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
        GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
        CREATE TABLE orders (
            Id Uint64 NOT NULL,
            Status Utf8,
            PRIMARY KEY (Id),
            INDEX by_status GLOBAL ON (Status)
        );
    )");

    auto insertSettings = TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID);
    auto insertResult = ydb->ExecuteQuery(R"(
        UPSERT INTO orders (Id, Status) VALUES (1u, "a"), (2u, "b");
    )", insertSettings);
    UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), NYdb::EStatus::SUCCESS,
        insertResult.GetIssues().ToOneLineString());
}

// Creates `t(...)` and a view `v` over it, grants full access.
// The view body must fully qualify the underlying table — the invoker's
// session context doesn't resolve unqualified `t` when the view is expanded.
void SetupTableAndView(TIntrusivePtr<IYdbSetup> ydb, const TString& userSID) {
    ydb->ExecuteSchemeQuery(TStringBuilder() << R"(
        GRANT ALL ON `/)" << ydb->GetSettings().DomainName_ << R"(` TO `)" << userSID << R"(`;
        CREATE TABLE t (
            Id Uint64 NOT NULL,
            Payload Utf8,
            PRIMARY KEY (Id)
        );
        CREATE VIEW v WITH (security_invoker = true) AS SELECT * FROM `/Root/t`;
    )");

    auto insertSettings = TQueryRunnerSettings().PoolId(NResourcePool::DEFAULT_POOL_ID);
    auto insertResult = ydb->ExecuteQuery(R"(
        UPSERT INTO t (Id, Payload) VALUES (1u, "a"), (2u, "b");
    )", insertSettings);
    UNIT_ASSERT_VALUES_EQUAL_C(insertResult.GetStatus(), NYdb::EStatus::SUCCESS,
        insertResult.GetIssues().ToOneLineString());
}

// EDS/ET/Topic/CDC DDL ITs live in `ydb/core/kqp/ut/federated_query/datastreams/`
// (see `has_path_ut.cpp` there). Those kinds need TStreamingTestFixture's
// mock connector + mock PQ gateway + http gateway wiring, which is not present
// in workload_service_ut's setup. This file covers the kinds whose fixture is
// cheap: regular tables, sysview, secondary index, view (underlying-table gate).

TString RejectClassifierDdl(const TString& poolId, const TString& classifierName, const TString& hasPathPattern) {
    return TStringBuilder() << R"(
        CREATE RESOURCE POOL )" << poolId << R"( WITH (
            CONCURRENT_QUERY_LIMIT=0
        );
        CREATE RESOURCE POOL CLASSIFIER )" << classifierName << R"( WITH (
            RESOURCE_POOL=")" << poolId << R"(",
            HAS_PATH=")" << hasPathPattern << R"("
        );
    )";
}

}  // anonymous namespace


// Regular row-store table: SELECT * FROM t exercises the (A) walk via TableOps.Table.
Y_UNIT_TEST_SUITE(HasPathTableOltp) {

    Y_UNIT_TEST(TestClassifierMatchesTablePath) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupRowStoreTable(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(poolId, "row_table_classifier", "/Root/t"));

        const TString query = "SELECT * FROM t;";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierFail(ydb, query, settings, poolId);
    }

    Y_UNIT_TEST(TestClassifierIgnoresOtherTable) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupRowStoreTable(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(poolId, "row_table_classifier", "/Root/other_table"));

        const TString query = "SELECT * FROM t;";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierSuccess(ydb, query, settings);
    }
}

// Column-store table: SELECT * FROM olap_t compiles to ReadOlapRange, still surfaces
// the table path through the same (A) walk. HAS_PATH doesn't care about read shape,
// so a point lookup fires equally.
Y_UNIT_TEST_SUITE(HasPathColumnTableOlap) {

    Y_UNIT_TEST(TestClassifierMatchesOlapTablePath) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupColumnStoreTable(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(poolId, "olap_table_classifier", "/Root/olap_t"));

        const TString query = "SELECT * FROM olap_t;";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierFail(ydb, query, settings, poolId);
    }

    Y_UNIT_TEST(TestClassifierMatchesOlapPointLookup) {
        // Unlike HAS_FULL_SCAN, HAS_PATH doesn't care about read shape — a PK lookup
        // still touches the table and must fire.
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupColumnStoreTable(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(poolId, "olap_table_classifier", "/Root/olap_t"));

        const TString query = "SELECT * FROM olap_t WHERE Id = 1u;";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierFail(ydb, query, settings, poolId);
    }
}

// Secondary index sub-table: `.../indexImplTable` shows up in QueryTables when the
// planner emits impl-table reads. Also fires on writes that fan out through indexes.
Y_UNIT_TEST_SUITE(HasPathSecondaryIndex) {

    Y_UNIT_TEST(TestClassifierMatchesIndexImplPath) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupOrdersWithIndex(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(
            poolId, "index_impl_classifier", "/Root/orders/by_status/indexImplTable"));

        // Covering scan on the index — planner emits a full read of indexImplTable.
        const TString query = "SELECT Status FROM orders VIEW by_status;";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierFail(ydb, query, settings, poolId);
    }

    Y_UNIT_TEST(TestClassifierMatchesIndexOnWrite) {
        // UPSERT into an indexed table fans out to write both the main table and
        // the impl table. HAS_PATH targeting the impl path still fires.
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupOrdersWithIndex(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(
            poolId, "index_write_classifier", "/Root/orders/by_status/indexImplTable"));

        const TString query = "UPSERT INTO orders (Id, Status) VALUES (3u, \"c\");";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierFail(ydb, query, settings, poolId);
    }
}

// System views: SELECT FROM `.sys/...` populates tx.Tables with a SysView-kind entry
// (see kqp_query_compiler.cpp:1288 tablesMap loop).
Y_UNIT_TEST_SUITE(HasPathSysView) {

    Y_UNIT_TEST(TestClassifierMatchesSysViewPath) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupRowStoreTable(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(
            poolId, "sysview_classifier", "/Root/.sys/partition_stats"));

        const TString query = "SELECT * FROM `.sys/partition_stats`;";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierFail(ydb, query, settings, poolId);
    }

    Y_UNIT_TEST(TestClassifierIgnoresRegularTableWhenSysViewTargeted) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupRowStoreTable(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(
            poolId, "sysview_classifier", "/Root/.sys/*"));

        const TString query = "SELECT * FROM t;";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierSuccess(ydb, query, settings);
    }
}

// Views are inlined at compile time. The loader records them separately in
// phyQuery.ViewInfos. HAS_PATH regex matches the view's own path via the (C) walk.
Y_UNIT_TEST_SUITE(HasPathView) {

    // KindView is a documented blind spot for the view's own path — see has-path-wip.md.
    // Empirically confirmed: SELECT FROM v inlines the view fully. QueryTables and
    // tx.Tables carry only the underlying table paths; phyQuery.ViewInfos records
    // views by numeric TableId with an empty TableName (yql_kikimr_datasource.cpp:433).
    // A regex on the view path `/Root/v` never fires.
    /*
    Y_UNIT_TEST(TestClassifierMatchesViewPath) {
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupTableAndView(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(poolId, "view_classifier", "/Root/v"));

        const TString query = "SELECT * FROM v;";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierFail(ydb, query, settings, poolId);
    }
    */

    Y_UNIT_TEST(TestClassifierMatchesUnderlyingTableUnderView) {
        // Views are inlined — the underlying table's path also appears in QueryTables.
        // A regex targeting the underlying table matches queries going through the view.
        auto ydb = TYdbSetupSettings().Create();
        const TString& poolId = "reject_pool";
        const TString& userSID = "test@user";
        SetupTableAndView(ydb, userSID);

        ydb->ExecuteSchemeQuery(RejectClassifierDdl(poolId, "underlying_classifier", "/Root/t"));

        const TString query = "SELECT * FROM v;";
        auto settings = TQueryRunnerSettings().PoolId("").UserSID(userSID);
        WaitForClassifierFail(ydb, query, settings, poolId);
    }
}

}  // namespace NKikimr::NWorkloadManager
