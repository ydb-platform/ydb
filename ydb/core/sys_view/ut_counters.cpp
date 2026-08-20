#include "ut_common.h"

#include <ydb/core/base/counters.h>
#include <ydb/core/kqp/ut/common/kqp_ut_common.h>

namespace NKikimr {
namespace NSysView {

using namespace NYdb;
using namespace NYdb::NTable;
using namespace NYdb::NScheme;

namespace {

void CreateDatabase(TTestEnv& env, const TString& databaseName) {
    auto subdomain = GetSubDomainDeclareSettings(databaseName);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().CreateExtSubdomain("/Root", subdomain));

    env.GetTenants().Run("/Root/" + databaseName, 1);

    auto subdomainSettings = GetSubDomainDefaultSettings(databaseName, env.GetPools());
    subdomainSettings.SetExternalSysViewProcessor(true);
    subdomainSettings.SetExternalSchemeShard(true);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
}

void CreateDatabases(TTestEnv& env) {
    CreateDatabase(env, "Database1");
    CreateDatabase(env, "Database2");
}

void CreateTables(TTestEnv& env) {
    auto driverConfig = TDriverConfig()
        .SetEndpoint(env.GetEndpoint())
        .SetDiscoveryMode(EDiscoveryMode::Off);
    auto driver = TDriver(driverConfig);

    {
        TTableClient client(driver, TClientSettings().Database("/Root/Database1"));
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Database1/Table1` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/Database1/Table1` (Key, Value) VALUES
                (1u, "A"),
                (2u, "B"),
                (3u, "C");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync());
    }

    {
        TTableClient client(driver, TClientSettings().Database("/Root/Database2"));
        auto session = client.CreateSession().GetValueSync().GetSession();
        NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
            CREATE TABLE `/Root/Database2/Table2` (
                Key Uint64,
                Value String,
                PRIMARY KEY (Key)
            );
        )").GetValueSync());

        NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
            REPLACE INTO `/Root/Database2/Table2` (Key, Value) VALUES
                (4u, "D"),
                (5u, "E");
        )", TTxControl::BeginTx().CommitTx()).GetValueSync());
    }
}

void CreateDatabasesAndTables(TTestEnv& env) {
    CreateDatabases(env);
    CreateTables(env);
}

// ---- Detailed metrics (ydb_detailed) helpers, step 13 -------------------

// Same shape as CreateDatabase(), plus a database-level TablesMetricsLevel:
// with no per-table METRICS_LEVEL proto plumbing yet (that is later, YQL
// facing step), DataShard's GetEffectiveMetricsLevel falls back to the
// subdomain level for every table that sets none, so this alone is enough to
// make every table of the database report at PARTITION level.
void CreateDetailedDatabase(TTestEnv& env, const TString& databaseName) {
    auto subdomain = GetSubDomainDeclareSettings(databaseName);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().CreateExtSubdomain("/Root", subdomain));

    env.GetTenants().Run("/Root/" + databaseName, 1);

    auto subdomainSettings = GetSubDomainDefaultSettings(databaseName, env.GetPools());
    subdomainSettings.SetExternalSysViewProcessor(true);
    subdomainSettings.SetExternalSchemeShard(true);
    subdomainSettings.SetTablesMetricsLevel(
        NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition);
    UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
        env.GetClient().AlterExtSubdomain("/Root", subdomainSettings));
}

void CreateDetailedTable(TTestEnv& env, const TString& databaseName, const TString& tableName) {
    auto driverConfig = TDriverConfig()
        .SetEndpoint(env.GetEndpoint())
        .SetDiscoveryMode(EDiscoveryMode::Off);
    auto driver = TDriver(driverConfig);

    const TString databasePath = "/Root/" + databaseName;
    const TString tablePath = databasePath + "/" + tableName;

    TTableClient client(driver, TClientSettings().Database(databasePath));
    auto session = client.CreateSession().GetValueSync().GetSession();
    NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(TStringBuilder() << R"(
        CREATE TABLE `)" << tablePath << R"(` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );
    )").GetValueSync());

    NKqp::AssertSuccessResult(session.ExecuteDataQuery(TStringBuilder() << R"(
        REPLACE INTO `)" << tablePath << R"(` (Key, Value) VALUES
            (1u, "A"),
            (2u, "B"),
            (3u, "C");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());
}

// The public ydb_detailed tree's fixed hops above table=: host="" always
// (processor-move protection, mirrors ydb_serverless), then
// monitoring_project_id=<value> (present only once the DB user attribute is
// set), then database=<path>. Uses FindSubgroup (never GetSubgroup, which
// would create the group and make the absence assertions vacuous).
::NMonitoring::TDynamicCounterPtr FindDetailedTableGroup(
    ::NMonitoring::TDynamicCounterPtr ydbDetailedRoot,
    const TString& monitoringProjectId,
    const TString& databasePath,
    const TString& relativeTablePath)
{
    if (!ydbDetailedRoot) {
        return nullptr;
    }
    auto hostGroup = ydbDetailedRoot->FindSubgroup("host", "");
    if (!hostGroup) {
        return nullptr;
    }
    auto projectGroup = hostGroup->FindSubgroup("monitoring_project_id", monitoringProjectId);
    if (!projectGroup) {
        return nullptr;
    }
    auto databaseGroup = projectGroup->FindSubgroup("database", databasePath);
    if (!databaseGroup) {
        return nullptr;
    }
    return databaseGroup->FindSubgroup("table", relativeTablePath);
}

// The first tablet_id=<id> child of a table= group, whichever id it turns
// out to be (the test does not pin down DataShard's own choice of tablet id).
::NMonitoring::TDynamicCounterPtr FindAnyTabletIdGroup(::NMonitoring::TDynamicCounterPtr tableGroup) {
    TString tabletId;
    tableGroup->EnumerateSubgroups([&](const TString& name, const TString& value) {
        if (name == "tablet_id" && tabletId.empty()) {
            tabletId = value;
        }
    });
    if (tabletId.empty()) {
        return nullptr;
    }
    return tableGroup->FindSubgroup("tablet_id", tabletId);
}

} // namespace

Y_UNIT_TEST_SUITE(DbCounters) {

    Y_UNIT_TEST(TabletsSimple) {
        TTestEnv env(1, 2, {.EnableSVP = true});
        CreateDatabasesAndTables(env);

        for (size_t iter = 0; iter < 30; ++iter) {
            Cerr << "iteration " << iter << Endl;

            auto checkTabletCounters = [] (::NMonitoring::TDynamicCounterPtr databaseGroup,
                const char* databaseName)
            {
                auto checkCounter = [databaseName] (::NMonitoring::TDynamicCounterPtr group,
                    const char* sensorName, bool isDerivative)
                {
                    auto value = group->GetCounter(sensorName, isDerivative)->Val();
                    Cerr << "Database " << databaseName << ", sensor " << sensorName << ", value " << value << Endl;
                    return (value > 0);
                };

                bool isGood = true;

                auto tabletGroup = databaseGroup->GetSubgroup("host", "");
                auto datashardGroup = tabletGroup->GetSubgroup("type", "DataShard");
                {
                    auto executorGroup = datashardGroup->GetSubgroup("category", "executor");
                    {
                        TStringStream ss;
                        executorGroup->OutputHtml(ss);
                        Cerr << ss.Str() << Endl;
                    }

                    isGood &= checkCounter(executorGroup, "SUM(UsedTabletMemory)", false);
                    isGood &= checkCounter(executorGroup, "MAX(UsedTabletMemory)", false);

                    isGood &= checkCounter(executorGroup, "TabletBytesWritten", true);

                    auto appGroup = datashardGroup->GetSubgroup("category", "app");
                    {
                        TStringStream ss;
                        appGroup->OutputHtml(ss);
                        Cerr << ss.Str() << Endl;
                    }

                    isGood &= checkCounter(appGroup, "DataShard/EngineHostRowUpdateBytes", true);
                    isGood &= checkCounter(appGroup, "MAX(DataShard/EngineHostRowUpdateBytes)", false);
                }

                auto schemeshardGroup = tabletGroup->GetSubgroup("type", "SchemeShard");
                {
                    auto executorGroup = schemeshardGroup->GetSubgroup("category", "executor");
                    {
                        TStringStream ss;
                        executorGroup->OutputHtml(ss);
                        Cerr << ss.Str() << Endl;
                    }

                    isGood &= checkCounter(executorGroup, "SUM(UsedTabletMemory)", false);
                    isGood &= checkCounter(executorGroup, "MAX(UsedTabletMemory)", false);

                    isGood &= checkCounter(executorGroup, "TabletBytesWritten", true);

                    auto appGroup = schemeshardGroup->GetSubgroup("category", "app");
                    {
                        TStringStream ss;
                        appGroup->OutputHtml(ss);
                        Cerr << ss.Str() << Endl;
                    }

                    isGood &= checkCounter(appGroup, "SUM(SchemeShard/Tables)", false);
                    isGood &= checkCounter(appGroup, "MAX(SchemeShard/Tables)", false);

                    isGood &= checkCounter(appGroup, "SchemeShard/FinishedOps/CreateTable", true);
                }

                return isGood;
            };

            bool checkDb1 = false, checkDb2 = false;

            for (ui32 nodeId = 0; nodeId < env.GetServer().GetRuntime()->GetNodeCount(); ++nodeId) {
                auto counters = env.GetServer().GetRuntime()->GetAppData(nodeId).Counters;
                auto dbGroup = GetServiceCounters(counters, "tablets_serverless", false);

                auto databaseGroup1 = dbGroup->FindSubgroup("database", "/Root/Database1");
                if (databaseGroup1) {
                    checkDb1 = checkTabletCounters(databaseGroup1, "/Root/Database1");
                }
                auto databaseGroup2 = dbGroup->FindSubgroup("database", "/Root/Database2");
                if (databaseGroup2) {
                    checkDb2 = checkTabletCounters(databaseGroup2, "/Root/Database2");
                }
            }

            if (checkDb1 && checkDb2) {
                return;
            }

            Sleep(TDuration::Seconds(5));
        }

        UNIT_ASSERT_C(false, "out of iterations");
    }

    // Cross-node detailed per-table metrics, published by the SysView
    // Processor into the public ydb_detailed group (step 13). host="" is
    // always present (processor-move protection, mirrors ydb_serverless),
    // monitoring_project_id=<value> comes from the DB user attribute of the
    // same name (ATTR_MONITORING_PROJECT_ID) and is absent when unset.
    Y_UNIT_TEST(DetailedTables) {
        TTestEnv env(1, 2, {.EnableSVP = true, .EnableDetailedMetrics = true});

        CreateDetailedDatabase(env, "Database1");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().AlterUserAttributes("/Root", "Database1",
                {{"monitoring_project_id", "proj1"}}));
        CreateDetailedTable(env, "Database1", "Table1");

        const TString databasePath = "/Root/Database1";
        const TString relativeTablePath = "Table1";
        const TString projectId = "proj1";

        for (size_t iter = 0; iter < 30; ++iter) {
            Cerr << "iteration " << iter << Endl;

            bool isGood = false;

            for (ui32 nodeId = 0; nodeId < env.GetServer().GetRuntime()->GetNodeCount(); ++nodeId) {
                auto counters = env.GetServer().GetRuntime()->GetAppData(nodeId).Counters;
                auto root = GetServiceCounters(counters, "ydb_detailed", false);

                auto tableGroup = FindDetailedTableGroup(root, projectId, databasePath, relativeTablePath);
                if (!tableGroup) {
                    continue;
                }

                {
                    TStringStream ss;
                    tableGroup->OutputHtml(ss);
                    Cerr << "node " << nodeId << ", table group:" << Endl << ss.Str() << Endl;
                }

                // A table-level metric present and > 0 directly on table=
                auto rowCount = tableGroup->FindNamedCounter("name", "table.datashard.row_count");
                if (!rowCount || rowCount->Val() <= 0) {
                    continue;
                }

                // A partition leaf: some tablet_id=<id>/follower_id=0 carrying
                // the same metric name
                auto tabletGroup = FindAnyTabletIdGroup(tableGroup);
                if (!tabletGroup) {
                    continue;
                }
                auto followerGroup = tabletGroup->FindSubgroup("follower_id", "0");
                if (!followerGroup) {
                    continue;
                }
                auto leafRowCount = followerGroup->FindNamedCounter("name", "table.datashard.row_count");
                if (!leafRowCount) {
                    continue;
                }

                // Neither a detailed_metrics= hop nor a replicas_only group is
                // ever materialized in the public tree (decision S3)
                UNIT_ASSERT(!tableGroup->FindSubgroup("detailed_metrics"));
                UNIT_ASSERT(!tableGroup->FindSubgroup("follower_id", "replicas_only"));

                isGood = true;
                break;
            }

            if (isGood) {
                return;
            }

            Sleep(TDuration::Seconds(5));
        }

        UNIT_ASSERT_C(false, "out of iterations");
    }

    // Changing the monitoring_project_id user attribute moves the whole
    // per-database tree under the new value and leaves nothing behind under
    // the old one.
    Y_UNIT_TEST(DetailedTablesMonitoringProjectIdChange) {
        TTestEnv env(1, 2, {.EnableSVP = true, .EnableDetailedMetrics = true});

        CreateDetailedDatabase(env, "Database1");
        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().AlterUserAttributes("/Root", "Database1",
                {{"monitoring_project_id", "proj1"}}));
        CreateDetailedTable(env, "Database1", "Table1");

        const TString databasePath = "/Root/Database1";
        const TString relativeTablePath = "Table1";

        // Wait until the tree is visible under proj1, remembering which node
        // (the one hosting Database1's SysView Processor) it showed up on
        ui32 foundNodeId = Max<ui32>();
        for (size_t iter = 0; iter < 30 && foundNodeId == Max<ui32>(); ++iter) {
            Cerr << "iteration " << iter << " (waiting for proj1)" << Endl;

            for (ui32 nodeId = 0; nodeId < env.GetServer().GetRuntime()->GetNodeCount(); ++nodeId) {
                auto counters = env.GetServer().GetRuntime()->GetAppData(nodeId).Counters;
                auto root = GetServiceCounters(counters, "ydb_detailed", false);

                auto tableGroup = FindDetailedTableGroup(root, "proj1", databasePath, relativeTablePath);
                if (tableGroup && tableGroup->FindNamedCounter("name", "table.datashard.row_count")) {
                    foundNodeId = nodeId;
                    break;
                }
            }

            if (foundNodeId == Max<ui32>()) {
                Sleep(TDuration::Seconds(5));
            }
        }
        UNIT_ASSERT_C(foundNodeId != Max<ui32>(), "out of iterations waiting for proj1");

        UNIT_ASSERT_VALUES_EQUAL(NMsgBusProxy::MSTATUS_OK,
            env.GetClient().AlterUserAttributes("/Root", "Database1",
                {{"monitoring_project_id", "proj2"}}));

        for (size_t iter = 0; iter < 30; ++iter) {
            Cerr << "iteration " << iter << " (waiting for the move to proj2)" << Endl;

            auto counters = env.GetServer().GetRuntime()->GetAppData(foundNodeId).Counters;
            auto root = GetServiceCounters(counters, "ydb_detailed", false);

            auto newTableGroup = FindDetailedTableGroup(root, "proj2", databasePath, relativeTablePath);
            bool moved = newTableGroup && newTableGroup->FindNamedCounter("name", "table.datashard.row_count");

            // Nothing left behind under the old monitoring_project_id: either
            // the group is gone entirely, or (if it survives empty) it holds
            // no database= child anymore
            bool leftBehind = false;
            if (root) {
                auto hostGroup = root->FindSubgroup("host", "");
                if (hostGroup) {
                    auto oldProjectGroup = hostGroup->FindSubgroup("monitoring_project_id", "proj1");
                    if (oldProjectGroup && oldProjectGroup->FindSubgroup("database")) {
                        leftBehind = true;
                    }
                }
            }

            if (moved && !leftBehind) {
                return;
            }

            Sleep(TDuration::Seconds(5));
        }

        UNIT_ASSERT_C(false, "out of iterations");
    }
}

} // NSysView
} // NKikimr
