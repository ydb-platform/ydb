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
    TTableClient client(env.GetDriver());
    auto session = client.CreateSession().GetValueSync().GetSession();

    NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `Root/Database1/Table1` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );
    )").GetValueSync());

    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Database1/Table1` (Key, Value) VALUES
            (1u, "A"),
            (2u, "B"),
            (3u, "C");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());

    NKqp::AssertSuccessResult(session.ExecuteSchemeQuery(R"(
        CREATE TABLE `Root/Database2/Table2` (
            Key Uint64,
            Value String,
            PRIMARY KEY (Key)
        );
    )").GetValueSync());

    NKqp::AssertSuccessResult(session.ExecuteDataQuery(R"(
        REPLACE INTO `Root/Database2/Table2` (Key, Value) VALUES
            (4u, "D"),
            (5u, "E");
    )", TTxControl::BeginTx().CommitTx()).GetValueSync());
}

void CreateDatabasesAndTables(TTestEnv& env) {
    CreateDatabases(env);
    CreateTables(env);
}

} // namespace

Y_UNIT_TEST_SUITE(DbCounters) {

    Y_UNIT_TEST(TabletsSimple) {
        TTestEnv env(1, 2, 0, 0, true);
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
}

} // NSysView
} // NKikimr
