#include <ydb/core/tx/datashard/ut_common/datashard_ut_common.h>
#include "datashard_ut_common_kqp.h"

#include <ydb/core/base/tablet.h>
#include <ydb/core/scheme/scheme_types_defs.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/util/pb.h>
#include <ydb/public/lib/deprecated/kicli/kicli.h>

#include <util/string/printf.h>

namespace NKikimr {

using namespace NKikimr::NDataShard::NKqpHelpers;
using namespace NSchemeShard;
using namespace Tests;
using NClient::TValue;

namespace {

// TEvTabletSetTableInfo is not copyable (all-const members), so observers snapshot it.
struct TReportedTableInfo {
    ui64 TabletID;
    ui32 FollowerId;
    TPathId TableId;
    TString TablePath;
    ui64 SchemaVersion;
    ui32 MetricsLevel;

    explicit TReportedTableInfo(const TEvTabletCounters::TEvTabletSetTableInfo &ev)
        : TabletID(ev.TabletID)
        , FollowerId(ev.FollowerId)
        , TableId(ev.TableId)
        , TablePath(ev.TablePath)
        , SchemaVersion(ev.SchemaVersion)
        , MetricsLevel(ev.MetricsLevel)
    {}
};

TString GetTablePath(TTestActorRuntime &runtime,
                     TActorId sender,
                     ui64 tableId,
                     ui64 shard)
{
    auto request = MakeHolder<TEvTablet::TEvLocalMKQL>();
    const char *miniKQL =   R"___((
        (let row '('('Tid (Uint64 '%lu))))
        (let select '('Schema))
        (let pgmReturn (AsList
            (SetResult 'myRes (SelectRow 'UserTables row select))
        ))
        (return pgmReturn)
    ))___";

    request->Record.MutableProgram()->MutableProgram()->SetText(Sprintf(miniKQL, tableId));
    runtime.SendToPipe(shard, sender, request.Release(), 0, GetPipeConfigWithRetries());

    TAutoPtr<IEventHandle> handle;
    auto reply = runtime.GrabEdgeEventRethrow<TEvTablet::TEvLocalMKQLResponse>(handle);
    auto &res = reply->Record.GetExecutionEngineEvaluatedResponse();
    UNIT_ASSERT_VALUES_EQUAL(reply->Record.GetStatus(), 0);
    TValue value = TValue::Create(res.GetValue(), res.GetType());
    TString schema = value["myRes"]["Schema"];

    NKikimrSchemeOp::TTableDescription desc;
    Y_PROTOBUF_SUPPRESS_NODISCARD desc.ParseFromArray(schema.data(), schema.size());

    return desc.GetPath();
}

// The test runtime sets up the leader Tablet Counters Aggregator only, and
// events sent to an absent service never reach the observers.
void SetupFollowerCountersAggregator(TTestActorRuntime &runtime) {
    runtime.RegisterService(MakeTabletCountersAggregatorID(runtime.GetNodeId(0), true),
        runtime.Register(CreateTabletCountersAggregator(true)));
}

// How many periodic scheme syncs the follower of the tablet has completed.
ui64 GetFollowerSchemeSyncCount(TTestActorRuntime &runtime, ui64 tabletId) {
    auto sender = runtime.AllocateEdgeActor();
    auto pipeConfig = GetPipeConfigWithRetries();
    pipeConfig.ForceFollower = true;
    runtime.SendToPipe(tabletId, sender, new TEvTablet::TEvGetCounters(), 0, pipeConfig);
    auto ev = runtime.GrabEdgeEventRethrow<TEvTablet::TEvGetCountersResponse>(sender);
    for (const auto &counter : ev->Get()->Record.GetTabletCounters().GetAppCounters().GetCumulativeCounters()) {
        if (counter.GetName() == "DataShard/TxSyncSchemeOnFollower/RoCompleted") {
            return counter.GetValue();
        }
    }
    UNIT_FAIL("DataShard/TxSyncSchemeOnFollower/RoCompleted counter not found");
    return 0;
}

// The latest report of the table from its leader or from its follower, if any.
const TReportedTableInfo* FindLastReport(const TVector<TReportedTableInfo> &reported,
                                         const TString &tablePath,
                                         bool follower)
{
    for (auto it = reported.rbegin(); it != reported.rend(); ++it) {
        if (it->TablePath == tablePath && (it->FollowerId != 0) == follower) {
            return &*it;
        }
    }
    return nullptr;
}

}

Y_UNIT_TEST_SUITE(TTxDataShardTestInit) {

    Y_UNIT_TEST(TestGetShardStateAfterInitialization) {
        TTestBasicRuntime runtime;
        TTester::Setup(runtime);

        TActorId sender = runtime.AllocateEdgeActor();
        CreateTestBootstrapper(runtime, CreateTestTabletInfo(TTestTxConfig::TxTablet0, TTabletTypes::DataShard), &CreateDataShard);

        TDispatchOptions options;
        options.FinalEvents.push_back(TDispatchOptions::TFinalEventCondition(TEvTablet::EvTabletActive));
        runtime.DispatchEvents(options);

        Y_UNUSED(sender);
        ForwardToTablet(runtime, TTestTxConfig::TxTablet0, sender, new TEvDataShard::TEvGetShardState(sender));
        TAutoPtr<IEventHandle> handle;
        auto event = runtime.GrabEdgeEvent<TEvDataShard::TEvGetShardStateResult>(handle);
        UNIT_ASSERT(event);
        UNIT_ASSERT_EQUAL(event->GetOrigin(), TTestTxConfig::TxTablet0);
        UNIT_ASSERT_EQUAL(event->GetState(), NDataShard::TShardState::WaitScheme);
    }

    void TestTablePath(bool oldCreate, bool restart)
    {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();
        TAutoPtr<IEventHandle> handle;

        runtime.SetLogPriority(NKikimrServices::TX_DATASHARD, NLog::PRI_DEBUG);

        InitRoot(server, sender);

        ui64 tableId = 0;
        bool sawResolve = false;
        bool dropResolve = restart;
        // Remove table path from propose.
        auto captureTableId = [&](TAutoPtr<IEventHandle> &event) -> auto {
            if (event->GetTypeRewrite() == TEvDataShard::EvProposeTransaction) {
                auto &rec = event->Get<TEvDataShard::TEvProposeTransaction>()->Record;
                if (rec.GetTxKind() == NKikimrTxDataShard::TX_KIND_SCHEME) {
                    TString body = rec.GetTxBody();
                    NKikimrTxDataShard::TFlatSchemeTransaction tx;
                    Y_PROTOBUF_SUPPRESS_NODISCARD tx.ParseFromArray(body.data(), body.size());
                    if (tx.HasCreateTable()) {
                        tableId = tx.GetCreateTable().GetId_Deprecated();
                        if (tx.GetCreateTable().HasPathId()) {
                            UNIT_ASSERT_EQUAL(ChangeStateStorage(Tests::SchemeRoot, serverSettings.Domain), tx.GetCreateTable().GetPathId().GetOwnerId());
                            tableId = tx.GetCreateTable().GetPathId().GetLocalId();
                        }
                        if (oldCreate) {
                            tx.MutableCreateTable()->ClearPath();
                            Y_PROTOBUF_SUPPRESS_NODISCARD tx.SerializeToString(&body);
                            rec.SetTxBody(body);
                        }
                    }
                }
            } else if (event->GetTypeRewrite() == TEvSchemeShard::EvDescribeSchemeResult) {
                auto &rec = event->Get<TEvSchemeShard::TEvDescribeSchemeResult>()->GetRecord();
                const bool hasPartitioning = rec.GetPathDescription().TablePartitionsSize();
                // there are few in-flight TEvDescribeSchemeResult msgs, we need one with no partitioning
                if (!hasPartitioning && rec.GetPathDescription().GetSelf().GetPathId() == tableId) {
                    sawResolve = true;
                    if (dropResolve)
                        return TTestActorRuntime::EEventAction::DROP;
                }
            }
            return TTestActorRuntime::EEventAction::PROCESS;
        };
        runtime.SetObserverFunc(captureTableId);

        CreateShardedTable(server, sender, "/Root", "table-1", 1);
        UNIT_ASSERT(tableId);

        dropResolve = false;
        ui64 tabletId = GetTableShards(server, sender, "/Root/table-1")[0];
        if (restart) {
            sawResolve = false;
            runtime.Register(CreateTabletKiller(tabletId));

            if (oldCreate) {
                while (!sawResolve) {
                    TDispatchOptions options;
                    options.FinalEvents.emplace_back(TEvSchemeShard::EvDescribeSchemeResult);
                    runtime.DispatchEvents(options);
                }
            }
        }

        TString path = GetTablePath(runtime, sender, tableId, tabletId);
        UNIT_ASSERT_VALUES_EQUAL(path, "/Root/table-1");
    }

    Y_UNIT_TEST(TestTableHasPath) {
        TestTablePath(false, false);
    }

    Y_UNIT_TEST(TestResolvePathAfterRestart) {
        TestTablePath(true, true);
    }

    Y_UNIT_TEST(TestSetTableInfoReflectsRename) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        auto shard = GetTableShards(server, sender, "/Root/table-1")[0];

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        // DataShard reports its identity from DoPeriodicTasks(), which reschedules every 5s.
        SimulateSleep(server, TDuration::Seconds(6));

        UNIT_ASSERT(!reported.empty());
        UNIT_ASSERT_VALUES_EQUAL(reported.back().TabletID, shard);
        UNIT_ASSERT_VALUES_EQUAL(reported.back().FollowerId, 0u);
        UNIT_ASSERT_VALUES_EQUAL(reported.back().TablePath, "/Root/table-1");
        auto prevTableId = reported.back().TableId;

        WaitTxNotification(server, sender, AsyncMoveTable(server, "/Root/table-1", "/Root/table-1-moved"));

        // No cache to invalidate: the very next tick reads the renamed table out of TableInfos.
        reported.clear();
        SimulateSleep(server, TDuration::Seconds(6));

        UNIT_ASSERT(!reported.empty());
        UNIT_ASSERT_VALUES_EQUAL(reported.back().TablePath, "/Root/table-1-moved");
        UNIT_ASSERT_UNEQUAL(reported.back().TableId, prevTableId);
    }

    Y_UNIT_TEST(TestSetTableInfoNotSentWithoutFeatureFlag) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        // EnableDataShardDetailedMetrics defaults to false
        serverSettings.SetDomainName("Root").SetUseRealThreads(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        SimulateSleep(server, TDuration::Seconds(6));

        UNIT_ASSERT(reported.empty());
    }

    Y_UNIT_TEST(TestSetTableInfoReflectsTableMetricsLevel) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        // No per-table override and no database default => nothing to report.
        SimulateSleep(server, TDuration::Seconds(6));
        UNIT_ASSERT(!reported.empty());
        UNIT_ASSERT_VALUES_EQUAL(reported.back().MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified));

        WaitTxNotification(server, sender, AsyncAlterSetMetricsLevel(server, "/Root", "table-1",
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable));

        reported.clear();
        SimulateSleep(server, TDuration::Seconds(6));
        UNIT_ASSERT(!reported.empty());
        UNIT_ASSERT_VALUES_EQUAL(reported.back().MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable));

        WaitTxNotification(server, sender, AsyncAlterSetMetricsLevel(server, "/Root", "table-1",
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        reported.clear();
        SimulateSleep(server, TDuration::Seconds(6));
        UNIT_ASSERT(!reported.empty());
        UNIT_ASSERT_VALUES_EQUAL(reported.back().MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));
    }

    Y_UNIT_TEST(TestSetTableInfoReflectsIndexImplTableMetricsLevel) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Indexes({{"by_value", {"value"}}}));

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        WaitTxNotification(server, sender, AsyncAlterSetMetricsLevel(server, "/Root", "table-1",
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        reported.clear();
        // DataShard reports its identity from DoPeriodicTasks(), which reschedules every 5s.
        SimulateSleep(server, TDuration::Seconds(6));

        auto byPathSuffix = [&](const TString &suffix) {
            TVector<TReportedTableInfo> matched;
            for (const auto &info : reported) {
                if (info.TablePath.EndsWith(suffix)) {
                    matched.push_back(info);
                }
            }
            return matched;
        };

        auto indexReports = byPathSuffix("/by_value/indexImplTable");
        UNIT_ASSERT_C(!indexReports.empty(), "expected at least one report from the index impl table shard");
        for (const auto &info : indexReports) {
            UNIT_ASSERT_VALUES_EQUAL_C(info.MetricsLevel,
                ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition),
                "index impl table " << info.TablePath << " did not inherit the base table's metrics level");
        }

        auto baseReports = byPathSuffix("/table-1");
        UNIT_ASSERT_C(!baseReports.empty(), "expected at least one report from the base table shard");
        for (const auto &info : baseReports) {
            UNIT_ASSERT_VALUES_EQUAL_C(info.MetricsLevel,
                ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition),
                "base table " << info.TablePath << " did not report its own metrics level");
        }
    }

    // The database-wide TABLES_METRICS_LEVEL reaches DataShard on the subdomain
    // publish, which bumps no table SchemaVersion. Patch the published subdomain
    // description on the wire so the DataShard-side handling can be exercised
    // without a control-plane surface for the database attribute.
    Y_UNIT_TEST(TestSetTableInfoUsesSubDomainMetricsLevel) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        // Installed before the shard exists, so the very first subdomain
        // notification it gets already carries the database default.
        auto patcher = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [&](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                auto *msg = ev->Get();
                NKikimrScheme::TEvDescribeSchemeResult record = *msg->Result;
                record.MutablePathDescription()->MutableDomainDescription()->SetTablesMetricsLevel(
                    NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable);
                msg->Result = NSchemeCache::TDescribeResult::Create(record);
            });

        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        SimulateSleep(server, TDuration::Seconds(6));

        UNIT_ASSERT(!reported.empty());
        UNIT_ASSERT_VALUES_EQUAL(reported.back().MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable));

        // A per-table override wins over the database default.
        WaitTxNotification(server, sender, AsyncAlterSetMetricsLevel(server, "/Root", "table-1",
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        reported.clear();
        SimulateSleep(server, TDuration::Seconds(6));
        UNIT_ASSERT(!reported.empty());
        UNIT_ASSERT_VALUES_EQUAL(reported.back().MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));
    }

    // The database-wide default is persisted, so a restarted shard keeps
    // reporting it instead of falling back to Unspecified until the next
    // subdomain publish arrives.
    Y_UNIT_TEST(TestSubDomainMetricsLevelSurvivesRestart) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        auto patcher = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [&](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                auto *msg = ev->Get();
                NKikimrScheme::TEvDescribeSchemeResult record = *msg->Result;
                record.MutablePathDescription()->MutableDomainDescription()->SetTablesMetricsLevel(
                    NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition);
                msg->Result = NSchemeCache::TDescribeResult::Create(record);
            });

        CreateShardedTable(server, sender, "/Root", "table-1", 1);

        auto shard = GetTableShards(server, sender, "/Root/table-1")[0];

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        SimulateSleep(server, TDuration::Seconds(6));
        UNIT_ASSERT(!reported.empty());
        UNIT_ASSERT_VALUES_EQUAL(reported.back().MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        // Cut the subscription off entirely, so the restarted shard can only
        // know the database default from its own local database.
        patcher.Remove();
        auto blocker = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                ev.Reset();
            });

        RebootTablet(runtime, shard, sender);

        reported.clear();
        SimulateSleep(server, TDuration::Seconds(6));
        UNIT_ASSERT(!reported.empty());
        UNIT_ASSERT_VALUES_EQUAL(reported.back().MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));
    }

    // Followers never watch the subdomain: the database default reaches them
    // only through the Sys row persisted by the leader, which a follower
    // reloads when it syncs its scheme.
    Y_UNIT_TEST(TestSetTableInfoFollowerUsesSubDomainMetricsLevel) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        SetupFollowerCountersAggregator(runtime);

        auto patcher = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [&](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                auto *msg = ev->Get();
                NKikimrScheme::TEvDescribeSchemeResult record = *msg->Result;
                record.MutablePathDescription()->MutableDomainDescription()->SetTablesMetricsLevel(
                    NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition);
                msg->Result = NSchemeCache::TDescribeResult::Create(record);
            });

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(1));

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        // A stale read of a single shard table is served by the follower
        KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");

        SimulateSleep(server, TDuration::Seconds(6));

        const auto *followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));
    }

    Y_UNIT_TEST(TestSetTableInfoFollowerSeesSubDomainMetricsLevelChange) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        SetupFollowerCountersAggregator(runtime);

        auto level = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition;
        auto patcher = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [&](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                auto *msg = ev->Get();
                NKikimrScheme::TEvDescribeSchemeResult record = *msg->Result;
                record.MutablePathDescription()->MutableDomainDescription()->SetTablesMetricsLevel(level);
                msg->Result = NSchemeCache::TDescribeResult::Create(record);
            });

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(1));

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");

        SimulateSleep(server, TDuration::Seconds(6));

        const auto *followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        // Creating a table changes /Root, which republishes the subdomain
        // description with the new database default
        level = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable;
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        reported.clear();
        SimulateSleep(server, TDuration::Seconds(6));

        const auto *leaderReport = FindLastReport(reported, "/Root/table-1", false);
        UNIT_ASSERT_C(leaderReport, "expected a report from the leader");
        UNIT_ASSERT_VALUES_EQUAL(leaderReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable));

        // The follower picks up the persisted level on its next scheme sync
        KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");

        reported.clear();
        SimulateSleep(server, TDuration::Seconds(6));

        followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable));
    }

    // A follower serving no requests syncs its scheme periodically, so it
    // reports its table and follows level changes without a single read.
    Y_UNIT_TEST(TestSetTableInfoIdleFollowerUsesSubDomainMetricsLevel) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        SetupFollowerCountersAggregator(runtime);

        auto level = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition;
        auto patcher = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [&](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                auto *msg = ev->Get();
                NKikimrScheme::TEvDescribeSchemeResult record = *msg->Result;
                record.MutablePathDescription()->MutableDomainDescription()->SetTablesMetricsLevel(level);
                msg->Result = NSchemeCache::TDescribeResult::Create(record);
            });

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(1));

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        // A sync that has to wait is reported on the tick after it
        SimulateSleep(server, TDuration::Seconds(11));

        const auto *followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        level = NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable;
        CreateShardedTable(server, sender, "/Root", "table-2", 1);

        reported.clear();
        SimulateSleep(server, TDuration::Seconds(11));

        followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelTable));
    }

    // A per-table METRICS_LEVEL change rewrites the table's UserTables row but
    // leaves the local schema intact, so only a data update reaches the follower.
    Y_UNIT_TEST(TestSetTableInfoIdleFollowerSeesTableMetricsLevelChange) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        SetupFollowerCountersAggregator(runtime);

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(1));

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        SimulateSleep(server, TDuration::Seconds(11));

        const auto *followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelUnspecified));
        const ui64 prevSchemaVersion = followerReport->SchemaVersion;

        WaitTxNotification(server, sender, AsyncAlterSetMetricsLevel(server, "/Root", "table-1",
            NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        reported.clear();
        SimulateSleep(server, TDuration::Seconds(11));

        followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        // The aggregator drops reports with a stale SchemaVersion, so the
        // follower must report the same version as the leader
        const auto *leaderReport = FindLastReport(reported, "/Root/table-1", false);
        UNIT_ASSERT_C(leaderReport, "expected a report from the leader");
        UNIT_ASSERT_GT(followerReport->SchemaVersion, prevSchemaVersion);
        UNIT_ASSERT_VALUES_EQUAL(followerReport->SchemaVersion, leaderReport->SchemaVersion);
    }

    // The periodic sync runs only after the leader's changes reach the follower,
    // so a follower of an idle leader stops paying for it.
    Y_UNIT_TEST(TestIdleFollowerSyncsSchemeOnlyAfterLeaderChanges) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        // The leader keeps persisting its low watermark for KeepSnapshotTimeout
        // after its last write. The default of 5 minutes is never reached by
        // the simulated clock starting at zero, so pin it to really settle down
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetKeepSnapshotTimeout(TDuration::Seconds(1))
            .SetEnableDataShardDetailedMetrics(true)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(1));

        auto shard = GetTableShards(server, sender, "/Root/table-1")[0];

        // Let the follower sync the initial state and the leader advance its
        // low watermark, which happens once per 15s snapshot cleanup period
        SimulateSleep(server, TDuration::Seconds(35));

        const ui64 settled = GetFollowerSchemeSyncCount(runtime, shard);
        UNIT_ASSERT_GT(settled, 0u);

        // Longer than a snapshot cleanup period
        SimulateSleep(server, TDuration::Seconds(20));
        UNIT_ASSERT_VALUES_EQUAL(GetFollowerSchemeSyncCount(runtime, shard), settled);

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");

        SimulateSleep(server, TDuration::Seconds(6));
        UNIT_ASSERT_GT(GetFollowerSchemeSyncCount(runtime, shard), settled);
    }

    // Without detailed metrics a follower syncs its scheme for requests only
    Y_UNIT_TEST(TestIdleFollowerSkipsSchemeSyncWithoutFeatureFlag) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        // EnableDataShardDetailedMetrics defaults to false
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(1));

        auto shard = GetTableShards(server, sender, "/Root/table-1")[0];

        ExecSQL(server, sender, "UPSERT INTO `/Root/table-1` (key, value) VALUES (1, 1);");

        SimulateSleep(server, TDuration::Seconds(11));
        UNIT_ASSERT_VALUES_EQUAL(GetFollowerSchemeSyncCount(runtime, shard), 0u);
    }

    // Followers run periodic tasks only with EnableFollowerStats on, so they
    // neither sync their scheme periodically nor report their tables
    Y_UNIT_TEST(TestSetTableInfoNotSentByFollowerWithoutFollowerStats) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true)
            .SetEnableForceFollowers(true)
            .SetEnableFollowerStats(false);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        SetupFollowerCountersAggregator(runtime);

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(1));

        auto shard = GetTableShards(server, sender, "/Root/table-1")[0];

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        // The request syncs the scheme of the follower, which still has
        // no periodic tasks to report it
        KqpSimpleStaleRoExec(runtime, "SELECT * FROM `/Root/table-1`", "/Root");

        SimulateSleep(server, TDuration::Seconds(11));

        UNIT_ASSERT_C(FindLastReport(reported, "/Root/table-1", false), "expected a report from the leader");
        UNIT_ASSERT_C(!FindLastReport(reported, "/Root/table-1", true), "expected no report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(GetFollowerSchemeSyncCount(runtime, shard), 0u);
    }

    // A restarted follower starts over with a fresh state, which comes without
    // follower update notifications, so it syncs its scheme on activation
    Y_UNIT_TEST(TestSetTableInfoRestartedIdleFollowerUsesSubDomainMetricsLevel) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        SetupFollowerCountersAggregator(runtime);

        auto patcher = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [&](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                auto *msg = ev->Get();
                NKikimrScheme::TEvDescribeSchemeResult record = *msg->Result;
                record.MutablePathDescription()->MutableDomainDescription()->SetTablesMetricsLevel(
                    NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition);
                msg->Result = NSchemeCache::TDescribeResult::Create(record);
            });

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(1));

        auto shard = GetTableShards(server, sender, "/Root/table-1")[0];

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        SimulateSleep(server, TDuration::Seconds(11));

        const auto *followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        // Keep the leader from learning anything new, so it stays idle
        patcher.Remove();
        auto blocker = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                ev.Reset();
            });

        SendViaPipeCache(runtime, shard, sender,
            std::make_unique<TEvents::TEvPoison>(),
            { .Follower = true });

        // Let the old follower die before collecting reports of the new one
        SimulateSleep(server, TDuration::Seconds(1));
        reported.clear();
        SimulateSleep(server, TDuration::Seconds(11));

        followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the restarted follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));
    }

    // A leader reboot reactivates its followers, which start over with a
    // fresh state and must keep reporting their tables
    Y_UNIT_TEST(TestSetTableInfoIdleFollowerSurvivesLeaderReboot) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        SetupFollowerCountersAggregator(runtime);

        auto patcher = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [&](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                auto *msg = ev->Get();
                NKikimrScheme::TEvDescribeSchemeResult record = *msg->Result;
                record.MutablePathDescription()->MutableDomainDescription()->SetTablesMetricsLevel(
                    NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition);
                msg->Result = NSchemeCache::TDescribeResult::Create(record);
            });

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(1));

        auto shard = GetTableShards(server, sender, "/Root/table-1")[0];

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        SimulateSleep(server, TDuration::Seconds(11));

        const auto *followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        // The rebooted leader knows the database default from its local database only
        patcher.Remove();
        auto blocker = runtime.AddObserver<TEvTxProxySchemeCache::TEvWatchNotifyUpdated>(
            [](TEvTxProxySchemeCache::TEvWatchNotifyUpdated::TPtr &ev) {
                ev.Reset();
            });

        RebootTablet(runtime, shard, sender);

        reported.clear();
        SimulateSleep(server, TDuration::Seconds(11));

        const auto *leaderReport = FindLastReport(reported, "/Root/table-1", false);
        UNIT_ASSERT_C(leaderReport, "expected a report from the leader");
        UNIT_ASSERT_VALUES_EQUAL(leaderReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));

        followerReport = FindLastReport(reported, "/Root/table-1", true);
        UNIT_ASSERT_C(followerReport, "expected a report from the follower");
        UNIT_ASSERT_VALUES_EQUAL(followerReport->MetricsLevel,
            ui32(NKikimrSchemeOp::TTableDetailedMetricsSettings::MetricsLevelPartition));
    }

    Y_UNIT_TEST(TestSetTableInfoReportedByEveryIdleFollower) {
        TPortManager pm;
        TServerSettings serverSettings(pm.GetPort(2134));
        serverSettings.SetDomainName("Root")
            .SetUseRealThreads(false)
            .SetEnableDataShardDetailedMetrics(true)
            .SetEnableForceFollowers(true);

        Tests::TServer::TPtr server = new TServer(serverSettings);
        auto &runtime = *server->GetRuntime();
        auto sender = runtime.AllocateEdgeActor();

        InitRoot(server, sender);
        SetupFollowerCountersAggregator(runtime);

        CreateShardedTable(server, sender, "/Root", "table-1",
            TShardedTableOptions().Followers(2));

        TVector<TReportedTableInfo> reported;
        auto observer = runtime.AddObserver<TEvTabletCounters::TEvTabletSetTableInfo>(
            [&](TEvTabletCounters::TEvTabletSetTableInfo::TPtr &ev) {
                reported.push_back(TReportedTableInfo(*ev->Get()));
            });

        SimulateSleep(server, TDuration::Seconds(11));

        THashSet<ui32> followerIds;
        for (const auto &info : reported) {
            if (info.TablePath == "/Root/table-1" && info.FollowerId != 0) {
                followerIds.insert(info.FollowerId);
            }
        }
        UNIT_ASSERT_VALUES_EQUAL(followerIds.size(), 2u);
    }
}

}
