#include <ydb/core/statistics/aggregator/analyze_scan_budget.h>

#include <ydb/core/cms/console/console.h>
#include <ydb/core/statistics/ut_common/ut_common.h>
#include <ydb/core/testlib/actors/block_events.h>
#include <ydb/core/testlib/actors/test_runtime.h>
#include <ydb/core/testlib/basics/appdata.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NStat {
namespace {

using TEvBudget = TEvAnalyzeScanBudget;

// Hold each slot until the test explicitly finishes the scan, even on cancellation.
class THeldScan : public TActorBootstrapped<THeldScan> {
    TActorId Edge;
    ui64 Label;
    TActorId Budget;

public:
    THeldScan(TActorId edge, ui64 label)
        : Edge(edge)
        , Label(label)
    {}

    void Bootstrap(const TActorId& owner) {
        Budget = owner;
        Send(Edge, new TEvents::TEvWakeup(Label));
        Become(&TThis::StateWork);
    }

    void OnPoison() {
        Send(Edge, new TEvents::TEvGone());
    }

    void Finish() {
        Send(Budget, new TEvBudget::TEvStopped());
        PassAway();
    }

    STRICT_STFUNC(StateWork,
        sFunc(TEvents::TEvPoison, OnPoison)
        sFunc(TEvents::TEvWakeup, Finish)
    )
};

struct TFixture {
    TTestActorRuntime Runtime;
    TActorId Edge;
    TActorId Budget;

    TFixture(ui32 total, ui32 perNode) {
        Runtime.Initialize(TAppPrepare(TAppPrepare::TLightweightTag{}).Unwrap());
        Edge = Runtime.AllocateEdgeActor();
        Budget = Runtime.Register(CreateAnalyzeScanBudget(total, perNode));
    }

    TActorId Open(bool userRequested) {
        auto table = Runtime.AllocateEdgeActor();
        Runtime.Send(Budget, table, new TEvBudget::TEvOpen(userRequested), 0, true);
        return table;
    }

    void Submit(TActorId table, ui32 node, ui64 label) {
        Runtime.Send(Budget, table, new TEvBudget::TEvSubmit(node, std::make_unique<THeldScan>(Edge, label)), 0, true);
    }

    TActorId ExpectStart(ui64 label) {
        auto ev = Runtime.GrabEdgeEvent<TEvents::TEvWakeup>(Edge, TDuration::Seconds(1));
        UNIT_ASSERT_C(ev, "Expected scan " << label << " to start");
        UNIT_ASSERT_VALUES_EQUAL(ev->Get()->Tag, label);
        return ev->Sender;
    }

    void ExpectWaiting() {
        // The test runtime advances simulated time after draining runnable mailboxes.
        Runtime.Schedule(new IEventHandle(Edge, {}, new TEvents::TEvWakeup(0)), TDuration::MilliSeconds(1));
        ExpectStart(0);
    }

    void Finish(TActorId worker) {
        Runtime.Send(worker, Edge, new TEvents::TEvWakeup(), 0, true);
    }
};

} // namespace

Y_UNIT_TEST_SUITE(AnalyzeScanBudget) {
    Y_UNIT_TEST(UserTablesShareTurnsAheadOfBackground) {
        TFixture env(1, 1);
        const auto background = env.Open(false);
        const auto tableA = env.Open(true);
        const auto tableB = env.Open(true);
        env.Submit(background, 1, 10);
        const auto active = env.ExpectStart(10);
        env.Submit(background, 1, 11);
        env.Submit(tableA, 1, 20);
        env.Submit(tableA, 1, 21);
        env.Submit(tableB, 1, 30);
        env.ExpectWaiting();
        env.Finish(active);
        env.Finish(env.ExpectStart(20));
        env.Finish(env.ExpectStart(30));
        env.Finish(env.ExpectStart(21));
        env.Finish(env.ExpectStart(11));
    }

    Y_UNIT_TEST(BackgroundBlockingAUserSharesUserTurns) {
        TFixture env(1, 1);
        const auto user = env.Open(true);
        const auto unrelated = env.Open(false);
        env.Submit(user, 1, 1);
        const auto active = env.ExpectStart(1);
        env.Submit(unrelated, 1, 10);

        // The request may queue while the background actor is still locating its shards.
        const auto blocking = env.Runtime.AllocateEdgeActor();
        env.Runtime.Send(env.Budget, env.Edge, new TEvBudget::TEvSetUserDependencies({blocking}), 0, true);
        env.Runtime.Send(env.Budget, blocking, new TEvBudget::TEvOpen(false), 0, true);
        env.Submit(blocking, 1, 20);
        env.Submit(blocking, 1, 21);
        env.Submit(user, 1, 2);
        env.Submit(user, 1, 3);
        env.Submit(env.Open(true), 1, 4);

        env.Finish(active);
        env.Finish(env.ExpectStart(2));
        const auto promoted = env.ExpectStart(20);
        // The waiter cancels. Ordinary user work should again precede both backgrounds.
        env.Runtime.Send(env.Budget, env.Edge, new TEvBudget::TEvSetUserDependencies({}), 0, true);
        env.Finish(promoted);
        env.Finish(env.ExpectStart(4));
        env.Finish(env.ExpectStart(3));
        env.Finish(env.ExpectStart(10));
        env.Finish(env.ExpectStart(21));
    }

    Y_UNIT_TEST(QueuedUserPromotesBackgroundOnTheSamePathUntilCancellation) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            settings.AppConfig->MutableStatisticsConfig()->SetEnableBackgroundColumnStatsCollection(true);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        TBlockEvents<TEvBudget::TEvSubmit> scans(runtime);
        const auto table = CreateEmptyTable(env, "Database", "Table", false);
        runtime.WaitFor("background scan", [&] { return !scans.empty(); }, TDuration::Seconds(30));
        const auto owner = scans.front()->Sender;
        const auto budget = scans.front()->GetRecipientRewrite();
        THashSet<TActorId> dependencies;
        auto priorities = runtime.AddObserver<TEvBudget::TEvSetUserDependencies>([&](auto& ev) {
            if (ev->GetRecipientRewrite() == budget) {
                dependencies = ev->Get()->Owners;
            }
        });
        const auto sender = runtime.AllocateEdgeActor();
        runtime.SendToPipe(table.SaTabletId, sender,
            MakeAnalyzeRequest({table.PathId}, "wait-for-background", "/Root/Database").release());
        runtime.WaitFor("background inherits user priority", [&] { return dependencies.contains(owner); }, TDuration::Seconds(30));
        UNIT_ASSERT_VALUES_EQUAL(dependencies.size(), 1);

        TestCancelAnalyzeOp(runtime, table.SaTabletId, "/Root/Database", "wait-for-background", Ydb::StatusIds::SUCCESS);
        runtime.WaitFor("background returns to normal priority", [&] { return dependencies.empty(); }, TDuration::Seconds(30));
        scans.Stop().Unblock();
    }

    Y_UNIT_TEST(BlockedNodeDoesNotBlockOtherNodesOrWholeTableQueries) {
        TFixture env(2, 1);
        const auto tableA = env.Open(true);
        const auto tableB = env.Open(true);
        env.Submit(tableA, 1, 1);
        const auto first = env.ExpectStart(1);
        env.Submit(tableB, 1, 2);
        env.Submit(tableB, 2, 3);
        const auto otherNode = env.ExpectStart(3);
        env.Submit(tableA, 0, 4);
        env.Submit(tableB, 0, 5);
        env.ExpectWaiting();
        env.Finish(otherNode);
        const auto wholeA = env.ExpectStart(4);
        env.ExpectWaiting();
        // Releasing node 1 admits B's waiting node-1 scan before its whole-table scan.
        env.Finish(first);
        env.Finish(env.ExpectStart(2));
        const auto wholeB = env.ExpectStart(5);
        env.Finish(wholeA);
        env.Finish(wholeB);
    }

    Y_UNIT_TEST(CancelRetainsRunningSlotAndDropsOnlyThatTablesQueue) {
        TFixture env(0, 0);
        const auto tableA = env.Open(true);
        const auto tableB = env.Open(true);
        env.Submit(tableA, 1, 1);
        const auto first = env.ExpectStart(1);
        env.Submit(tableA, 1, 2);
        env.Submit(tableB, 2, 3);
        env.Runtime.Send(env.Budget, tableA, new TEvBudget::TEvClose(), 0, true);
        env.Submit(tableA, 1, 4); // Late submission from the cancelled attempt.
        auto cancelled = env.Runtime.GrabEdgeEvent<TEvents::TEvGone>(env.Edge, TDuration::Seconds(1));
        UNIT_ASSERT(cancelled);
        UNIT_ASSERT_VALUES_EQUAL(cancelled->Sender, first);
        env.ExpectWaiting();
        env.Finish(first);
        const auto second = env.ExpectStart(3);
        env.Runtime.Send(env.Budget, first, new TEvBudget::TEvStopped(), 0, true);
        env.Submit(tableB, 2, 5);
        env.ExpectWaiting();
        env.Finish(second);
        env.Finish(env.ExpectStart(5));
        env.ExpectWaiting();
    }

    Y_UNIT_TEST(LowerLimitsDrainRunningScans) {
        TFixture env(2, 2);
        const auto table = env.Open(true);
        env.Submit(table, 1, 1);
        const auto first = env.ExpectStart(1);
        env.Submit(table, 1, 2);
        const auto second = env.ExpectStart(2);
        env.Submit(table, 1, 3);
        env.Runtime.Send(env.Budget, env.Edge, new TEvBudget::TEvConfigure(0, 0), 0, true);
        env.ExpectWaiting();
        env.Finish(first);
        env.ExpectWaiting();
        env.Finish(second);
        env.Finish(env.ExpectStart(3));
    }

    Y_UNIT_TEST(ActiveTableRefillsOnScanLimitIncrease) {
        TTestEnv env(1, 1, false, [](Tests::TServerSettings& settings) {
            auto* config = settings.AppConfig->MutableStatisticsConfig();
            config->SetAnalyzeRowTableWholeTableScanMaxBytes(0);
            config->SetAnalyzeMaxTotalScanActorsInFlight(1);
            config->SetAnalyzeMaxPerNodeScanActorsInFlight(2);
        });
        auto& runtime = *env.GetServer().GetRuntime();
        CreateDatabase(env, "Database");
        const auto table = PrepareTable(env, "Database", "Table", false);
        const auto sender = runtime.AllocateEdgeActor();
        // Keep every submitted range outstanding. Only a live config change can trigger a refill.
        TBlockEvents<TEvBudget::TEvSubmit> submitted(runtime);
        runtime.SendToPipe(table.SaTabletId, sender,
            MakeAnalyzeRequest({table.PathId}, "raise-scan-limits", "/Root/Database").release());

        auto expectSubmitted = [&](size_t count) {
            runtime.WaitFor("submitted ANALYZE scans", [&] { return submitted.size() >= count; }, TDuration::Seconds(30));
            runtime.SimulateSleep(TDuration::MilliSeconds(1));
            UNIT_ASSERT_VALUES_EQUAL(submitted.size(), count);
        };
        auto configure = [&](ui32 total, ui32 perNode) {
            auto request = std::make_unique<NConsole::TEvConsole::TEvConfigNotificationRequest>();
            auto* config = request->Record.MutableConfig()->MutableStatisticsConfig();
            *config = env.GetSettings()->AppConfig->GetStatisticsConfig();
            config->SetAnalyzeMaxTotalScanActorsInFlight(total);
            config->SetAnalyzeMaxPerNodeScanActorsInFlight(perNode);
            runtime.SendToPipe(table.SaTabletId, sender, request.release());
            UNIT_ASSERT(runtime.GrabEdgeEventRethrow<NConsole::TEvConsole::TEvConfigNotificationResponse>(sender));
        };

        // The four ranges are on one node. First the total limit binds, then the per-node limit.
        expectSubmitted(1);
        configure(3, 2);
        expectSubmitted(2);
        configure(3, 3);
        expectSubmitted(3);
        for (const auto& scan : submitted) {
            UNIT_ASSERT(scan->Get()->NodeId);
            UNIT_ASSERT_VALUES_EQUAL(scan->Get()->NodeId, submitted.front()->Get()->NodeId);
        }

        submitted.Stop().Unblock();
        auto response = runtime.GrabEdgeEventRethrow<TEvStatistics::TEvAnalyzeResponse>(sender);
        UNIT_ASSERT_VALUES_EQUAL_C(response->Get()->Record.GetStatus(), NKikimrStat::TEvAnalyzeResponse::STATUS_SUCCESS,
            response->Get()->Record.DebugString());
        ValidateStatistics(runtime, table.PathId, ColumnTableRowsNumber);
    }
}

} // namespace NKikimr::NStat
