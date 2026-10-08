#include <ydb/core/subsystems/actor_system_monitoring/viewer.h>
#include <ydb/core/subsystems/actor_system_monitoring/metrics.h>
#include <library/cpp/testing/unittest/registar.h>
#include <ydb/library/actors/testlib/test_runtime.h>
#include <ydb/library/actors/core/subsystems/inmemory_metrics.h>
#include <util/datetime/base.h>

namespace NKikimr::NActorSystemMonitoring {
Y_UNIT_TEST_SUITE(TActorSystemMonitoringTest) {
    Y_UNIT_TEST(SubsystemStartsAndStopsWithRuntime) {
        for (unsigned iteration = 0; iteration != 2; ++iteration) {
            NActors::TActorId endpoint;
            NActors::TTestActorRuntimeBase runtime(1, true);
            runtime.SetupNodeSubSystems = [&](ui32, NActors::TActorSystemSetup* setup) {
                setup->RegisterSubSystem(NActors::MakeInMemoryMetricsRegistry({
                    .MemoryBytes = 64 * 1024,
                    .MaxLines = 32,
                    .AllowedMetricPrefixes = {"actor_system."},
                }));
                TConfig config;
                config.Pools.emplace_back().Name = "System";
                config.RegisterPage = [&](NActors::TActorSystem&, const NActors::TActorId& actor) {
                    endpoint = actor;
                };
                setup->RegisterSubSystem(MakeActorSystemMonitoring(std::move(config)));
            };
            runtime.Initialize();
            UNIT_ASSERT(endpoint);
            UNIT_ASSERT(runtime.GetActorSystem(0)->GetSubSystem<TActorSystemMonitoring>());
        }
    }

    Y_UNIT_TEST(RatesUseActualInterval) {
        TSnapshot before, after;
        before.Monotonic = TMonotonic::Seconds(10);
        after.Monotonic = TMonotonic::Seconds(12);
        before.Pools.resize(1); after.Pools.resize(1);
        before.Pools[0].CpuUs = 100;
        before.Pools[0].Events = 10;
        after.Pools[0].CpuUs = 3000100;
        after.Pools[0].Events = 510;
        CalculateRates(before, &after);
        UNIT_ASSERT(after.Pools[0].HasRate);
        UNIT_ASSERT_DOUBLES_EQUAL(after.Pools[0].CpuCores, 1.5, 1e-9);
        UNIT_ASSERT_DOUBLES_EQUAL(after.Pools[0].EventsPerSecond, 250, 1e-9);
    }
    Y_UNIT_TEST(PoolMetricsAreRecordedInRegistry) {
        NActors::TTestActorRuntimeBase runtime(1, true);
        runtime.SetupNodeSubSystems = [&](ui32, NActors::TActorSystemSetup* setup) {
            setup->RegisterSubSystem(NActors::MakeInMemoryMetricsRegistry({
                .MemoryBytes = 64 * 1024,
                .MaxLines = 16,
                .AllowedMetricPrefixes = {"actor_system."},
            }));
            TConfig config;
            config.Pools.emplace_back().Name = "System";
            setup->RegisterSubSystem(MakeActorSystemMonitoring(std::move(config)));
        };
        runtime.Initialize();
        auto* registry = NActors::GetInMemoryMetrics(*runtime.GetActorSystem(0));
        const auto edge = runtime.AllocateEdgeActor();
        bool recorded = false;
        bool countersRecorded = false;
        const auto deadline = TMonotonic::Now() + TDuration::Seconds(10);
        while ((!recorded || !countersRecorded) && TMonotonic::Now() < deadline) {
            UNIT_ASSERT(registry->RequestSnapshot(edge, 123));
            auto reply = runtime.GrabEdgeEventRethrow<NActors::TEvInMemoryMetricsSnapshot>(edge, TDuration::Seconds(5));
            reply->Get()->Snapshot.Read([&](const NActors::TSnapshotView& view) {
                view.ForEachLine([&](const NActors::TLineSnapshot& line) {
                    if (line.Name == "actor_system.pools.counters") {
                        UNIT_ASSERT_VALUES_EQUAL(line.Meta.Frontend->Fields.size(), PoolCounterNames.size());
                        UNIT_ASSERT_VALUES_EQUAL(line.Meta.Frontend->Fields[0].Name, PoolCounterNames[0]);
                        line.Meta.Frontend->ReadNumericRange(line, TInstant::Zero(), TInstant::Max(), &countersRecorded,
                            [](void* opaque, TInstant, std::span<const NActors::TLineNumericValue> values) {
                                UNIT_ASSERT_VALUES_EQUAL(values.size(), PoolCounterNames.size());
                                UNIT_ASSERT(std::holds_alternative<ui64>(values[0]));
                                *static_cast<bool*>(opaque) = true;
                            });
                    }
                    if (line.Name == "actor_system.pools") {
                        UNIT_ASSERT(line.Labels.empty());
                        UNIT_ASSERT_VALUES_EQUAL(line.Meta.Frontend->Fields.size(), 4);
                        UNIT_ASSERT_VALUES_EQUAL(line.Meta.Frontend->Fields[0].Labels[1].Value, "System");
                        line.Meta.Frontend->ReadNumericRange(line, TInstant::Zero(), TInstant::Max(), &recorded,
                            [](void* opaque, TInstant, std::span<const NActors::TLineNumericValue> values) {
                                UNIT_ASSERT(std::get<double>(values[3]) > 0);
                                *static_cast<bool*>(opaque) = true;
                            });
                    }
                });
            });
            if (!recorded) {
                Sleep(TDuration::MilliSeconds(20));
            }
        }
        UNIT_ASSERT(countersRecorded);
        UNIT_ASSERT_C(recorded, "Actor-system samples were not recorded in the allowed registry");
    }
    Y_UNIT_TEST(FirstSampleAndResetHaveNoRate) {
        TSnapshot before, after;
        after.Monotonic = TMonotonic::Seconds(12);
        after.Pools.resize(1);
        CalculateRates(before, &after);
        UNIT_ASSERT(!after.Pools[0].HasRate);
        before.Monotonic = TMonotonic::Seconds(10);
        before.Pools.resize(1);
        before.Pools[0].CpuUs = 100;
        CalculateRates(before, &after);
        UNIT_ASSERT(!after.Pools[0].HasRate);
    }
    Y_UNIT_TEST(RenderEscapesNamesAndDoesNotReflectUnknownTab) {
        TSnapshot s;
        s.Timestamp = TInstant::Seconds(100);
        s.Pools.resize(1);
        s.Pools[0].Config.Name = "<script>";
        const auto page = RenderPage(s, "<unsafe>", s.Timestamp);
        UNIT_ASSERT_STRING_CONTAINS(page, "&lt;script&gt;");
        UNIT_ASSERT(!page.Contains("<script>"));
        UNIT_ASSERT(!page.Contains("<unsafe>"));
        UNIT_ASSERT_STRING_CONTAINS(page, "Actor system configuration");
        UNIT_ASSERT(!page.Contains("CPU usage history"));
        UNIT_ASSERT(!page.Contains("http-equiv='refresh'"));
        const auto runtime = RenderPage(s, "runtime", s.Timestamp);
        UNIT_ASSERT_STRING_CONTAINS(runtime, "warming up");
        UNIT_ASSERT_STRING_CONTAINS(runtime, "../static/metric-chart/chart.js");
        UNIT_ASSERT_STRING_CONTAINS(runtime, "createInMemoryMetricsClient({endpoint:'metrics'})");
        UNIT_ASSERT_STRING_CONTAINS(runtime, "actor_system.pool.cpu_cores");
        UNIT_ASSERT_STRING_CONTAINS(runtime, "actor_system.pool.events_per_second");
        UNIT_ASSERT(!runtime.Contains("<svg"));
        UNIT_ASSERT_STRING_CONTAINS(runtime, "type:'area'");
        UNIT_ASSERT_STRING_CONTAINS(runtime, "color:colors.get(pool)");
        UNIT_ASSERT(!page.Contains("<p class='asm-muted'>"));
        UNIT_ASSERT(!runtime.Contains("Snapshot age"));
        UNIT_ASSERT(!runtime.Contains("Collection "));
        UNIT_ASSERT_STRING_CONTAINS(runtime, "CPU usage &middot; cores");
    }
    Y_UNIT_TEST(OverviewShowsStartupConfiguration) {
        TSnapshot s;
        s.Timestamp = TInstant::Seconds(100);
        s.AutoConfigured = true;
        s.SystemParameters = "SysExecutor: 0\nScheduler { Resolution: 64 }";
        s.Pools.resize(2);
        auto& basic = s.Pools[0].Config;
        basic.Name = "System";
        basic.Threads = "4";
        basic.MinThreads = "2";
        basic.MaxThreads = "8";
        basic.Priority = "-1";
        basic.SharedThreads = "One";
        basic.Parameters = "Threads: 4\nAffinity { CpuList: \"<cpu>\" }";
        auto& io = s.Pools[1].Config;
        io.Name = "IO";
        io.IsIo = true;
        io.Threads = "1";
        const auto page = RenderPage(s, "overview", s.Timestamp);
        UNIT_ASSERT_STRING_CONTAINS(page, "Auto-configured");
        UNIT_ASSERT_STRING_CONTAINS(page, "<td>BASIC</td><td>4</td><td>2</td><td>8</td><td>-1</td><td>One</td>");
        UNIT_ASSERT_STRING_CONTAINS(page, "<td>IO</td><td>1</td><td>&mdash;</td>");
        UNIT_ASSERT_STRING_CONTAINS(page, "&lt;cpu&gt;");
        UNIT_ASSERT_STRING_CONTAINS(page, "SysExecutor: 0");
        UNIT_ASSERT(!page.Contains("CPU usage history"));
    }
    Y_UNIT_TEST(PoolWideCountersAreNotSummedAcrossThreads) {
        std::array<ui64, PoolCounterNames.size()> counters = {};
        NActors::TExecutorThreadStats first, second;
        first.ReceivedEvents = 10;
        second.ReceivedEvents = 20;
        first.PoolActorRegistrations = 100;
        second.PoolActorRegistrations = 105;
        first.PoolDestroyedActors = 50;
        second.PoolDestroyedActors = 50;
        first.MailboxPushedOutByTime = 3;
        second.MailboxPushedOutByTime = 4;
        AddPoolCounters(first, &counters);
        AddPoolCounters(second, &counters);
        UNIT_ASSERT_VALUES_EQUAL(counters[0], 30);
        UNIT_ASSERT_VALUES_EQUAL(counters[4], 105);
        UNIT_ASSERT_VALUES_EQUAL(counters[5], 50);
        UNIT_ASSERT_VALUES_EQUAL(counters[9], 7);
    }

    Y_UNIT_TEST(EventCountersAreExactAndLinkToMetricViewer) {
        TSnapshot snapshot;
        snapshot.Pools.resize(1);
        snapshot.Pools[0].Config.Name = "<System>";
        snapshot.Pools[0].Counters[0] = 9007199254740993ull;
        const auto page = RenderPage(snapshot, "events", TInstant::Now());
        UNIT_ASSERT_STRING_CONTAINS(page, "9007199254740993");
        UNIT_ASSERT_STRING_CONTAINS(page, "&lt;System&gt;");
        UNIT_ASSERT_STRING_CONTAINS(page, "metrics?metric=actor_system.pool.events_received_total");
    }

    Y_UNIT_TEST(PoolAndHarmonizerViews) {
        TSnapshot s;
        s.Timestamp = TInstant::Seconds(100);
        s.Pools.resize(1);
        s.Pools[0].Config.Name = "System";
        s.Pools[0].State.IsStarved = true;
        UNIT_ASSERT_STRING_CONTAINS(RenderPage(s, "pools", s.Timestamp), "Shared CPU quota");
        const auto page = RenderPage(s, "harmonizer", s.Timestamp);
        UNIT_ASSERT_STRING_CONTAINS(page, "Starved");
        UNIT_ASSERT_STRING_CONTAINS(page, "Thread adjustments");
        UNIT_ASSERT(!page.Contains("CPU usage history"));
    }
}
}
