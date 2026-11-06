#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/testlib/test_runtime.h>

#include <library/cpp/monlib/metrics/metric_registry.h>
#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {
    struct TCollectorRuntime {
        TVector<TDuration> Delays;
        TTestActorRuntimeBase Runtime;
        TActorId Collector;

        TCollectorRuntime() {
            Runtime.Initialize();
            Runtime.SetScheduledEventFilter([this](auto&, auto& event, TDuration delay, auto&) {
                UNIT_ASSERT(event->GetTypeRewrite() == TEvents::TSystem::Wakeup);
                Delays.push_back(delay);
                return true;
            });
        }

        void Start(IActor* actor) {
            Collector = Runtime.Register(actor);
            TDispatchOptions options;
            options.CustomFinalCondition = [this] { return Delays.size() == 1; };
            Runtime.DispatchEvents(options);
            UNIT_ASSERT_VALUES_EQUAL(Delays.size(), 1);
        }

        void Wakeup() {
            Runtime.Send(new IEventHandle(Collector, TActorId(), new TEvents::TEvWakeup()));
        }
    };

    const TStringBuf GaugeNames[] = {
        "process.VmSize", "process.AnonRssSize", "process.FileRssSize",
        "process.CGroupMemLimit", "process.UptimeSeconds", "process.NumThreads",
        "system.UptimeSeconds",
    };
    const TStringBuf RateNames[] = {
        "process.UserTime", "process.SystemTime", "process.MinorPageFaults", "process.MajorPageFaults",
    };

    void SeedRegistry(NMonitoring::TMetricRegistry& registry) {
        for (auto name : GaugeNames) {
            registry.IntGauge({{"sensor", TString(name)}})->Set(-1);
        }
        for (auto name : RateNames) {
            auto* rate = registry.Rate({{"sensor", TString(name)}});
            rate->Reset();
            rate->Add(1ULL << 63);
        }
    }

    void CheckRegistry(NMonitoring::TMetricRegistry& registry) {
        for (auto name : GaugeNames) {
            UNIT_ASSERT_C(registry.IntGauge({{"sensor", TString(name)}})->Get() >= 0, name);
        }
        for (auto name : RateNames) {
            UNIT_ASSERT_C(registry.Rate({{"sensor", TString(name)}})->Get() < (1ULL << 63), name);
        }
        UNIT_ASSERT(registry.IntGauge({{"sensor", "process.VmSize"}})->Get() > 0);
        UNIT_ASSERT(registry.IntGauge({{"sensor", "process.NumThreads"}})->Get() > 0);
        UNIT_ASSERT(registry.Rate({{"sensor", "process.MinorPageFaults"}})->Get() > 0);
    }
}

Y_UNIT_TEST_SUITE(TProcStat) {
    Y_UNIT_TEST(Fill) {
        TProcStat stat;
#ifdef _linux_
        UNIT_ASSERT(stat.Fill(getpid()));
        UNIT_ASSERT_VALUES_EQUAL(stat.Pid, getpid());
        UNIT_ASSERT_VALUES_EQUAL(stat.Ppid, getppid());
        UNIT_ASSERT(stat.NumThreads >= 1);
        UNIT_ASSERT(stat.Vsize > 0);
        UNIT_ASSERT(stat.Rss > 0);
        UNIT_ASSERT(stat.FileRss + stat.AnonRss > 0);
        UNIT_ASSERT_VALUES_EQUAL((stat.FileRss + stat.AnonRss) % sysconf(_SC_PAGESIZE), 0);
        UNIT_ASSERT(stat.MemTotal > 0);
        UNIT_ASSERT(stat.MemAvailable <= stat.MemTotal);
        UNIT_ASSERT(stat.SystemUptime >= stat.Uptime);
        UNIT_ASSERT(stat.Fill(getpid()));
        UNIT_ASSERT_VALUES_EQUAL(stat.Pid, getpid());
#else
        UNIT_ASSERT(!stat.Fill(getpid()));
#endif
    }

#ifdef _linux_
    Y_UNIT_TEST(MissingProcessResetsProcessSamplesAndStillReadsSystemMemory) {
        TProcStat stat;
        stat.Vsize = stat.Utime = stat.Stime = stat.MinFlt = stat.MajFlt = 1;
        stat.NumThreads = 1;
        stat.FileRss = stat.AnonRss = stat.CGroupMemLim = 1;
        stat.Uptime = stat.SystemUptime = TDuration::Seconds(1);
        UNIT_ASSERT(stat.Fill(-1));
        UNIT_ASSERT_VALUES_EQUAL(stat.Vsize, 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.Utime, 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.Stime, 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.MinFlt, 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.MajFlt, 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.NumThreads, 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.FileRss, 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.AnonRss, 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.CGroupMemLim, 0);
        UNIT_ASSERT_VALUES_EQUAL(stat.Uptime, TDuration::Zero());
        UNIT_ASSERT_VALUES_EQUAL(stat.SystemUptime, TDuration::Zero());
        UNIT_ASSERT(stat.MemTotal > 0);
    }

    Y_UNIT_TEST(DynamicCountersPublishAndRefreshProcessSamples) {
        auto counters = MakeIntrusive<NMonitoring::TDynamicCounters>();
        TCollectorRuntime fixture;
        fixture.Start(CreateProcStatCollector(7, counters));
        auto group = counters->FindSubgroup("counters", "utils");
        UNIT_ASSERT(group);
        UNIT_ASSERT(group->FindCounter("Process/VmSize")->Val() > 0);
        UNIT_ASSERT(group->FindCounter("Process/NumThreads")->Val() > 0);

        const TStringBuf names[] = {
            "Process/VmSize", "Process/AnonRssSize", "Process/FileRssSize",
            "Process/UserTime", "Process/SystemTime", "Process/MinorPageFaults",
            "Process/MajorPageFaults", "Process/UptimeSeconds", "Process/NumThreads",
            "System/UptimeSeconds",
        };
        for (auto name : names) {
            auto counter = group->FindCounter(TString(name));
            UNIT_ASSERT_C(counter, name);
            *counter = Max<i64>();
        }
        fixture.Wakeup();
        for (auto name : names) {
            UNIT_ASSERT_C(group->FindCounter(TString(name))->Val() != Max<i64>(), name);
        }
        UNIT_ASSERT(group->FindCounter("Process/CGroupMemLimit"));
        UNIT_ASSERT_VALUES_EQUAL(fixture.Delays.size(), 2);
        for (auto delay : fixture.Delays) {
            UNIT_ASSERT_VALUES_EQUAL(delay, TDuration::Seconds(7));
        }
    }

    Y_UNIT_TEST(RegistryPublishesAndReplacesSamplesOnWakeup) {
        NMonitoring::TMetricRegistry registry;
        SeedRegistry(registry);
        TCollectorRuntime fixture;
        const auto interval = TDuration::MilliSeconds(125);
        fixture.Start(CreateProcStatCollector(interval, registry));
        CheckRegistry(registry);
        SeedRegistry(registry);
        fixture.Wakeup();
        CheckRegistry(registry);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Delays.size(), 2);
        for (auto delay : fixture.Delays) {
            UNIT_ASSERT_VALUES_EQUAL(delay, interval);
        }
    }

    Y_UNIT_TEST(WeakRegistryUpdatesWhileAliveAndDoesNotKeepRegistryAlive) {
        auto registry = std::make_shared<NMonitoring::TMetricRegistry>();
        std::weak_ptr<NMonitoring::TMetricRegistry> weak = registry;
        SeedRegistry(*registry);
        TCollectorRuntime fixture;
        fixture.Start(CreateProcStatCollector(TDuration::Seconds(3), weak));
        CheckRegistry(*registry);
        SeedRegistry(*registry);
        fixture.Wakeup();
        CheckRegistry(*registry);
        registry.reset();
        UNIT_ASSERT(weak.expired());
        fixture.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Delays.size(), 3);
        UNIT_ASSERT_VALUES_EQUAL(fixture.Delays.back(), TDuration::Seconds(3));
    }

    Y_UNIT_TEST(ExpiredRegistryAtBootstrapStillSchedulesNextSample) {
        TCollectorRuntime fixture;
        fixture.Start(CreateProcStatCollector(TDuration::Seconds(2),
            std::weak_ptr<NMonitoring::TMetricRegistry>()));
        fixture.Wakeup();
        UNIT_ASSERT_VALUES_EQUAL(fixture.Delays.size(), 2);
        for (auto delay : fixture.Delays) {
            UNIT_ASSERT_VALUES_EQUAL(delay, TDuration::Seconds(2));
        }
    }
#endif
}
