#include "subsystem.h"
#include "viewer.h"
#include "metrics.h"
#include <ydb/library/actors/metrics/lines/dynamic_group_line.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/subsystems/inmemory_metrics.h>
#include <atomic>
#include <ydb/library/actors/core/hfunc.h>
#include <util/string/cast.h>

namespace NKikimr::NActorSystemMonitoring {

void AddPoolCounters(const NActors::TExecutorThreadStats& thread,
        std::array<ui64, PoolCounterNames.size()>* counters) {
    (*counters)[0] += thread.ReceivedEvents;
    (*counters)[1] += thread.SentEvents;
    (*counters)[2] += thread.NonDeliveredEvents;
    (*counters)[3] += thread.PreemptedEvents;
    (*counters)[4] = std::max((*counters)[4], thread.PoolActorRegistrations);
    (*counters)[5] = std::max((*counters)[5], thread.PoolDestroyedActors);
    (*counters)[6] += thread.NotEnoughCpuExecutions;
    (*counters)[7] += thread.MailboxPushedOutByTailSending;
    (*counters)[8] += thread.MailboxPushedOutBySoftPreemption;
    (*counters)[9] += thread.MailboxPushedOutByTime;
    (*counters)[10] += thread.MailboxPushedOutByEventCount;

}

namespace {
using namespace NActors;

class TCollector : public TActorBootstrapped<TCollector> {
    const TConfig Config;
    const std::shared_ptr<std::atomic<bool>> Stopping;
    TSnapshot Snapshot;
    TDynamicGroupLine MetricLine;
    TDynamicGroupLine CounterLine;

    void Collect() {
        if (Stopping->load(std::memory_order_acquire)) {
            return;
        }
        const auto begin = TMonotonic::Now();
        TSnapshot next;
        next.NodeId = SelfId().NodeId();
        next.SystemParameters = Config.SystemParameters;
        next.AutoConfigured = Config.AutoConfigured;
        next.Timestamp = TInstant::Now();
        next.Monotonic = begin;
        const auto& source = GetActorSystemStats();
        source.GetHarmonizerStats(next.Harmonizer);
        for (ui32 id = 0; id < Config.Pools.size(); ++id) {
            auto& pool = next.Pools.emplace_back();
            pool.Config = Config.Pools[id];
            TVector<TExecutorThreadStats> threads, shared;
            source.GetPoolStats(id, pool.Stats, threads, shared);
            if (!pool.Config.IsIo) {
                source.GetExecutorPoolState(id, pool.State);
            }
            const auto add = [&pool](const auto& stats) {
                for (const auto& thread : stats) {
                    pool.CpuUs += thread.CpuUs;
                    pool.Events += thread.ReceivedEvents;
                    AddPoolCounters(thread, &pool.Counters);
                    for (const auto count : thread.ActorsAliveByActivity) {
                        pool.Actors += count;
                    }
                }
            };
            add(threads);
            add(shared);
        }
        CalculateRates(Snapshot, &next);
        if (MetricLine) {
            std::array<TLineNumericValue, TDynamicGroupSchema::MaxFields> values;
            size_t count = 0;
            bool complete = !next.Pools.empty();
            for (const auto& pool : next.Pools) {
                complete &= pool.HasRate;
                values[count++] = pool.CpuCores;
                values[count++] = pool.EventsPerSecond;
                values[count++] = ui64(std::max<i64>(0, pool.Actors));
                values[count++] = double(pool.Stats.CurrentThreadCount);
            }
            if (complete) MetricLine.Append({values.data(), count});
        }
        if (CounterLine) {
            std::array<TLineNumericValue, TDynamicGroupSchema::MaxFields> values;
            size_t count = 0;
            for (const auto& pool : next.Pools) {
                for (ui64 value : pool.Counters) {
                    values[count++] = value;
                }
            }
            CounterLine.Append({values.data(), count});
        }
        next.CollectionUs = (TMonotonic::Now() - begin).MicroSeconds();
        Snapshot = std::move(next);
        Schedule(Config.SamplePeriod, new TEvents::TEvWakeup());
    }

    void Handle(NMon::TEvHttpInfo::TPtr ev) {
        const auto tab = ev->Get()->Request.GetParams().Get("tab");
        Send(ev->Sender, new NMon::TEvHttpInfoRes(RenderPage(Snapshot, tab, TInstant::Now()),
            ev->Get()->SubRequestId), 0, ev->Cookie);
    }

public:
    TCollector(TConfig config, std::shared_ptr<std::atomic<bool>> stopping)
        : Config(std::move(config)), Stopping(std::move(stopping)) {}

    void Bootstrap() {
        Become(&TThis::StateWork);
        if (auto* metrics = GetMetricSystem(); metrics && Config.Pools.size() <= TDynamicGroupSchema::MaxFields / 4) {
            TVector<TDynamicGroupField> fields;
            for (size_t i = 0; i < Config.Pools.size(); ++i) {
                const TVector<TLabel> labels = {{"pool_id", ToString(i)}, {"pool", Config.Pools[i].Name}};
                fields.push_back({TString(TPoolMetrics::TCpuCores::Name), labels, EGroupValueType::Decimal});
                fields.push_back({TString(TPoolMetrics::TEventsPerSecond::Name), labels, EGroupValueType::Decimal});
                fields.push_back({TString(TPoolMetrics::TActors::Name), labels, EGroupValueType::Unsigned});
                fields.push_back({TString(TPoolMetrics::TThreads::Name), labels, EGroupValueType::Decimal});
            }
            if (!fields.empty()) MetricLine = TDynamicGroupLine::Create(metrics, "actor_system.pools", std::move(fields));
        }
        if (auto* metrics = GetMetricSystem(); metrics && !Config.Pools.empty()
                && Config.Pools.size() <= TDynamicGroupSchema::MaxFields / PoolCounterNames.size()) {
            TVector<TDynamicGroupField> fields;
            for (size_t i = 0; i < Config.Pools.size(); ++i) {
                const TVector<TLabel> labels = {{"pool_id", ToString(i)}, {"pool", Config.Pools[i].Name}};
                for (TStringBuf name : PoolCounterNames) {
                    fields.push_back({TString(name), labels, EGroupValueType::Unsigned});
                }
            }
            CounterLine = TDynamicGroupLine::Create(metrics, "actor_system.pools.counters", std::move(fields));
        }
        Collect();
    }

    STRICT_STFUNC(StateWork,
        cFunc(TEvents::TSystem::Wakeup, Collect)
        cFunc(TEvents::TSystem::Poison, PassAway)
        hFunc(NMon::TEvHttpInfo, Handle)
    )
};

class TMonitoringSubSystem : public TActorSystemMonitoring {
    const TConfig Config;
    std::shared_ptr<std::atomic<bool>> Stopping = std::make_shared<std::atomic<bool>>(false);
    TActorId Collector;
public:
    explicit TMonitoringSubSystem(TConfig config) : Config(std::move(config)) {
        Y_ABORT_UNLESS(Config.SamplePeriod >= TDuration::Seconds(1));
    }

    TSubSystemDependencies GetDependencies() const override {
        return DependsOn<TActorSystemStatsSubSystem>() && DependsOn<TInMemoryMetricsRegistry>();
    }

    void OnAfterStart(TActorSystem& system) override {
        Collector = system.Register(new TCollector(Config, Stopping), TMailboxType::ReadAsFilled, Config.ExecutorPool);
        if (Config.RegisterPage) {
            Config.RegisterPage(system, Collector);
        }
    }

    void OnBeforeStop(TActorSystem& system) override {
        Stopping->store(true, std::memory_order_release);
        system.Send(new IEventHandle(Collector, TActorId(), new TEvents::TEvPoison()));
    }
};
} // namespace

std::unique_ptr<TActorSystemMonitoring> MakeActorSystemMonitoring(TConfig config) {
    return std::make_unique<TMonitoringSubSystem>(std::move(config));
}
} // namespace NKikimr::NActorSystemMonitoring
