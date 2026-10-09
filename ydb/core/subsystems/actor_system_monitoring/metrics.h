#pragma once

#include <ydb/library/actors/metrics/lines/group_line_frontend.h>
#include <ydb/library/actors/core/mon_stats.h>

namespace NKikimr::NActorSystemMonitoring {

struct TPoolMetrics {
    template<class TValue>
    struct TField {
        using TValueType = TValue;
        inline static constexpr std::array<NActors::TLineLabelView, 0> Labels = {};
    };

    struct TCpuCores : TField<double> {
        static constexpr TStringBuf Name = "actor_system.pool.cpu_cores";
    };
    struct TEventsPerSecond : TField<double> {
        static constexpr TStringBuf Name = "actor_system.pool.events_per_second";
    };
    struct TActors : TField<ui64> {
        static constexpr TStringBuf Name = "actor_system.pool.actors";
    };
    struct TThreads : TField<double> {
        static constexpr TStringBuf Name = "actor_system.pool.threads";
    };

    using TFields = std::tuple<TCpuCores, TEventsPerSecond, TActors, TThreads>;
};

using TPoolMetricsFrontend = NActors::TGroupLineFrontend<TPoolMetrics,
    NActors::TCompressedLineStorage<100'000,
        NActors::TDecimalEncoding<2>,
        NActors::TDecimalEncoding<2>,
        NActors::TIntegerEncoding<>,
        NActors::TDecimalEncoding<2>>>;

inline constexpr std::array<TStringBuf, 11> PoolCounterNames = {
    "actor_system.pool.events_received_total",
    "actor_system.pool.events_sent_total",
    "actor_system.pool.events_not_delivered_total",
    "actor_system.pool.events_preempted_total",
    "actor_system.pool.actor_registrations_total",
    "actor_system.pool.actors_destroyed_total",
    "actor_system.pool.cpu_insufficient_total",
    "actor_system.pool.mailbox_yields_tail_send_total",
    "actor_system.pool.mailbox_yields_soft_preemption_total",
    "actor_system.pool.mailbox_yields_time_total",
    "actor_system.pool.mailbox_yields_event_count_total",
};

void AddPoolCounters(const NActors::TExecutorThreadStats& thread,
    std::array<ui64, PoolCounterNames.size()>* counters);

} // namespace NKikimr::NActorSystemMonitoring
