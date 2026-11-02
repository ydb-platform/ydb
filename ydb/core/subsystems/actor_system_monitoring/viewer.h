#pragma once

#include "subsystem.h"
#include "metrics.h"
#include <ydb/library/actors/core/subsystems/stats.h>
#include <library/cpp/time_provider/monotonic.h>

namespace NKikimr::NActorSystemMonitoring {

struct TPoolSnapshot {
    TPoolConfig Config;
    NActors::TExecutorPoolStats Stats;
    NActors::TExecutorPoolState State;
    ui64 CpuUs = 0;
    ui64 Events = 0;
    std::array<ui64, PoolCounterNames.size()> Counters = {};
    i64 Actors = 0;
    double CpuCores = 0;
    double EventsPerSecond = 0;
    bool HasRate = false;
};

struct TSnapshot {
    ui32 NodeId = 0;
    TString SystemParameters;
    bool AutoConfigured = false;
    TInstant Timestamp;
    TMonotonic Monotonic;
    double CollectionUs = 0;
    NActors::THarmonizerStats Harmonizer;
    std::vector<TPoolSnapshot> Pools;
};

void CalculateRates(const TSnapshot& previous, TSnapshot* current);
TString RenderPage(const TSnapshot& snapshot, TStringBuf tab, TInstant now);

} // namespace NKikimr::NActorSystemMonitoring
