#pragma once

#include <util/datetime/base.h>

#include <ydb/library/actors/core/event.h>

namespace NActors {
    struct TActorContext;
}

namespace NKikimr::NSchemeShard {

namespace TEvSchemeShard {
    struct TEvWakeupToRunDataErasure;
    using TEvWakeupToRunDataErasurePtr = TAutoPtr<NActors::TEventHandle<TEvWakeupToRunDataErasure>>;

    struct TEvCompleteDataErasure;
    using TEvCompleteDataErasurePtr = TAutoPtr<NActors::TEventHandle<TEvCompleteDataErasure>>;
}

class TSchemeShard;

class TDataErasureScheduler {
public:
    struct TRestoreValues {
        bool IsInitialized = true;
        ui64 Generation = 0;
        bool DataErasureInFlight = false;
        TDuration DataErasureDuration;
    };

private:
    static constexpr TDuration DATA_ERASURE_INTERVAL = TDuration::Days(7);

public:
    TDataErasureScheduler(const NActors::TActorId& schemeShardId);

    void Handle(TEvSchemeShard::TEvWakeupToRunDataErasurePtr& ev, const NActors::TActorContext& ctx);
    void ScheduleDataErasureWakeup(const NActors::TActorContext& ctx);
    void StartDataErasure(const NActors::TActorContext& ctx);
    void ContinueDataErasure(const NActors::TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvCompleteDataErasurePtr& ev, const NActors::TActorContext& ctx);

    bool IsDataErasureInFlight() const;
    ui64 GetGeneration() const;
    bool NeedInitialize() const;

    void Restore(const TRestoreValues& restoreValues);

private:
    const NActors::TActorId SchemeShardId;

    bool DataErasureInFlight;
    TInstant StartTime;
    TInstant FinishTime;
    bool DataErasureWakeupScheduled;
    ui64 Generation = 0;
    TDuration CurrentWakeupInterval;
    bool IsInitialized = true;
};

} // NKikimr::NSchemeShard
