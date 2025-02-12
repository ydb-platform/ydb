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
        bool IsInitialized = false;
        ui64 Generation = 0;
        bool DataErasureInFlight = false;
        TInstant StartTime;
    };

public:
    TDataErasureScheduler(const NActors::TActorId& schemeShardId, const TDuration& dataErasureInterval);

    void Handle(TEvSchemeShard::TEvWakeupToRunDataErasurePtr& ev, const NActors::TActorContext& ctx);
    void ScheduleDataErasureWakeup(const NActors::TActorContext& ctx);
    void StartDataErasure(const NActors::TActorContext& ctx);
    void ContinueDataErasure(const NActors::TActorContext& ctx);
    void Handle(TEvSchemeShard::TEvCompleteDataErasurePtr& ev, const NActors::TActorContext& ctx);

    bool IsDataErasureInFlight() const;
    ui64 GetGeneration() const;
    bool NeedInitialize() const;

    void Restore(const TRestoreValues& restoreValues, const NActors::TActorContext& ctx);

private:
    const NActors::TActorId SchemeShardId;
    const TDuration DataErasureInterval;

    bool DataErasureInFlight;
    TInstant StartTime;
    TInstant FinishTime;
    bool DataErasureWakeupScheduled;
    ui64 Generation = 0;
    TDuration CurrentWakeupInterval;
    bool IsInitialized = false;
};

} // NKikimr::NSchemeShard
