#include "schemeshard_data_erasure_scheduler.h"
#include "schemeshard.h"

namespace NKikimr::NSchemeShard {

TDataErasureScheduler::TDataErasureScheduler(const NActors::TActorId& schemeShardId)
    : SchemeShardId(schemeShardId)
    , DataErasureInFlight(false)
    , DataErasureWakeupScheduled(false)
    , CurrentWakeupInterval(DATA_ERASURE_INTERVAL)
{}

void TDataErasureScheduler::Handle(TEvSchemeShard::TEvCompleteDataErasurePtr& ev, const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    DataErasureInFlight = false;
    FinishTime = AppData(ctx)->TimeProvider->Now();
    TDuration dataErasureDuration = FinishTime - StartTime;
    if (dataErasureDuration > DATA_ERASURE_INTERVAL) {
        StartDataErasure(ctx);
    } else {
        CurrentWakeupInterval = DATA_ERASURE_INTERVAL - dataErasureDuration;
        ScheduleDataErasureWakeup(ctx);
    }
}

void TDataErasureScheduler::Handle(TEvSchemeShard::TEvWakeupToRunDataErasurePtr& ev, const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    DataErasureWakeupScheduled = false;
    StartDataErasure(ctx);
}

void TDataErasureScheduler::StartDataErasure(const NActors::TActorContext& ctx) {
    if (DataErasureInFlight) {
        return;
    }
    Generation++;
    ctx.Send(SchemeShardId, new TEvSchemeShard::TEvRunDataErasure(Generation));
    DataErasureInFlight = true;
    StartTime = AppData(ctx)->TimeProvider->Now();
}

void TDataErasureScheduler::ContinueDataErasure(const NActors::TActorContext& ctx) {
    ctx.Send(SchemeShardId, new TEvSchemeShard::TEvRunDataErasure(Generation));
    DataErasureInFlight = true;
}

void TDataErasureScheduler::ScheduleDataErasureWakeup(const NActors::TActorContext& ctx) {
    if (DataErasureWakeupScheduled) {
        return;
    }

    ctx.Schedule(CurrentWakeupInterval, new TEvSchemeShard::TEvWakeupToRunDataErasure);
    DataErasureWakeupScheduled = true;
}

bool TDataErasureScheduler::IsDataErasureInFlight() const {
    return DataErasureInFlight;
}

ui64 TDataErasureScheduler::GetGeneration() const {
    return Generation;
}

bool TDataErasureScheduler::NeedInitialize() const {
    return !IsInitialized;
}

void TDataErasureScheduler::Restore(const TRestoreValues& restoreValues) {
    IsInitialized = restoreValues.IsInitialized;
    Generation = restoreValues.Generation;
    DataErasureInFlight = restoreValues.DataErasureInFlight;
    if (!DataErasureInFlight) {
        if (restoreValues.DataErasureDuration > DATA_ERASURE_INTERVAL) {
            CurrentWakeupInterval = TDuration::Zero();
        } else {
            CurrentWakeupInterval = DATA_ERASURE_INTERVAL - restoreValues.DataErasureDuration;
        }
    }
}

} // NKikimr::NSchemeShard
