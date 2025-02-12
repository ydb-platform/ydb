#include "schemeshard_data_erasure_scheduler.h"
#include "schemeshard.h"

namespace NKikimr::NSchemeShard {

TDataErasureScheduler::TDataErasureScheduler(const NActors::TActorId& schemeShardId, const TDuration& dataErasureInterval)
    : SchemeShardId(schemeShardId)
    , DataErasureInterval(dataErasureInterval)
    , DataErasureInFlight(false)
    , DataErasureWakeupScheduled(false)
    , CurrentWakeupInterval(DataErasureInterval)
{}

void TDataErasureScheduler::Handle(TEvSchemeShard::TEvCompleteDataErasurePtr& ev, const NActors::TActorContext& ctx) {
    Y_UNUSED(ev);
    DataErasureInFlight = false;
    FinishTime = AppData(ctx)->TimeProvider->Now();
    TDuration dataErasureDuration = FinishTime - StartTime;
    if (dataErasureDuration > DataErasureInterval) {
        StartDataErasure(ctx);
    } else {
        CurrentWakeupInterval = DataErasureInterval - dataErasureDuration;
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
    DataErasureInFlight = true;
    StartTime = AppData(ctx)->TimeProvider->Now();
    ctx.Send(SchemeShardId, new TEvSchemeShard::TEvRunDataErasure(Generation, StartTime));
}

void TDataErasureScheduler::ContinueDataErasure(const NActors::TActorContext& ctx) {
    ctx.Send(SchemeShardId, new TEvSchemeShard::TEvRunDataErasure(Generation, StartTime));
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

void TDataErasureScheduler::Restore(const TRestoreValues& restoreValues, const NActors::TActorContext& ctx) {
    IsInitialized = restoreValues.IsInitialized;
    Generation = restoreValues.Generation;
    DataErasureInFlight = restoreValues.DataErasureInFlight;
    StartTime = restoreValues.StartTime;
    if (!DataErasureInFlight) {
        TDuration interval = AppData(ctx)->TimeProvider->Now() - StartTime;
        if (interval > DataErasureInterval) {
            CurrentWakeupInterval = TDuration::Zero();
        } else {
            CurrentWakeupInterval = DataErasureInterval - interval;
        }
    }
}

} // NKikimr::NSchemeShard
