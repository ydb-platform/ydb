#include "schemeshard_data_erasure_scheduler.h"
#include "schemeshard.h"

namespace NKikimr::NSchemeShard {

TDataErasureScheduler::TDataErasureScheduler(const NActors::TActorId& schemeShardId, const TDuration& dataErasureInterval, const TDuration& dataErasureBSCInterval)
    : SchemeShardId(schemeShardId)
    , DataErasureInterval(dataErasureInterval)
    , DataErasureBSCInterval(dataErasureBSCInterval)
    , DataErasureWakeupScheduled(false)
    , CurrentWakeupInterval(DataErasureInterval)
{}

void TDataErasureScheduler::CompleteDataErasure(const NActors::TActorContext& ctx) {
    Status = EStatus::COMPLETED;
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
    if (Status == EStatus::IN_PROGRESS_TENANT || Status == EStatus::IN_PROGRESS_BSC) {
        return;
    }
    Generation++;
    Status = EStatus::IN_PROGRESS_TENANT;
    StartTime = AppData(ctx)->TimeProvider->Now();
    ctx.Send(SchemeShardId, new TEvSchemeShard::TEvRunDataErasure(Generation, StartTime));
}

void TDataErasureScheduler::ContinueDataErasure(const NActors::TActorContext& ctx) {
    if (Status == EStatus::IN_PROGRESS_TENANT) {
        ctx.Send(SchemeShardId, new TEvSchemeShard::TEvRunDataErasure(Generation, StartTime));
    } else if (Status == EStatus::IN_PROGRESS_BSC) {
        // do request to BSC
    }
}

void TDataErasureScheduler::ScheduleDataErasureWakeup(const NActors::TActorContext& ctx) {
    if (DataErasureWakeupScheduled) {
        return;
    }

    ctx.Schedule(CurrentWakeupInterval, new TEvSchemeShard::TEvWakeupToRunDataErasure);
    DataErasureWakeupScheduled = true;
}

TDataErasureScheduler::EStatus TDataErasureScheduler::GetStatus() const {
    return Status;
}

ui64 TDataErasureScheduler::GetGeneration() const {
    return Generation;
}

bool TDataErasureScheduler::NeedInitialize() const {
    return !IsInitialized;
}

void TDataErasureScheduler::SetStatus(const EStatus& status) {
    Status = status;
}

TDuration TDataErasureScheduler::GetDataErasureBSCInterval() const {
    return DataErasureBSCInterval;
}

void TDataErasureScheduler::Restore(const TRestoreValues& restoreValues, const NActors::TActorContext& ctx) {
    IsInitialized = restoreValues.IsInitialized;
    Generation = restoreValues.Generation;
    Status = restoreValues.Status;
    StartTime = restoreValues.StartTime;
    if (Status == EStatus::UNSPECIFIED || Status == EStatus::COMPLETED) {
        TDuration interval = AppData(ctx)->TimeProvider->Now() - StartTime;
        if (interval > DataErasureInterval) {
            CurrentWakeupInterval = TDuration::Zero();
        } else {
            CurrentWakeupInterval = DataErasureInterval - interval;
        }
    }
}

} // NKikimr::NSchemeShard
