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
}

class TSchemeShard;

class TDataErasureScheduler {
public:
    enum class EStatus : ui32 {
        UNSPECIFIED,
        COMPLETED,
        IN_PROGRESS_TENANT,
        IN_PROGRESS_BSC,
    };

    struct TRestoreValues {
        bool IsInitialized = false;
        ui64 Generation = 0;
        EStatus Status = EStatus::UNSPECIFIED;
        TInstant StartTime;
    };

public:
    TDataErasureScheduler(const NActors::TActorId& schemeShardId, const TDuration& dataErasureInterval, const TDuration& dataErasureBSCInterval);

    void Handle(TEvSchemeShard::TEvWakeupToRunDataErasurePtr& ev, const NActors::TActorContext& ctx);
    void ScheduleDataErasureWakeup(const NActors::TActorContext& ctx);
    void StartDataErasure(const NActors::TActorContext& ctx);
    void ContinueDataErasure(const NActors::TActorContext& ctx);
    void CompleteDataErasure(const NActors::TActorContext& ctx);

    void SetStatus(const EStatus& status);
    EStatus GetStatus() const;
    ui64 GetGeneration() const;
    TDuration GetDataErasureBSCInterval() const;
    bool NeedInitialize() const;

    void Restore(const TRestoreValues& restoreValues, const NActors::TActorContext& ctx);

private:
    const NActors::TActorId SchemeShardId;
    const TDuration DataErasureInterval;
    const TDuration DataErasureBSCInterval;

    EStatus Status = EStatus::UNSPECIFIED;
    TInstant StartTime;
    TInstant FinishTime;
    bool DataErasureWakeupScheduled;
    ui64 Generation = 0;
    TDuration CurrentWakeupInterval;
    bool IsInitialized = false;
};

} // NKikimr::NSchemeShard
