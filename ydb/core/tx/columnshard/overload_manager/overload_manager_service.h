#pragma once

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/accessor/positive_integer.h>

#include <atomic>
#include <memory>

namespace NKikimr::NColumnShard::NOverload {

enum class EResourcesStatus {
    Ok,
    WritesInFlyLimitReached,
    WritesSizeInFlyLimitReached
};

class TOverloadManagerServiceOperator {
private:
    using TSelf = TOverloadManagerServiceOperator;

    static TPositiveControlInteger WritesInFlight;
    static TPositiveControlInteger WritesSizeInFlight;
    static std::atomic<EResourcesStatus> ResourcesStatus;
    static inline const double WritesInFlightSoftLimitCoefficient = 0.9;
    static inline const double WritesInFlightSizeSoftLimitCoefficient = 0.9;

public:
    static NActors::TActorId MakeServiceId();
    static std::unique_ptr<NActors::IActor> CreateService(TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup);

    static ui64 GetShardWritesInFlyLimit();
    static ui64 GetShardWritesSizeInFlyLimit();

    static ui64 GetShardWritesInFly() {
        return WritesInFlight.Val();
    };
    static ui64 GetShardWritesSizeInFly() {
        return WritesSizeInFlight.Val();
    }

    static void NotifyIfResourcesAvailable(bool force);

    static EResourcesStatus RequestResources(ui64 writesCount, ui64 writesSize);
    static void ReleaseResources(ui64 writesCount, ui64 writesSize);
};

} // namespace NKikimr::NColumnShard::NOverload
