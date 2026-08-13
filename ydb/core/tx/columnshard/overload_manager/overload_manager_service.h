#pragma once

#include <ydb/library/accessor/positive_integer.h>
#include <ydb/library/actors/core/actor.h>

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
    static std::atomic<bool> CompactionOverloaded;
    static inline const double WritesInFlightSoftLimitCoefficient = 0.9;
    static inline const double WritesInFlightSizeSoftLimitCoefficient = 0.9;

    static bool AreWriteResourcesBelowSoftLimit();

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

    static bool IsWriteSideOverloaded() {
        return ResourcesStatus.load() != EResourcesStatus::Ok;
    }

    static bool IsCompactionOverloaded() {
        return CompactionOverloaded.load();
    }

    // Called from OM actor only when the set of compaction-overloaded tablets becomes empty/non-empty.
    static void SetCompactionOverloaded(bool overloaded);

    static void NotifyIfResourcesAvailable(bool force);

    // Ask OM to publish OVERLOADED/READY from current write + compaction state (edge + refresh safe).
    static void SyncNodeOverloadPublication();

    static EResourcesStatus RequestResources(ui64 writesCount, ui64 writesSize);
    static void ReleaseResources(ui64 writesCount, ui64 writesSize);

    // Edge-triggered from ColumnShard write queue: tablet entered/left compaction wait.
    // Returns true when the event was actually handed to the overload manager (feature flag on
    // and an actor system present); callers must only advance sticky local state on success.
    static bool ReportCompactionOverload(ui64 tabletId, bool overloaded);
};

}   // namespace NKikimr::NColumnShard::NOverload
