#pragma once
#include "abstract.h"

#include <ydb/core/base/events.h>

#include <ydb/library/accessor/accessor.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>

namespace NKikimr::NOlap::NGroupedMemoryManager::NEvents {
struct TEvExternal {
    enum EEv {
        EvStartAllocationTask = EventSpaceBegin(TKikimrEvents::ES_GROUPED_ALLOCATIONS_MANAGER),
        EvFinishAllocationTask,
        EvStartAllocationGroup,
        EvFinishAllocationGroup,
        EvUpdateAllocationTask,
        EvEnd
    };

    class TEvStartTask: public NActors::TEventLocal<TEvStartTask, EvStartAllocationTask> {
    private:
        YDB_READONLY_DEF(std::vector<std::shared_ptr<IAllocation>>, Allocations);
        YDB_READONLY_DEF(std::shared_ptr<TStageFeatures>, StageFeatures);
        YDB_READONLY(ui64, ExternalGroupId, 0);

    public:
        explicit TEvStartTask(const std::vector<std::shared_ptr<IAllocation>>& allocations, const std::shared_ptr<TStageFeatures>& stageFeatures,
            const ui64 externalGroupId)
            : Allocations(allocations)
            , StageFeatures(stageFeatures)
            , ExternalGroupId(externalGroupId) {
            AFL_VERIFY(Allocations.size());
            AFL_VERIFY(StageFeatures);
        }
    };

    class TEvFinishTask: public NActors::TEventLocal<TEvFinishTask, EvFinishAllocationTask> {
    private:
        YDB_READONLY(ui64, AllocationId, 0);

    public:
        explicit TEvFinishTask(const ui64 allocationId)
            : AllocationId(allocationId) {
        }
    };

    class TEvUpdateTask: public NActors::TEventLocal<TEvUpdateTask, EvUpdateAllocationTask> {
    private:
        YDB_READONLY(ui64, AllocationId, 0);
        YDB_READONLY(ui64, Volume, 0);

    public:
        explicit TEvUpdateTask(const ui64 allocationId, const ui64 volume)
            : AllocationId(allocationId)
            , Volume(volume)
        {
        }
    };

    class TEvFinishGroup: public NActors::TEventLocal<TEvFinishGroup, EvFinishAllocationGroup> {
    private:
        YDB_READONLY(ui64, ExternalGroupId, 0);

    public:
        explicit TEvFinishGroup(const ui64 externalGroupId)
            : ExternalGroupId(externalGroupId) {
        }
    };

    class TEvStartGroup: public NActors::TEventLocal<TEvStartGroup, EvStartAllocationGroup> {
    private:
        YDB_READONLY(ui64, ExternalGroupId, 0);

    public:
        explicit TEvStartGroup(const ui64 externalGroupId)
            : ExternalGroupId(externalGroupId) {
        }
    };
};
}   // namespace NKikimr::NOlap::NGroupedMemoryManager::NEvents
