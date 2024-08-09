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
        EvStartAllocationProcess,
        EvFinishAllocationProcess,
        EvEnd
    };

    class TEvStartTask: public NActors::TEventLocal<TEvStartTask, EvStartAllocationTask> {
    private:
        YDB_READONLY_DEF(std::vector<std::shared_ptr<IAllocation>>, Allocations);
        YDB_READONLY_DEF(std::shared_ptr<TStageFeatures>, StageFeatures);
        YDB_READONLY(ui64, ExternalProcessId, 0);
        YDB_READONLY(ui64, ExternalGroupId, 0);

    public:
        explicit TEvStartTask(const ui64 externalProcessId,
            const ui64 externalGroupId, const std::vector<std::shared_ptr<IAllocation>>& allocations,
            const std::shared_ptr<TStageFeatures>& stageFeatures)
            : Allocations(allocations)
            , StageFeatures(stageFeatures)
            , ExternalProcessId(externalProcessId)
            , ExternalGroupId(externalGroupId) {
            AFL_VERIFY(Allocations.size());
            AFL_VERIFY(StageFeatures);
        }
    };

    class TEvFinishTask: public NActors::TEventLocal<TEvFinishTask, EvFinishAllocationTask> {
    private:
        YDB_READONLY(ui64, ExternalProcessId, 0);
        YDB_READONLY(ui64, AllocationId, 0);

    public:
        explicit TEvFinishTask(const ui64 externalProcessId, const ui64 allocationId)
            : ExternalProcessId(externalProcessId)
            , AllocationId(allocationId) {
        }
    };

    class TEvUpdateTask: public NActors::TEventLocal<TEvUpdateTask, EvUpdateAllocationTask> {
    private:
        YDB_READONLY(ui64, ExternalProcessId, 0);
        YDB_READONLY(ui64, AllocationId, 0);
        YDB_READONLY(ui64, Volume, 0);

    public:
        explicit TEvUpdateTask(const ui64 externalProcessId, const ui64 allocationId, const ui64 volume)
            : ExternalProcessId(externalProcessId)
            , AllocationId(allocationId)
            , Volume(volume) {
        }
    };

    class TEvFinishGroup: public NActors::TEventLocal<TEvFinishGroup, EvFinishAllocationGroup> {
    private:
        YDB_READONLY(ui64, ExternalProcessId, 0);
        YDB_READONLY(ui64, ExternalGroupId, 0);

    public:
        explicit TEvFinishGroup(const ui64 externalProcessId, const ui64 externalGroupId)
            : ExternalProcessId(externalProcessId)
            , ExternalGroupId(externalGroupId) {
        }
    };

    class TEvStartGroup: public NActors::TEventLocal<TEvStartGroup, EvStartAllocationGroup> {
    private:
        YDB_READONLY(ui64, ExternalProcessId, 0);
        YDB_READONLY(ui64, ExternalGroupId, 0);

    public:
        explicit TEvStartGroup(const ui64 externalProcessId, const ui64 externalGroupId)
            : ExternalProcessId(externalProcessId)
            , ExternalGroupId(externalGroupId) {
        }
    };

    class TEvFinishProcess: public NActors::TEventLocal<TEvFinishProcess, EvFinishAllocationProcess> {
    private:
        YDB_READONLY(ui64, ExternalProcessId, 0);

    public:
        explicit TEvFinishProcess(const ui64 externalProcessId)
            : ExternalProcessId(externalProcessId) {
        }
    };

    class TEvStartProcess: public NActors::TEventLocal<TEvStartProcess, EvStartAllocationProcess> {
    private:
        YDB_READONLY(ui64, ExternalProcessId, 0);

    public:
        explicit TEvStartProcess(const ui64 externalProcessId)
            : ExternalProcessId(externalProcessId) {
        }
    };
};
}   // namespace NKikimr::NOlap::NGroupedMemoryManager::NEvents
