#pragma once
#include "common.h"
#include "config.h"

#include <ydb/core/tx/conveyor_composite/usage/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NConveyorComposite {

class TServiceOperator {
private:
    using TSelf = TServiceOperator;
    std::atomic<bool> IsEnabledFlag = false;

public:
    static void Register(const NConfig::TConfig& serviceConfig) {
        Singleton<TSelf>()->IsEnabledFlag = serviceConfig.IsEnabled();
    }

public:
    static bool SendTaskToExecute(const std::shared_ptr<ITask>& task, const ESpecialTaskCategory category, const ui64 internalProcessId,
        const bool useBatchPool = false) {
        if (TSelf::IsEnabled() && NActors::TlsActivationContext) {
            auto& context = NActors::TActorContext::AsActorContext();
            const NActors::TActorId& selfId = context.SelfID;
            context.Send(MakeServiceId(selfId.NodeId(), useBatchPool),
                new NConveyorComposite::TEvExecution::TEvNewTask(task, category, internalProcessId));
            return true;
        } else {
            task->Execute(nullptr, task);
            return false;
        }
    }
    static bool IsEnabled() {
        return Singleton<TSelf>()->IsEnabledFlag;
    }
    static NActors::TActorId MakeServiceId(const ui32 nodeId, const bool useBatchPool = false) {
        static constexpr auto kUserServiceName = "ConvCmpUser";
        static constexpr auto kBatchServiceName = "ConvCmpBatch";

        return NActors::TActorId(nodeId, useBatchPool ? kBatchServiceName : kUserServiceName);
    }
    static TProcessGuard StartProcess(
        const ESpecialTaskCategory category, const TString& scopeId, const ui64 externalProcessId, const TCPULimitsConfig& cpuLimits,
        const bool useBatchPool = false) {
        if (TSelf::IsEnabled() && NActors::TlsActivationContext) {
            auto& context = NActors::TActorContext::AsActorContext();
            const NActors::TActorId& selfId = context.SelfID;
            return TProcessGuard(category, scopeId, externalProcessId, cpuLimits, MakeServiceId(selfId.NodeId(), useBatchPool));
        } else {
            return TProcessGuard(category, scopeId, externalProcessId, cpuLimits, {});
        }
    }
};

class TInsertServiceOperator {
public:
    static bool SendTaskToExecute(const std::shared_ptr<ITask>& task) {
        return TServiceOperator::SendTaskToExecute(task, ESpecialTaskCategory::Insert, 0);
    }
};

class TNormalizerServiceOperator {
public:
    static bool SendTaskToExecute(const std::shared_ptr<ITask>& task) {
        return TServiceOperator::SendTaskToExecute(task, ESpecialTaskCategory::Normalizer, 0);
    }
};

class TCompServiceOperator {
public:
    static bool SendTaskToExecute(const std::shared_ptr<ITask>& task) {
        return TServiceOperator::SendTaskToExecute(task, ESpecialTaskCategory::Compaction, 0);
    }
};

class TScanServiceOperator {
public:
    static bool SendTaskToExecute(
        const std::shared_ptr<ITask>& task, const ui64 internalProcessId, const bool useBatchPool = false) {
        return TServiceOperator::SendTaskToExecute(task, ESpecialTaskCategory::Scan, internalProcessId, useBatchPool);
    }

    static TProcessGuard StartProcess(
        const ui64 externalProcessId, const TString& scopeId, const TCPULimitsConfig& cpuLimits, const bool useBatchPool = false) {
        return TServiceOperator::StartProcess(ESpecialTaskCategory::Scan, scopeId, externalProcessId, cpuLimits, useBatchPool);
    }
};

class TDeduplicationServiceOperator {
public:
    static bool SendTaskToExecute(const std::shared_ptr<ITask>& task) {
        return TServiceOperator::SendTaskToExecute(task, ESpecialTaskCategory::Deduplication, 0);
    }
};

}   // namespace NKikimr::NConveyorComposite
