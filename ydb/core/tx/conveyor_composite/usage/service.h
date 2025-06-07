#pragma once
#include "common.h"
#include "config.h"

#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NConveyorComposite {

class TServiceOperator {
private:
    std::atomic<bool> IsEnabledFlag = false;
    static void Register(const NConfig::TConfig& serviceConfig) {
        Singleton<TSelf>()->IsEnabledFlag = serviceConfig.IsEnabled();
    }

public:
    static bool SendTaskToExecute(const std::shared_ptr<ITask>& task, const ESpecialTaskCategory category, const TString& scopeId, const ui64 processId) {
        if (TSelf::IsEnabled() && NActors::TlsActivationContext) {
            auto& context = NActors::TActorContext::AsActorContext();
            const NActors::TActorId& selfId = context.SelfID;
            context.Send(MakeServiceId(selfId.NodeId()), new NConveyorComposite::TEvExecution::TEvNewTask(task, category, scopeId, processId));
            return true;
        } else {
            task->Execute(nullptr, task);
            return false;
        }
    }
    static bool IsEnabled() {
        return Singleton<TSelf>()->IsEnabledFlag;
    }
    static NActors::TActorId MakeServiceId(const ui32 nodeId) {
        return NActors::TActorId(nodeId, "SrvcConvCmps");
    }
    static NActors::IActor* CreateService(const NConfig::TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals) {
        Register(config);
        return new TDistributor(config, conveyorSignals);
    }
    static TProcessGuard StartProcess(
        const ESpecialTaskCategory category, const TString& scopeId, const ui64 externalProcessId, const TCPULimitsConfig& cpuLimits) {
        if (TSelf::IsEnabled() && NActors::TlsActivationContext) {
            auto& context = NActors::TActorContext::AsActorContext();
            const NActors::TActorId& selfId = context.SelfID;
            context.Send(MakeServiceId(selfId.NodeId()),
                new NConveyorComposite::TEvExecution::TEvRegisterProcess(cpuLimits, category, scopeId, externalProcessId));
            return TProcessGuard(category, scopeId, externalProcessId, MakeServiceId(selfId.NodeId()));
        } else {
            return TProcessGuard(category, scopeId, externalProcessId, {});
        }
    }
};

class TInsertServiceOperator {
public:
    static bool SendTaskToExecute(
        const std::shared_ptr<ITask>& task, const ESpecialTaskCategory category, const TString& scopeId, const ui64 processId) {
        TServiceOperator::SendTaskToExecute(task, ESpecialTaskCategory::Insert, "DEFAULT", 0);
    }
};

class TCompServiceOperator {
public:
    static bool SendTaskToExecute(
        const std::shared_ptr<ITask>& task, const ESpecialTaskCategory category, const TString& scopeId, const ui64 processId) {
        TServiceOperator::SendTaskToExecute(task, ESpecialTaskCategory::Compaction, "DEFAULT", 0);
    }
};

}   // namespace NKikimr::NConveyorComposite
