#pragma once
#include "config.h"

#include <ydb/core/tx/conveyor/service/service.h>
#include <ydb/core/tx/conveyor/usage/events.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/actorid.h>

namespace NKikimr::NConveyor {

class TAsyncTaskExecutor: public TActorBootstrapped<TAsyncTaskExecutor> {
private:
    const std::shared_ptr<ITask> Task;

public:
    TAsyncTaskExecutor(const std::shared_ptr<ITask>& task)
        : Task(task) {
    }

    void Bootstrap() {
        auto gAway = PassAwayGuard();
        Task->Execute(nullptr, Task);
    }
};

enum class ESpecialTaskProcesses {
    Insert = 1,
    Compaction = 2,
    Normalizer = 3
};

template <class TConveyorPolicy>
class TServiceOperatorImpl {
private:
    using TSelf = TServiceOperatorImpl<TConveyorPolicy>;
    std::atomic<bool> IsEnabledFlag = false;
    static void Register(const TConfig& serviceConfig) {
        Singleton<TSelf>()->IsEnabledFlag = serviceConfig.IsEnabled();
    }
    static const TString& GetConveyorName() {
        Y_ABORT_UNLESS(TConveyorPolicy::Name.size() == 4);
        return TConveyorPolicy::Name;
    }

public:
    static void AsyncTaskToExecute(const std::shared_ptr<ITask>& task) {
        auto& context = NActors::TActorContext::AsActorContext();
        context.Register(new TAsyncTaskExecutor(task));
    }
    static bool SendTaskToExecute(const std::shared_ptr<ITask>& task, const ESpecialTaskProcesses processType) {
        return SendTaskToExecute(task, (ui64)processType);
    }
    static bool SendTaskToExecute(const std::shared_ptr<ITask>& task, const ui64 processId = 0) {
        if (TSelf::IsEnabled() && NActors::TlsActivationContext) {
            auto& context = NActors::TActorContext::AsActorContext();
            const NActors::TActorId& selfId = context.SelfID;
            context.Send(MakeServiceId(selfId.NodeId()), new NConveyor::TEvExecution::TEvNewTask(task, processId));
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
        return NActors::TActorId(nodeId, "SrvcConv" + GetConveyorName());
    }
    static NActors::IActor* CreateService(const TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals) {
        Register(config);
        return new TDistributor(config, GetConveyorName(), TConveyorPolicy::EnableProcesses, conveyorSignals);
    }
    static TProcessGuard StartProcess(const ui64 externalProcessId, const TCPULimitsConfig& cpuLimits) {
        if (TSelf::IsEnabled() && NActors::TlsActivationContext) {
            auto& context = NActors::TActorContext::AsActorContext();
            const NActors::TActorId& selfId = context.SelfID;
            context.Send(MakeServiceId(selfId.NodeId()), new NConveyor::TEvExecution::TEvRegisterProcess(externalProcessId, cpuLimits));
            return TProcessGuard(externalProcessId, MakeServiceId(selfId.NodeId()));
        } else {
            return TProcessGuard(externalProcessId, {});
        }
    }
};

class TScanConveyorPolicy {
public:
    static const inline TString Name = "Scan";
    static constexpr bool EnableProcesses = true;
};

using TScanServiceOperator = TServiceOperatorImpl<TScanConveyorPolicy>;

}   // namespace NKikimr::NConveyor
