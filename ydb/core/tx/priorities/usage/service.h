#pragma once
#include "config.h"
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/actor.h>
#include <ydb/core/tx/priorities/service/service.h>
#include <ydb/core/tx/priorities/usage/events.h>

namespace NKikimr::NPrioritiesQueue {

template <class TQueuePolicy>
class TServiceOperatorImpl {
private:
    using TSelf = TServiceOperatorImpl<TQueuePolicy>;
    std::atomic<bool> IsEnabledFlag = false;
    static void Register(const TConfig& serviceConfig) {
        Singleton<TSelf>()->IsEnabledFlag = serviceConfig.IsEnabled();
    }
    static const TString& GetQueueName() {
        Y_ABORT_UNLESS(TQueuePolicy::Name.size() == 4);
        return TQueuePolicy::Name;
    }
public:
    [[nodiscard]] static ui64 RegisterClient() {
        static TAtomicCounter Counter = 0;
        const ui64 id = Counter.Inc();
        if (TSelf::IsEnabled()) {
            auto& context = NActors::TActorContext::AsActorContext();
            context.Send(MakeServiceId(), new TEvExecution::TEvRegisterClient(id));
        }
        return id;
    }
    static void UnregisterClient(const ui64 clientId) {
        auto& context = NActors::TActorContext::AsActorContext();
        if (TSelf::IsEnabled()) {
            context.Send(MakeServiceId(), new TEvExecution::TEvUnregisterClient(clientId));
        }
    }
    static void Ask(const ui64 clientId, const ui64 priority, const std::shared_ptr<IRequest>& request, const ui32 count = 1) {
        AFL_VERIFY(request);
        if (TSelf::IsEnabled()) {
            NActors::TActorContext::AsActorContext().Send(MakeServiceId(), new TEvExecution::TEvAsk(clientId, count, request, priority));
        } else {
            request->OnAllocated(std::make_shared<TAllocationGuard>(NActors::TActorId(), clientId, count));
        }
    }
    static void AskMax(const ui64 clientId, const ui64 priority, const std::shared_ptr<IRequest>& request, const ui32 count = 1) {
        AFL_VERIFY(request);
        if (TSelf::IsEnabled()) {
            NActors::TActorContext::AsActorContext().Send(MakeServiceId(), new TEvExecution::TEvAskMax(clientId, count, request, priority));
        } else {
            request->OnAllocated(std::make_shared<TAllocationGuard>(NActors::TActorId(), clientId, count));
        }
    }
    static bool IsEnabled() {
        return Singleton<TSelf>()->IsEnabledFlag;
    }
    static NActors::TActorId MakeServiceId() {
        return NActors::TActorId(NActors::TActorContext::AsActorContext().SelfID.NodeId(), "SrvcPrqe" + GetQueueName());
    }
    static NActors::TActorId MakeServiceId(const ui64 nodeId) {
        return NActors::TActorId(nodeId, "SrvcPrqe" + GetQueueName());
    }
    static NActors::IActor* CreateService(const TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> queueSignals) {
        Register(config);
        return new TDistributor(config, GetQueueName(), queueSignals);
    }

};

class TCompConveyorPolicy {
public:
    static const inline TString Name = "Comp";
};

using TCompServiceOperator = TServiceOperatorImpl<TCompConveyorPolicy>;

}
