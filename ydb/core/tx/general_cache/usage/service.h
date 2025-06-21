#pragma once
#include "config.h"

#include <ydb/core/tx/general_cache/service/service.h>

namespace NKikimr::NGeneralCache {

template <class TPolicy>
class TServiceOperator {
private:
    using TAddress = typename TPolicy::TAddress;
    using EConsumer = typename TPolicy::EConsumer;

    using TSelf = TServiceOperator<TPolicy>;

public:
    static void AskObjects(const EConsumer consumer, THashSet<TAddress>&& addresses, std::shared_ptr<ICallback>&& callback) {
        context.Send(GetCurrentNodeServiceId(), new NPublic::TEvents::TEvAskData(std::move(addresses), std::move(callback)));
    }
    static NActors::TActorId MakeServiceId(const ui32 nodeId) {
        return NActors::TActorId(nodeId, "SrvcCach" + TPolicy::GetServiceCode());
    }
    static NActors::TActorId GetCurrentNodeServiceId() {
        AFL_VERIFY(NActors::TlsActivationContext);
        auto& context = NActors::TActorContext::AsActorContext();
        const NActors::TActorId& selfId = context.SelfID;
        return MakeServiceId(selfId.NodeId());
    }
    static NActors::IActor* CreateService(const NPublic::TConfig& config, TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorSignals) {
        return new TDistributor(config, conveyorSignals);
    }
};

}   // namespace NKikimr::NGeneralCache
