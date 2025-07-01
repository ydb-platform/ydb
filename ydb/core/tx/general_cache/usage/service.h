#pragma once
#include "abstract.h"
#include "config.h"

#include <ydb/core/tx/general_cache/service/service.h>

namespace NKikimr::NGeneralCache {

template <class TPolicy>
class TServiceOperator {
public:
    using TAddress = typename TPolicy::TAddress;
    using TObject = typename TPolicy::TObject;
    using TSourceId = typename TPolicy::TSourceId;
    using EConsumer = typename TPolicy::EConsumer;
    using ICallback = NPublic::ICallback<TPolicy>;

private:
    using TSelf = TServiceOperator<TPolicy>;

public:
    static void KillSource(const TSourceId sourceId) {
        AFL_VERIFY(NActors::TlsActivationContext);
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(GetCurrentNodeServiceId(), new NPublic::TEvents<TPolicy>::TEvKillSource(sourceId));
    }

    static void AskObjects(const EConsumer consumer, THashSet<TAddress>&& addresses, std::shared_ptr<ICallback>&& callback) {
        AFL_VERIFY(NActors::TlsActivationContext);
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(GetCurrentNodeServiceId(), new NPublic::TEvents<TPolicy>::TEvAskData(consumer, std::move(addresses), std::move(callback)));
    }

    static void UpdateMaxCacheSize(const ui64 maxCacheSize) {
        AFL_VERIFY(NActors::TlsActivationContext);
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(GetCurrentNodeServiceId(), new NPublic::TEvents<TPolicy>::TEvUpdateMaxCacheSize(maxCacheSize));
    }

    static void ModifyObjects(const TSourceId sourceId, THashMap<TAddress, TObject>&& add, THashSet<TAddress>&& remove) {
        AFL_VERIFY(NActors::TlsActivationContext);
        auto& context = NActors::TActorContext::AsActorContext();
        context.Send(
            GetCurrentNodeServiceId(), new NSource::TEvents<TPolicy>::TEvAdditionalObjectsInfo(sourceId, std::move(add), std::move(remove)));
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
        return new NPrivate::TDistributor<TPolicy>(config, conveyorSignals);
    }
};

}   // namespace NKikimr::NGeneralCache
