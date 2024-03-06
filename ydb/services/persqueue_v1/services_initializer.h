#pragma once

#include <ydb/library/actors/core/actorsystem.h>

namespace NKikimr {

namespace NGRpcProxy::V1 {
class IClustersCfgProvider;
}

namespace NGRpcService {
namespace V1 {

class ServicesInitializer {
public:
    ServicesInitializer(NActors::TActorSystem* actorSystem,
                        NActors::TActorId schemeCache,
                        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters,
                        NGRpcProxy::V1::IClustersCfgProvider** p)
        : ActorSystem(actorSystem)
        , SchemeCache(schemeCache)
        , Counters(counters) 
        , ClusterCfgProvider(p) {
    }

    void Execute();

private:
    bool RequiresServiceRegistration(const NActors::TActorId serviceId);
    void RegisterService(const NActors::TActorId serviceId, NActors::IActor* service);

    NActors::TActorId InitNewSchemeCacheActor();
    void InitWriteService();
    void InitReadService();
    void InitSchemaService();

private:
    NActors::TActorSystem* ActorSystem;
    NActors::TActorId SchemeCache;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
    NGRpcProxy::V1::IClustersCfgProvider** ClusterCfgProvider;
};


} // namespace V1
} // namespace NGRpcService
} // namespace NKikimr
