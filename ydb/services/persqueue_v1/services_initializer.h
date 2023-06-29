#pragma once

#include <library/cpp/actors/core/actorsystem.h>

namespace NKikimr {
namespace NGRpcService {
namespace V1 {

class ServicesInitializer {
public:
    ServicesInitializer(NActors::TActorSystem* actorSystem,
                        NActors::TActorId schemeCache,
                        TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : ActorSystem(actorSystem)
        , SchemeCache(schemeCache)
        , Counters(counters) {
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
};


} // namespace V1
} // namespace NGRpcService
} // namespace NKikimr
