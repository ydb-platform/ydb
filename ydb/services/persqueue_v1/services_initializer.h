#pragma once

#include <ydb/library/actors/core/actorsystem_fwd.h>
#include <ydb/library/actors/core/actorid.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

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

private:
    NActors::TActorSystem* ActorSystem;
    NActors::TActorId SchemeCache;
    TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;
};


} // namespace V1
} // namespace NGRpcService
} // namespace NKikimr
