#include "services_initializer.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tx/scheme_board/cache.h>

#include "grpc_pq_read.h"
#include "grpc_pq_write.h"
#include "grpc_pq_schema.h"

namespace NKikimr {
namespace NGRpcService {
namespace V1 {

static const ui32 TopicWriteSessionsMaxCount = 1000000;
static const ui32 TopicReadSessionsMaxCount = 100000;

void ServicesInitializer::Execute() {
    InitWriteService();
    InitReadService();
    InitSchemaService();
}

bool ServicesInitializer::RequiresServiceRegistration(const TActorId serviceId) {
    return ActorSystem->AppData<TAppData>()->PQConfig.GetEnabled() && !ActorSystem->LookupLocalService(serviceId);
}

void ServicesInitializer::RegisterService(const TActorId serviceId, IActor* service) {
    auto actorId = ActorSystem->Register(service, TMailboxType::HTSwap, ActorSystem->AppData<TAppData>()->UserPoolId);
    ActorSystem->RegisterLocalService(serviceId, actorId);
}

TActorId ServicesInitializer::InitNewSchemeCacheActor() {
    auto appData = ActorSystem->AppData<TAppData>();
    auto cacheCounters = GetServiceCounters(Counters, "pqproxy|schemecache");
    auto cacheConfig = MakeIntrusive<NSchemeCache::TSchemeCacheConfig>(appData, cacheCounters);

    auto service = CreateSchemeBoardSchemeCache(cacheConfig.Get());
    return ActorSystem->Register(service, TMailboxType::HTSwap, appData->UserPoolId);
}

void ServicesInitializer::InitWriteService() {
    auto serviceId = NGRpcProxy::V1::GetPQWriteServiceActorID();
    if (RequiresServiceRegistration(serviceId)) {
        auto service = NGRpcProxy::V1::CreatePQWriteService(SchemeCache, Counters, TopicWriteSessionsMaxCount);
        RegisterService(serviceId, service);
    }
}

void ServicesInitializer::InitReadService() {
    auto serviceId = NGRpcProxy::V1::GetPQReadServiceActorID();
    if (RequiresServiceRegistration(serviceId)) {
        auto NewSchemeCache = InitNewSchemeCacheActor();
        auto service = NGRpcProxy::V1::CreatePQReadService(SchemeCache, NewSchemeCache, Counters, TopicReadSessionsMaxCount);
        RegisterService(serviceId, service);
    }
}

void ServicesInitializer::InitSchemaService() {
    auto serviceId = NGRpcProxy::V1::GetPQSchemaServiceActorID();
    static NGRpcProxy::V1::IClustersCfgProvider* providerService; 
    if (RequiresServiceRegistration(serviceId)) {
        auto service = NGRpcProxy::V1::CreatePQSchemaService(&providerService);
        RegisterService(serviceId, service);
    }
    *ClusterCfgProvider = providerService;
}

} // namespace V1
} // namespace NGRpcService
} // namespace NKikimr
