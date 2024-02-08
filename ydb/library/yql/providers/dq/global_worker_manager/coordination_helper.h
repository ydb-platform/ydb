#pragma once

#include <ydb/library/actors/core/actorsystem.h>

#include <ydb/library/yql/providers/common/metrics/metrics_registry.h>
#include <ydb/library/yql/providers/dq/global_worker_manager/service_node_resolver.h>
#include <ydb/library/yql/providers/dq/actors/yt/resource_manager.h>
#include <ydb/library/yql/providers/dq/config/config.pb.h>

#include <util/generic/ptr.h>

namespace NYql {

struct TWorkerRuntimeData;

class ICoordinationHelper: public TThrRefBase {
public:
    using TPtr = TIntrusivePtr<ICoordinationHelper>;

    virtual ~ICoordinationHelper() = default;

    virtual ui32 GetNodeId() = 0;
    virtual ui32 GetNodeId(
        const TMaybe<ui32> nodeId,
        const TMaybe<TString>& grpcPort,
        ui32 minNodeId,
        ui32 maxNodeId,
        const THashMap<TString, TString>& attributes) = 0;

    virtual TString GetHostname() = 0;
    virtual TString GetIp() = 0;

    virtual NActors::IActor* CreateLockOnCluster(NActors::TActorId ytWrapper, const TString& prefix, const TString& lockName, bool temporary = true) = 0;

    virtual NActors::IActor* CreateLock(const TString& lockName, bool temporary = true) = 0;

    virtual NActors::IActor* CreateServiceNodePinger(const IServiceNodeResolver::TPtr& ptr, const TResourceManagerOptions& rmOptions, const THashMap<TString, TString>& attributes = {}) = 0;

    virtual void StartRegistrator(NActors::TActorSystem* actorSystem) = 0;

    virtual void StartGlobalWorker(NActors::TActorSystem* actorSystem, const TVector<TResourceManagerOptions>& resourceUploaderOptions, IMetricsRegistryPtr metricsRegistry) = 0;

    virtual void StartCleaner(NActors::TActorSystem* actorSystem, const TMaybe<TString>& role) = 0;

    virtual IServiceNodeResolver::TPtr CreateServiceNodeResolver(
        NActors::TActorSystem* actorSystem, const TVector<TString>& hostPortPairs) = 0;

    virtual const NProto::TDqConfig::TYtCoordinator& GetConfig() = 0;

    virtual const NActors::TActorId GetWrapper(NActors::TActorSystem* actorSystem) = 0;

    virtual const NActors::TActorId GetWrapper() = 0;

    virtual const NActors::TActorId GetWrapper(NActors::TActorSystem* actorSystem, const TString& clusterName, const TString& user, const TString& token) = 0;

    virtual TWorkerRuntimeData* GetRuntimeData() = 0;

    virtual void Stop(NActors::TActorSystem* actorSystem) = 0;

    virtual TString GetRevision() = 0;
};

ICoordinationHelper::TPtr CreateCoordiantionHelper(const NProto::TDqConfig::TYtCoordinator& config, const NProto::TDqConfig::TScheduler& schedulerConfig, const TString& role, ui16 interconnectPort, const TString& host, const TString& ip);

} // namespace NYql
