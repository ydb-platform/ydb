#pragma once
#include "config.h"
#include "factories.h"
#include "service_initializer.h"

#include <ydb/core/memory_controller/memory_controller.h>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/core/base/appdata.h>
#include <ydb/core/base/statestorage.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/tablet/tablet_setup.h>
#include <ydb/core/tablet/node_tablet_monitor.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>

#include <ydb/core/protos/config.pb.h>

#include <ydb/public/lib/base/msgbus.h>

#include <ydb/core/fq/libs/shared_resources/interface/shared_resources.h>

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/log_settings.h>
#include <ydb/library/actors/core/scheduler_actor.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

#include <util/generic/vector.h>

namespace NKikimr {

namespace NKikimrServicesInitializers {

class IKikimrServicesInitializer : public IServiceInitializer {
protected:
    NKikimrConfig::TAppConfig& Config;
    const ui32                       NodeId;
    const TKikimrScopeId             ScopeId;

public:
    IKikimrServicesInitializer(const TKikimrRunConfig& runConfig);
};

// base, nameservice, interconnect
class TBasicServicesInitializer : public IKikimrServicesInitializer {
    static IExecutorPool*
    CreateExecutorPool(const NKikimrConfig::TActorSystemConfig::TExecutor& poolConfig,
        const NKikimrConfig::TActorSystemConfig& systemConfig,
        ui32 poolId, ui32 maxActivityType);

    static ISchedulerThread* CreateScheduler(const NKikimrConfig::TActorSystemConfig::TScheduler &config);

    std::shared_ptr<TModuleFactories> Factories;

public:
    TBasicServicesInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TImmediateControlBoardInitializer : public IKikimrServicesInitializer {
public:
    TImmediateControlBoardInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TBSNodeWardenInitializer : public IKikimrServicesInitializer {
public:
    TBSNodeWardenInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

// ss: proxies, replicas; board: replicas; scheme_board: replicas
class TStateStorageServiceInitializer : public IKikimrServicesInitializer {
public:
    TStateStorageServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TLocalServiceInitializer : public IKikimrServicesInitializer {
public:
    TLocalServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TSharedCacheInitializer : public IKikimrServicesInitializer {
public:
    TSharedCacheInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TBlobCacheInitializer : public IKikimrServicesInitializer {
public:
    TBlobCacheInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TLoggerInitializer : public IKikimrServicesInitializer {
    TIntrusivePtr<NActors::NLog::TSettings> LogSettings;
    std::shared_ptr<TLogBackend> LogBackend;
    TString PathToConfigCacheFile;

public:
    TLoggerInitializer(const TKikimrRunConfig& runConfig,
                       TIntrusivePtr<NActors::NLog::TSettings> logSettings,
                       std::shared_ptr<TLogBackend> logBackend);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TSchedulerActorInitializer : public IKikimrServicesInitializer {
public:
    TSchedulerActorInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TProfilerInitializer : public IKikimrServicesInitializer {
public:
    TProfilerInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TResourceBrokerInitializer : public IKikimrServicesInitializer {
public:
    TResourceBrokerInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TTabletResolverInitializer : public IKikimrServicesInitializer {
public:
    TTabletResolverInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TTabletPipePerNodeCachesInitializer : public IKikimrServicesInitializer {
public:
    TTabletPipePerNodeCachesInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TTabletMonitoringProxyInitializer : public IKikimrServicesInitializer {
public:
    TTabletMonitoringProxyInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TTabletCountersAggregatorInitializer : public IKikimrServicesInitializer {
public:
    TTabletCountersAggregatorInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TGRpcProxyStatusInitializer : public IKikimrServicesInitializer {
public:
    TGRpcProxyStatusInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TRestartsCountPublisher : public IKikimrServicesInitializer {
    static void PublishRestartsCount(const ::NMonitoring::TDynamicCounters::TCounterPtr& counter,
                                     const TString& restartsCountFile);

public:
    TRestartsCountPublisher(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TBootstrapperInitializer : public IKikimrServicesInitializer {
public:
    TBootstrapperInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

// alternative (RTMR-style) bootstrapper
class TTabletsInitializer : public IKikimrServicesInitializer {
    TIntrusivePtr<ITabletFactory> CustomTablets;

public:
    TTabletsInitializer(const TKikimrRunConfig& runConfig,
                        TIntrusivePtr<ITabletFactory> customTablets);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TMediatorTimeCastProxyInitializer : public IKikimrServicesInitializer {
public:
    TMediatorTimeCastProxyInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TTxProxyInitializer : public IKikimrServicesInitializer {
    TVector<ui64> CollectAllAllocatorsFromAllDomains(const NKikimr::TAppData* appData);

public:
    TTxProxyInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TLongTxServiceInitializer : public IKikimrServicesInitializer {
public:
    TLongTxServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TSequenceProxyServiceInitializer : public IKikimrServicesInitializer {
public:
    TSequenceProxyServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TMiniKQLCompileServiceInitializer : public IKikimrServicesInitializer {
public:
    TMiniKQLCompileServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

// msgbus: proxy, trace
class TMessageBusServicesInitializer : public IKikimrServicesInitializer {
    NMsgBusProxy::IMessageBusServer& BusServer;

public:
    TMessageBusServicesInitializer(const TKikimrRunConfig& runConfig,
                                   NMsgBusProxy::IMessageBusServer& busServer);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

// ticket_parser and so on...
class TSecurityServicesInitializer : public IKikimrServicesInitializer {
public:
    std::shared_ptr<TModuleFactories> Factories;
    TSecurityServicesInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

// grpc_proxy
class TGRpcServicesInitializer : public IKikimrServicesInitializer {
private:
    std::shared_ptr<TModuleFactories> Factories;

public:
    TGRpcServicesInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
// stats_collector, procstats_collector
class TStatsCollectorInitializer : public IKikimrServicesInitializer {
public:
    TStatsCollectorInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};
#endif

class TSelfPingInitializer : public IKikimrServicesInitializer {
public:
    TSelfPingInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TWhiteBoardServiceInitializer : public IKikimrServicesInitializer {
public:
    TWhiteBoardServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TTabletMonitorInitializer : public IKikimrServicesInitializer {
    TIntrusivePtr<NNodeTabletMonitor::ITabletStateClassifier> TabletStateClassifier;
    TIntrusivePtr<NNodeTabletMonitor::ITabletListRenderer> TabletListRenderer;

public:
    TTabletMonitorInitializer(const TKikimrRunConfig& runConfig,
                              const TIntrusivePtr<NNodeTabletMonitor::ITabletStateClassifier>& tabletStateClassifier,
                              const TIntrusivePtr<NNodeTabletMonitor::ITabletListRenderer>& tabletListRenderer);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TViewerInitializer : public IKikimrServicesInitializer {
    const TKikimrRunConfig& KikimrRunConfig;

public:
    TViewerInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;

private:
    std::shared_ptr<TModuleFactories> Factories;
};

class TLoadInitializer : public IKikimrServicesInitializer {
public:
    TLoadInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TFailureInjectionInitializer : public IKikimrServicesInitializer {
public:
    TFailureInjectionInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) override;
};

class TPersQueueL2CacheInitializer : public IKikimrServicesInitializer {
public:
    TPersQueueL2CacheInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TNetClassifierInitializer : public IKikimrServicesInitializer {
public:
    TNetClassifierInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TPersQueueClusterTrackerInitializer : public IKikimrServicesInitializer {
public:
    TPersQueueClusterTrackerInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TPersQueueDirectReadCacheInitializer : public IKikimrServicesInitializer {
public:
    TPersQueueDirectReadCacheInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TMemProfMonitorInitializer : public IKikimrServicesInitializer {
    TIntrusiveConstPtr<NMemory::IProcessMemoryInfoProvider> ProcessMemoryInfoProvider;
public:
    TMemProfMonitorInitializer(const TKikimrRunConfig& runConfig, TIntrusiveConstPtr<NMemory::IProcessMemoryInfoProvider> processMemoryInfoProvider);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TMemoryTrackerInitializer : public IKikimrServicesInitializer {
public:
    TMemoryTrackerInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TMemoryControllerInitializer : public IKikimrServicesInitializer {
    TIntrusiveConstPtr<NMemory::IProcessMemoryInfoProvider> ProcessMemoryInfoProvider;
public:
    TMemoryControllerInitializer(const TKikimrRunConfig& runConfig, TIntrusiveConstPtr<NMemory::IProcessMemoryInfoProvider> processMemoryInfoProvider);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TQuoterServiceInitializer : public IKikimrServicesInitializer {
public:
    TQuoterServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TKqpServiceInitializer : public IKikimrServicesInitializer {
public:
    TKqpServiceInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories,
        IGlobalObjectStorage& globalObjects);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
private:
    std::shared_ptr<TModuleFactories> Factories;
    IGlobalObjectStorage& GlobalObjects;
};

class TCompDiskLimiterInitializer: public IKikimrServicesInitializer {
public:
    TCompDiskLimiterInitializer(const TKikimrRunConfig& runConfig);
    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TCompConveyorInitializer: public IKikimrServicesInitializer {
public:
    TCompConveyorInitializer(const TKikimrRunConfig& runConfig);
    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TScanConveyorInitializer: public IKikimrServicesInitializer {
public:
    TScanConveyorInitializer(const TKikimrRunConfig& runConfig);
    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TInsertConveyorInitializer: public IKikimrServicesInitializer {
public:
    TInsertConveyorInitializer(const TKikimrRunConfig& runConfig);
    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TExternalIndexInitializer: public IKikimrServicesInitializer {
public:
    TExternalIndexInitializer(const TKikimrRunConfig& runConfig);
    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TMetadataProviderInitializer: public IKikimrServicesInitializer {
public:
    TMetadataProviderInitializer(const TKikimrRunConfig& runConfig);
    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TMemoryLogInitializer : public IKikimrServicesInitializer {
    size_t LogBufferSize = 0;
    size_t LogGrainSize = 0;

public:
    TMemoryLogInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TCmsServiceInitializer : public IKikimrServicesInitializer {
public:
    TCmsServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TLeaseHolderInitializer : public IKikimrServicesInitializer {
public:
    TLeaseHolderInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TSqsServiceInitializer : public IKikimrServicesInitializer {
public:
    TSqsServiceInitializer(const TKikimrRunConfig& runConfig, const std::shared_ptr<TModuleFactories>& factories);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;

private:
    std::shared_ptr<TModuleFactories> Factories;
};

class THttpProxyServiceInitializer : public IKikimrServicesInitializer {
public:
    THttpProxyServiceInitializer(const TKikimrRunConfig& runConfig, const std::shared_ptr<TModuleFactories>& factories);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;

private:
    std::shared_ptr<TModuleFactories> Factories;
};

class TConfigsDispatcherInitializer : public IKikimrServicesInitializer {
public:
    TConfigsDispatcherInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;

private:
    NConfig::TConfigsDispatcherInitInfo ConfigsDispatcherInitInfo;
};

class TConfigsCacheInitializer : public IKikimrServicesInitializer {
private:
    TString PathToConfigCacheFile;
public:
    TConfigsCacheInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TTabletInfoInitializer : public IKikimrServicesInitializer {
public:
    TTabletInfoInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TConfigValidatorsInitializer : public IKikimrServicesInitializer {
public:
    TConfigValidatorsInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TSysViewServiceInitializer : public IKikimrServicesInitializer {
public:
    TSysViewServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TStatServiceInitializer : public IKikimrServicesInitializer {
public:
    TStatServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TMeteringWriterInitializer : public IKikimrServicesInitializer {
public:
    TMeteringWriterInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
private:
    const TKikimrRunConfig& KikimrRunConfig;
};

class TAuditWriterInitializer : public IKikimrServicesInitializer {
public:
    TAuditWriterInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
private:
    const TKikimrRunConfig& KikimrRunConfig;
};

class TSchemeBoardMonitoringInitializer : public IKikimrServicesInitializer {
public:
    TSchemeBoardMonitoringInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TYqlLogsInitializer : public IKikimrServicesInitializer {
public:
    TYqlLogsInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class THealthCheckInitializer : public IKikimrServicesInitializer {
public:
    THealthCheckInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TFederatedQueryInitializer : public IKikimrServicesInitializer {
public:
    TFederatedQueryInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories, NFq::IYqSharedResources::TPtr yqSharedResources);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;

    static void SetIcPort(ui32 icPort);
private:
    std::shared_ptr<TModuleFactories> Factories;
    NFq::IYqSharedResources::TPtr YqSharedResources;
    static ui32 IcPort;
};

class TReplicationServiceInitializer : public IKikimrServicesInitializer {
public:
    TReplicationServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TLocalPgWireServiceInitializer : public IKikimrServicesInitializer {
public:
    TLocalPgWireServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TKafkaProxyServiceInitializer : public IKikimrServicesInitializer {
public:
    TKafkaProxyServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TIcNodeCacheServiceInitializer : public IKikimrServicesInitializer {
public:
    TIcNodeCacheServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TDatabaseMetadataCacheInitializer : public IKikimrServicesInitializer {
public:
    TDatabaseMetadataCacheInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TGraphServiceInitializer : public IKikimrServicesInitializer {
public:
    TGraphServiceInitializer(const TKikimrRunConfig& runConfig);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

class TAwsApiInitializer : public IServiceInitializer {
    IGlobalObjectStorage& GlobalObjects;

public:
    TAwsApiInitializer(IGlobalObjectStorage& globalObjects);

    void InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) override;
};

} // namespace NKikimrServicesInitializers
} // namespace NKikimr
