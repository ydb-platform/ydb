#pragma once

// todo(gvit): remove
#include <ydb/core/base/event_filter.h>
#include <util/generic/hash_set.h>

class TProgramShouldContinue;
class IRandomProvider;
class ITimeProvider;

namespace NMonotonic {
    class IMonotonicTimeProvider;
}

namespace NActors {
    class TMon;
}

namespace NKikimr {
    namespace NGRpcService {
        class TInFlightLimiterRegistry;
    }
    namespace NSharedCache {
        class TSharedCachePages;
    }
    namespace NJaegerTracing {
        class TSamplingThrottlingConfigurator;
    }
}

namespace NKikimrCms {
    class TCmsConfig;
}

namespace NKikimrSharedCache {
    class TSharedCacheConfig;
}

namespace NKikimrProto {
    class TKeyConfig;
    class TAuthConfig;
    class TDataIntegrityTrailsConfig;

    namespace NFolderService {
        class TFolderServiceConfig;
    }
}

namespace NKikimrStream {
    class TStreamingConfig;
}

namespace NKikimrConfig {
    class TAppConfig;
    class TStreamingConfig;
    class TMeteringConfig;
    class TSqsConfig;
    class TKafkaProxyConfig;
    class TAuthConfig;

    class THiveConfig;
    class TDataShardConfig;
    class TColumnShardConfig;
    class TSchemeShardConfig;
    class TMeteringConfig;
    class TAuditConfig;
    class TCompactionConfig;
    class TDomainsConfig;
    class TBootstrap;
    class TAwsCompatibilityConfig;
    class TAwsClientConfig;
    class TS3ProxyResolverConfig;
    class TBackgroundCleaningConfig;
    class TDataErasureConfig;
    class TGraphConfig;
    class TMetadataCacheConfig;
    class TMemoryControllerConfig;
    class TFeatureFlags;
    class THealthCheckConfig;
    class TWorkloadManagerConfig;
    class TQueryServiceConfig;
    class TBridgeConfig;
    class TStatisticsConfig;
    class TSystemTabletBackupConfig;
    class TRecoveryShardConfig;
    class TClusterDiagnosticsConfig;
}

namespace NKikimrReplication {
    class TReplicationDefaults;
}

namespace NKikimrNetClassifier {
    class TNetClassifierDistributableConfig;
    class TNetClassifierConfig;
}

namespace NKikimrPQ {
    class TPQClusterDiscoveryConfig;
    class TPQConfig;
}

namespace NActors {
    class IActor;
    class TActorSystem;
}

namespace NKikimr {
    struct TDynamicNameserviceConfig;
    struct TChannelProfiles;
    struct TDomainsInfo;
    class TResourceProfiles;
    class TControlBoard;
    class TDynamicControlBoard;
    class TFeatureFlags;
    class TMetricsConfig;
}

namespace NKikimr {
    namespace NPDisk {
        struct IIoContextFactory;
    }
}

namespace NMonitoring {
    class TBusNgMonPage;
}

namespace NYdb::inline Dev {
    class TDriver;
}

namespace NKikimr {

namespace NScheme {
    class TTypeRegistry;
}

namespace NMiniKQL {
    class IFunctionRegistry;
}

namespace NDataShard {
    class IExportFactory;
}

namespace NSQS {
    class IEventsWriterFactory;
    class IAuthFactory;
}

namespace NHttpProxy {
    class IAuthFactory;
}

namespace NMsgBusProxy {
    class IPersQueueGetReadSessionsInfoWorkerFactory;
}

namespace NPQ {
    class IPersQueueMirrorReaderFactory;
}

namespace NSchemeShard {
    class IOperationFactory;
}

namespace NReplication::NService {
    class ITransferWriterFactory;
}

class TFormatFactory;

namespace NYamlConfig {
    class IConfigSwissKnife;
}

namespace NAudit {
    class TAuditConfig;
}

struct TAppData {
    static const ui32 MagicTag = 0x2991AAF8;
    const ui32 Magic;

    struct TImpl;
    std::unique_ptr<TImpl> Impl;

    const ui32 SystemPoolId;
    const ui32 UserPoolId;
    const ui32 IOPoolId;
    const ui32 BatchPoolId;
    TMap<TString, ui32> ServicePools;

    const NScheme::TTypeRegistry* TypeRegistry = nullptr;
    const NMiniKQL::IFunctionRegistry* FunctionRegistry = nullptr;
    const NDataShard::IExportFactory *DataShardExportFactory = nullptr;
    const TFormatFactory* FormatFactory = nullptr;
    const NSQS::IEventsWriterFactory* SqsEventsWriterFactory = nullptr;
    const NSchemeShard::IOperationFactory *SchemeOperationFactory = nullptr;
    const NYamlConfig::IConfigSwissKnife *ConfigSwissKnife = nullptr;

    NSQS::IAuthFactory* SqsAuthFactory = nullptr;

    NHttpProxy::IAuthFactory* DataStreamsAuthFactory = nullptr;

    NActors::IActor*(*FolderServiceFactory)(const NKikimrProto::NFolderService::TFolderServiceConfig&) = nullptr;

    const NMsgBusProxy::IPersQueueGetReadSessionsInfoWorkerFactory* PersQueueGetReadSessionsInfoWorkerFactory = nullptr;
    const NPQ::IPersQueueMirrorReaderFactory* PersQueueMirrorReaderFactory = nullptr;
    std::shared_ptr<NReplication::NService::ITransferWriterFactory> TransferWriterFactory = nullptr;
    NYdb::TDriver* YdbDriver = nullptr;
    const NPDisk::IIoContextFactory* IoContextFactory = nullptr;

    static TIntrusivePtr<IRandomProvider> RandomProvider;
    static TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<NMonotonic::IMonotonicTimeProvider> MonotonicTimeProvider;
    TIntrusivePtr<TDomainsInfo> DomainsInfo;
    TIntrusivePtr<TChannelProfiles> ChannelProfiles;
    TIntrusivePtr<TDynamicNameserviceConfig> DynamicNameserviceConfig;

    ui64 ProxySchemeCacheNodes;
    ui64 ProxySchemeCacheDistrNodes;

    ui64 CompilerSchemeCachePaths;
    ui64 CompilerSchemeCacheTables;

    NActors::TMon* Mon;
    ::NMonitoring::TDynamicCounterPtr Counters;
    TIntrusivePtr<NKikimr::TControlBoard> Icb;
    TIntrusivePtr<NKikimr::TDynamicControlBoard> Dcb;
    TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> InFlightLimiterRegistry;
    TIntrusivePtr<NSharedCache::TSharedCachePages> SharedCachePages;

    TIntrusivePtr<NInterconnect::TPollerThreads> PollerThreads;

    THolder<NKikimrCms::TCmsConfig> DefaultCmsConfig;

    NKikimrStream::TStreamingConfig& StreamingConfig;
    NKikimrPQ::TPQConfig& PQConfig;
    NKikimrPQ::TPQClusterDiscoveryConfig& PQClusterDiscoveryConfig;
    NKikimrConfig::TKafkaProxyConfig& KafkaProxyConfig;
    NKikimrNetClassifier::TNetClassifierConfig& NetClassifierConfig;
    NKikimrNetClassifier::TNetClassifierDistributableConfig& NetClassifierDistributableConfig;
    NKikimrConfig::TSqsConfig& SqsConfig;
    NKikimrProto::TAuthConfig& AuthConfig;
    NKikimrProto::TKeyConfig& KeyConfig;
    NKikimrProto::TKeyConfig& PDiskKeyConfig;
    TFeatureFlags& FeatureFlags;
    NKikimrConfig::THiveConfig& HiveConfig;
    NKikimrConfig::TDataShardConfig& DataShardConfig;
    NKikimrConfig::TColumnShardConfig& ColumnShardConfig;
    NKikimrConfig::TSchemeShardConfig& SchemeShardConfig;
    NKikimrConfig::TMeteringConfig& MeteringConfig;
    NKikimr::NAudit::TAuditConfig& AuditConfig;
    NKikimrConfig::TCompactionConfig& CompactionConfig;
    NKikimrConfig::TDomainsConfig& DomainsConfig;
    NKikimrConfig::TBootstrap& BootstrapConfig;
    NKikimrConfig::TAwsCompatibilityConfig& AwsCompatibilityConfig;
    NKikimrConfig::TAwsClientConfig& AwsClientConfig;
    NKikimrConfig::TS3ProxyResolverConfig& S3ProxyResolverConfig;
    NKikimrConfig::TBackgroundCleaningConfig& BackgroundCleaningConfig;
    NKikimrConfig::TGraphConfig& GraphConfig;
    NKikimrSharedCache::TSharedCacheConfig& SharedCacheConfig;
    NKikimrConfig::TMetadataCacheConfig& MetadataCacheConfig;
    NKikimrConfig::TMemoryControllerConfig& MemoryControllerConfig;
    NKikimrReplication::TReplicationDefaults& ReplicationConfig;
    NKikimrProto::TDataIntegrityTrailsConfig& DataIntegrityTrailsConfig;
    NKikimrConfig::TDataErasureConfig& ShredConfig;
    NKikimrConfig::THealthCheckConfig& HealthCheckConfig;
    NKikimrConfig::TWorkloadManagerConfig& WorkloadManagerConfig;
    NKikimrConfig::TQueryServiceConfig& QueryServiceConfig;
    NKikimrConfig::TBridgeConfig& BridgeConfig;
    NKikimrConfig::TStatisticsConfig& StatisticsConfig;
    TMetricsConfig& MetricsConfig;
    NKikimrConfig::TSystemTabletBackupConfig& SystemTabletBackupConfig;
    NKikimrConfig::TRecoveryShardConfig& RecoveryShardConfig;
    NKikimrConfig::TClusterDiagnosticsConfig& ClusterDiagnosticsConfig;
    bool EnforceUserTokenRequirement = false;
    bool EnforceUserTokenCheckRequirement = false; // check token if it was specified
    bool AllowHugeKeyValueDeletes = true; // delete when all clients limit deletes per request
    bool EnableKqpSpilling = false;
    bool AllowShadowDataInSchemeShardForTests = false;
    bool EnableMvccSnapshotWithLegacyDomainRoot = false;
    bool UsePartitionStatsCollectorForTests = false;
    bool DisableCdcAutoSwitchingToReadyStateForTests = false;
    bool BridgeModeEnabled = false;
    bool SuppressBridgeModeBootstrapperLogic = false; // for tests

    TVector<TString> AdministrationAllowedSIDs; // use IsAdministrator method to check whether a user or a group is allowed to perform administrative tasks
    TVector<TString> RegisterDynamicNodeAllowedSIDs;
    TVector<TString> BootstrapAllowedSIDs;
    TVector<TString> DefaultUserSIDs;
    TString AllAuthenticatedUsers = "all-users@well-known"; // it's only here to avoid many unit-tests problems

    TString TenantName;
    TString NodeName;

    TIntrusivePtr<TResourceProfiles> ResourceProfiles;

    TProgramShouldContinue * const KikimrShouldContinue;
    bool EnableIntrospection = true;

    // Used to allow column families for testing
    bool AllowPrivateTableDescribeForTest = false;

    // Used to disable object deletion in schemeshard for cleanup tests
    bool DisableSchemeShardCleanupOnDropForTest = false;

    // Used to exclude indexes, cdc streams & sequences from table description on DataShards
    bool DisableRichTableDescriptionForTest = false;

    TMaybe<ui32> ZstdBlockSizeForTest;

    // Used to disable checking nodes with sys tablets only in cms
    bool DisableCheckingSysNodesCms = false;

    TKikimrScopeId LocalScopeId;

    TMap<TString, TString> Labels;

    TString ClusterName;

    bool YamlConfigEnabled = false;

    // Test failure injection system
    THashSet<ui64> InjectedFailures;

    // Tracing configurator (look for tracing config in ydb/core/jaeger_tracing/actors_tracing_control)
    TIntrusivePtr<NKikimr::NJaegerTracing::TSamplingThrottlingConfigurator> TracingConfigurator;

    TAppData(
            ui32 sysPoolId, ui32 userPoolId, ui32 ioPoolId, ui32 batchPoolId,
            TMap<TString, ui32> servicePools,
            const NScheme::TTypeRegistry* typeRegistry,
            const NMiniKQL::IFunctionRegistry* functionRegistry,
            const TFormatFactory* formatFactory,
            TProgramShouldContinue *kikimrShouldContinue);

    ~TAppData();

    void InitFeatureFlags(const NKikimrConfig::TFeatureFlags& flags);
    void UpdateRuntimeFlags(const NKikimrConfig::TFeatureFlags& flags);

    // Test failure injection methods
    void InjectFailure(ui64 failureType) {
        InjectedFailures.insert(failureType);
    }

    void RemoveFailure(ui64 failureType) {
        InjectedFailures.erase(failureType);
    }

    void ClearAllFailures() {
        InjectedFailures.clear();
    }

    bool HasInjectedFailure(ui64 failureType) const {
        return InjectedFailures.contains(failureType);
    }
};

inline bool CheckAppData(const TAppData* appData) {
    return appData && appData->Magic == TAppData::MagicTag;
}

inline TAppData* AppData(NActors::TActorSystem* actorSystem) {
    Y_DEBUG_ABORT_UNLESS(actorSystem);
    TAppData* const x = actorSystem->AppData<TAppData>();
    Y_DEBUG_ABORT_UNLESS(CheckAppData(x));
    return x;
}

inline TAppData* AppData() {
    return AppData(NActors::TActivationContext::ActorSystem());
}

inline TAppData* AppData(const NActors::TActorContext &ctx) {
    return AppData(ctx.ActorSystem());
}

inline bool HasAppData(NActors::TActorSystem* actorSystem) {
    return actorSystem && CheckAppData(actorSystem->AppData<TAppData>());
}

inline bool HasAppData() {
    return !!NActors::TlsActivationContext && HasAppData(NActors::TActivationContext::ActorSystem());
}

inline TAppData& AppDataVerified() {
    Y_ABORT_UNLESS(HasAppData());
    return *AppData();
}

} // NKikimr
