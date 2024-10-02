#pragma once

// todo(gvit): remove
#include <ydb/core/base/event_filter.h>

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
    class TS3ProxyResolverConfig;
    class TBackgroundCleaningConfig;
    class TGraphConfig;
    class TMetadataCacheConfig;
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
    class TFeatureFlags;
}

namespace NKikimr {
    namespace NPDisk {
        struct IIoContextFactory;
    }
}

namespace NMonitoring {
    class TBusNgMonPage;
}

namespace NYdb {
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

class TFormatFactory;

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

    NSQS::IAuthFactory* SqsAuthFactory = nullptr;

    NHttpProxy::IAuthFactory* DataStreamsAuthFactory = nullptr;

    NActors::IActor*(*FolderServiceFactory)(const NKikimrProto::NFolderService::TFolderServiceConfig&) = nullptr;

    const NMsgBusProxy::IPersQueueGetReadSessionsInfoWorkerFactory* PersQueueGetReadSessionsInfoWorkerFactory = nullptr;
    const NPQ::IPersQueueMirrorReaderFactory* PersQueueMirrorReaderFactory = nullptr;
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
    TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> InFlightLimiterRegistry;

    TIntrusivePtr<NInterconnect::TPollerThreads> PollerThreads;

    THolder<NKikimrCms::TCmsConfig> DefaultCmsConfig;

    NKikimrStream::TStreamingConfig& StreamingConfig;
    NKikimrPQ::TPQConfig& PQConfig;
    NKikimrPQ::TPQClusterDiscoveryConfig& PQClusterDiscoveryConfig;
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
    NKikimrConfig::TAuditConfig& AuditConfig;
    NKikimrConfig::TCompactionConfig& CompactionConfig;
    NKikimrConfig::TDomainsConfig& DomainsConfig;
    NKikimrConfig::TBootstrap& BootstrapConfig;
    NKikimrConfig::TAwsCompatibilityConfig& AwsCompatibilityConfig;
    NKikimrConfig::TS3ProxyResolverConfig& S3ProxyResolverConfig;
    NKikimrConfig::TBackgroundCleaningConfig& BackgroundCleaningConfig;
    NKikimrConfig::TGraphConfig& GraphConfig;
    NKikimrSharedCache::TSharedCacheConfig& SharedCacheConfig;
    NKikimrConfig::TMetadataCacheConfig& MetadataCacheConfig;
    NKikimrReplication::TReplicationDefaults& ReplicationConfig;
    bool EnforceUserTokenRequirement = false;
    bool EnforceUserTokenCheckRequirement = false; // check token if it was specified
    bool AllowHugeKeyValueDeletes = true; // delete when all clients limit deletes per request
    bool EnableKqpSpilling = false;
    bool AllowShadowDataInSchemeShardForTests = false;
    bool EnableMvccSnapshotWithLegacyDomainRoot = false;
    bool UsePartitionStatsCollectorForTests = false;
    bool DisableCdcAutoSwitchingToReadyStateForTests = false;
    TVector<TString> AdministrationAllowedSIDs; // users/groups which allowed to perform administrative tasks
    TVector<TString> DefaultUserSIDs;
    TString AllAuthenticatedUsers = "all-users@well-known";
    TVector<TString> RegisterDynamicNodeAllowedSIDs;
    TString TenantName;
    TString NodeName;

    TIntrusivePtr<TResourceProfiles> ResourceProfiles;

    TProgramShouldContinue * const KikimrShouldContinue;
    bool EnableIntrospection = true;

    // Used to allow column families for testing
    bool AllowColumnFamiliesForTest = false;
    bool AllowPrivateTableDescribeForTest = false;

    // Used to allow immediate ReadTable in tests
    bool AllowReadTableImmediate = false;

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

    TAppData(
            ui32 sysPoolId, ui32 userPoolId, ui32 ioPoolId, ui32 batchPoolId,
            TMap<TString, ui32> servicePools,
            const NScheme::TTypeRegistry* typeRegistry,
            const NMiniKQL::IFunctionRegistry* functionRegistry,
            const TFormatFactory* formatFactory,
            TProgramShouldContinue *kikimrShouldContinue);

    ~TAppData();
};

inline TAppData* AppData(NActors::TActorSystem* actorSystem) {
    Y_DEBUG_ABORT_UNLESS(actorSystem);
    TAppData* const x = actorSystem->AppData<TAppData>();
    Y_DEBUG_ABORT_UNLESS(x && x->Magic == TAppData::MagicTag);
    return x;
}

inline bool HasAppData() {
    return !!NActors::TlsActivationContext;
}

inline TAppData& AppDataVerified() {
    Y_ABORT_UNLESS(HasAppData());
    auto& actorSystem = NActors::TlsActivationContext->ExecutorThread.ActorSystem;
    Y_ABORT_UNLESS(actorSystem);
    TAppData* const x = actorSystem->AppData<TAppData>();
    Y_ABORT_UNLESS(x && x->Magic == TAppData::MagicTag);
    return *x;
}

inline TAppData* AppData() {
    return AppData(NActors::TlsActivationContext->ExecutorThread.ActorSystem);
}

inline TAppData* AppData(const NActors::TActorContext &ctx) {
    return AppData(ctx.ActorSystem());
}

} // NKikimr
