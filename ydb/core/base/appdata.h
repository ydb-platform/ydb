#pragma once

#include "defs.h"
#include "channel_profiles.h"
#include "domain.h"
#include "feature_flags.h"
#include "nameservice.h"
#include "tablet_types.h"
#include "resource_profile.h"
#include "event_filter.h"

#include <ydb/core/control/immediate_control_board_impl.h>
#include <ydb/core/grpc_services/grpc_helper.h>
#include <ydb/core/protos/auth.pb.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/key.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/stream.pb.h>
#include <ydb/library/pdisk_io/aio.h>

#include <library/cpp/actors/interconnect/poller_tcp.h>
#include <library/cpp/actors/core/executor_thread.h>
#include <library/cpp/actors/core/monotonic_provider.h>
#include <library/cpp/actors/util/should_continue.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NActors {
    class TMon;
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

struct TAppConfig : public NKikimrConfig::TAppConfig, public TThrRefBase, TNonCopyable {
    explicit TAppConfig(const NKikimrConfig::TAppConfig& c)
        : NKikimrConfig::TAppConfig(c)
    {}
};

struct TAppData {
    static const ui32 MagicTag = 0x2991AAF8;
    const ui32 Magic;

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

    IActor*(*FolderServiceFactory)(const NKikimrProto::NFolderService::TFolderServiceConfig&);

    const NMsgBusProxy::IPersQueueGetReadSessionsInfoWorkerFactory* PersQueueGetReadSessionsInfoWorkerFactory = nullptr;
    const NPQ::IPersQueueMirrorReaderFactory* PersQueueMirrorReaderFactory = nullptr;
    NYdb::TDriver* YdbDriver = nullptr;
    const NPDisk::IIoContextFactory* IoContextFactory = nullptr;

    static TIntrusivePtr<IRandomProvider> RandomProvider;
    static TIntrusivePtr<ITimeProvider> TimeProvider;
    TIntrusivePtr<IMonotonicTimeProvider> MonotonicTimeProvider;
    TIntrusivePtr<TDomainsInfo> DomainsInfo;
    TIntrusivePtr<TChannelProfiles> ChannelProfiles;
    TIntrusivePtr<TDynamicNameserviceConfig> DynamicNameserviceConfig;

    ui64 ProxySchemeCacheNodes;
    ui64 ProxySchemeCacheDistrNodes;

    ui64 CompilerSchemeCachePaths;
    ui64 CompilerSchemeCacheTables;

    NActors::TMon* Mon;
    ::NMonitoring::TDynamicCounterPtr Counters;
    NMonitoring::TBusNgMonPage* BusMonPage;
    TIntrusivePtr<NKikimr::TControlBoard> Icb;
    TIntrusivePtr<NGRpcService::TInFlightLimiterRegistry> InFlightLimiterRegistry;

    TIntrusivePtr<NInterconnect::TPollerThreads> PollerThreads;

    THolder<NKikimrBlobStorage::TNodeWardenServiceSet> StaticBlobStorageConfig;
    THolder<NKikimrCms::TCmsConfig> DefaultCmsConfig;

    NKikimrStream::TStreamingConfig StreamingConfig;
    NKikimrPQ::TPQConfig PQConfig;
    NKikimrPQ::TPQClusterDiscoveryConfig PQClusterDiscoveryConfig;
    NKikimrNetClassifier::TNetClassifierConfig NetClassifierConfig;
    NKikimrNetClassifier::TNetClassifierDistributableConfig NetClassifierDistributableConfig;
    NKikimrConfig::TSqsConfig SqsConfig;
    NKikimrProto::TAuthConfig AuthConfig;
    NKikimrProto::TKeyConfig KeyConfig;
    NKikimrProto::TKeyConfig PDiskKeyConfig;
    TFeatureFlags FeatureFlags;
    NKikimrConfig::THiveConfig HiveConfig;
    NKikimrConfig::TDataShardConfig DataShardConfig;
    NKikimrConfig::TColumnShardConfig ColumnShardConfig;
    NKikimrConfig::TSchemeShardConfig SchemeShardConfig;
    NKikimrConfig::TMeteringConfig MeteringConfig;
    NKikimrConfig::TAuditConfig AuditConfig;
    NKikimrConfig::TCompactionConfig CompactionConfig;
    NKikimrConfig::TDomainsConfig DomainsConfig;
    NKikimrConfig::TBootstrap BootstrapConfig;
    NKikimrConfig::TAwsCompatibilityConfig AwsCompatibilityConfig;
    std::optional<NKikimrSharedCache::TSharedCacheConfig> SharedCacheConfig;
    bool EnforceUserTokenRequirement = false;
    bool AllowHugeKeyValueDeletes = true; // delete when all clients limit deletes per request
    bool EnableKqpSpilling = false;
    bool AllowShadowDataInSchemeShardForTests = false;
    bool EnableMvccSnapshotWithLegacyDomainRoot = false;
    bool UsePartitionStatsCollectorForTests = false;
    bool DisableCdcAutoSwitchingToReadyStateForTests = false;
    TVector<TString> AdministrationAllowedSIDs; // users/groups which allowed to perform administrative tasks
    TVector<TString> DefaultUserSIDs;
    TString AllAuthenticatedUsers = "all-users@well-known";
    TString TenantName;

    TResourceProfilesPtr ResourceProfiles;

    TProgramShouldContinue * const KikimrShouldContinue;
    bool EnableIntrospection = true;

    // Used to allow column families for testing
    bool AllowColumnFamiliesForTest = false;
    bool AllowPrivateTableDescribeForTest = false;

    // Used to allow immediate ReadTable in tests
    bool AllowReadTableImmediate = false;

    // Used to disable object deletion in schemeshard for cleanup tests
    bool DisableSchemeShardCleanupOnDropForTest = false;

    TMaybe<ui32> ZstdBlockSizeForTest;

    // Used to disable checking nodes with sys tablets only in cms
    bool  DisableCheckingSysNodesCms = false;

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
};

inline TAppData* AppData(TActorSystem* actorSystem) {
    Y_VERIFY_DEBUG(actorSystem);
    TAppData * const x = actorSystem->AppData<TAppData>();
    Y_VERIFY_DEBUG(x && x->Magic == TAppData::MagicTag);
    return x;
}

inline TAppData* AppData() {
    return AppData(TlsActivationContext->ExecutorThread.ActorSystem);
}

inline TAppData* AppData(const TActorContext &ctx) {
    return AppData(ctx.ActorSystem());
}

} // NKikimr
