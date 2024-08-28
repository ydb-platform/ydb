#include "appdata.h"
#include "tablet_types.h"

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
#include <ydb/core/protos/bootstrap.pb.h>
#include <ydb/core/protos/blobstorage.pb.h>
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/key.pb.h>
#include <ydb/core/protos/memory_controller_config.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
#include <ydb/core/protos/replication.pb.h>
#include <ydb/core/protos/stream.pb.h>
#include <ydb/core/protos/netclassifier.pb.h>
#include <ydb/core/protos/datashard_config.pb.h>
#include <ydb/core/protos/shared_cache.pb.h>
#include <ydb/library/pdisk_io/aio.h>

#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <ydb/library/actors/core/executor_thread.h>
#include <ydb/library/actors/core/monotonic_provider.h>
#include <ydb/library/actors/util/should_continue.h>
#include <library/cpp/random_provider/random_provider.h>
#include <library/cpp/time_provider/time_provider.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {

struct TAppData::TImpl {
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
    NKikimrConfig::TS3ProxyResolverConfig S3ProxyResolverConfig;
    NKikimrConfig::TBackgroundCleaningConfig BackgroundCleaningConfig;
    NKikimrConfig::TGraphConfig GraphConfig;
    NKikimrSharedCache::TSharedCacheConfig SharedCacheConfig;
    NKikimrConfig::TMetadataCacheConfig MetadataCacheConfig;
    NKikimrConfig::TMemoryControllerConfig MemoryControllerConfig;
    NKikimrReplication::TReplicationDefaults ReplicationConfig;
};

TAppData::TAppData(
        ui32 sysPoolId, ui32 userPoolId, ui32 ioPoolId, ui32 batchPoolId,
        TMap<TString, ui32> servicePools,
        const NScheme::TTypeRegistry* typeRegistry,
        const NMiniKQL::IFunctionRegistry* functionRegistry,
        const TFormatFactory* formatFactory,
        TProgramShouldContinue *kikimrShouldContinue)
    : Magic(MagicTag)
    , Impl(new TImpl)
    , SystemPoolId(sysPoolId)
    , UserPoolId(userPoolId)
    , IOPoolId(ioPoolId)
    , BatchPoolId(batchPoolId)
    , ServicePools(servicePools)
    , TypeRegistry(typeRegistry)
    , FunctionRegistry(functionRegistry)
    , FormatFactory(formatFactory)
    , MonotonicTimeProvider(CreateDefaultMonotonicTimeProvider())
    , ProxySchemeCacheNodes(Max<ui64>() / 4)
    , ProxySchemeCacheDistrNodes(Max<ui64>() / 4)
    , CompilerSchemeCachePaths(Max<ui64>() / 4)
    , CompilerSchemeCacheTables(Max<ui64>() / 4)
    , Mon(nullptr)
    , Icb(new TControlBoard())
    , InFlightLimiterRegistry(new NGRpcService::TInFlightLimiterRegistry(Icb))
    , StreamingConfig(Impl->StreamingConfig)
    , PQConfig(Impl->PQConfig)
    , PQClusterDiscoveryConfig(Impl->PQClusterDiscoveryConfig)
    , NetClassifierConfig(Impl->NetClassifierConfig)
    , NetClassifierDistributableConfig(Impl->NetClassifierDistributableConfig)
    , SqsConfig(Impl->SqsConfig)
    , AuthConfig(Impl->AuthConfig)
    , KeyConfig(Impl->KeyConfig)
    , PDiskKeyConfig(Impl->PDiskKeyConfig)
    , FeatureFlags(Impl->FeatureFlags)
    , HiveConfig(Impl->HiveConfig)
    , DataShardConfig(Impl->DataShardConfig)
    , ColumnShardConfig(Impl->ColumnShardConfig)
    , SchemeShardConfig(Impl->SchemeShardConfig)
    , MeteringConfig(Impl->MeteringConfig)
    , AuditConfig(Impl->AuditConfig)
    , CompactionConfig(Impl->CompactionConfig)
    , DomainsConfig(Impl->DomainsConfig)
    , BootstrapConfig(Impl->BootstrapConfig)
    , AwsCompatibilityConfig(Impl->AwsCompatibilityConfig)
    , S3ProxyResolverConfig(Impl->S3ProxyResolverConfig)
    , BackgroundCleaningConfig(Impl->BackgroundCleaningConfig)
    , GraphConfig(Impl->GraphConfig)
    , SharedCacheConfig(Impl->SharedCacheConfig)
    , MetadataCacheConfig(Impl->MetadataCacheConfig)
    , MemoryControllerConfig(Impl->MemoryControllerConfig)
    , ReplicationConfig(Impl->ReplicationConfig)
    , KikimrShouldContinue(kikimrShouldContinue)
{}

TAppData::~TAppData()
{}

TIntrusivePtr<IRandomProvider> TAppData::RandomProvider = CreateDefaultRandomProvider();
TIntrusivePtr<ITimeProvider> TAppData::TimeProvider = CreateDefaultTimeProvider();

}
