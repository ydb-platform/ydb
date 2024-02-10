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
#include <ydb/core/protos/cms.pb.h>
#include <ydb/core/protos/config.pb.h>
#include <ydb/core/protos/key.pb.h>
#include <ydb/core/protos/pqconfig.pb.h>
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

TAppData::TAppData(
        ui32 sysPoolId, ui32 userPoolId, ui32 ioPoolId, ui32 batchPoolId,
        TMap<TString, ui32> servicePools,
        const NScheme::TTypeRegistry* typeRegistry,
        const NMiniKQL::IFunctionRegistry* functionRegistry,
        const TFormatFactory* formatFactory,
        TProgramShouldContinue *kikimrShouldContinue)
    : Magic(MagicTag)
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
    , StreamingConfigPtr(new NKikimrStream::TStreamingConfig())
    , PQConfigPtr(new NKikimrPQ::TPQConfig())
    , PQClusterDiscoveryConfigPtr(new NKikimrPQ::TPQClusterDiscoveryConfig())
    , NetClassifierConfigPtr(new NKikimrNetClassifier::TNetClassifierConfig())
    , NetClassifierDistributableConfigPtr(new NKikimrNetClassifier::TNetClassifierDistributableConfig())
    , SqsConfigPtr(new NKikimrConfig::TSqsConfig())
    , AuthConfigPtr(new NKikimrProto::TAuthConfig())
    , KeyConfigPtr(new NKikimrProto::TKeyConfig())
    , PDiskKeyConfigPtr(new NKikimrProto::TKeyConfig())
    , FeatureFlagsPtr(new TFeatureFlags())
    , HiveConfigPtr(new NKikimrConfig::THiveConfig())
    , DataShardConfigPtr(new NKikimrConfig::TDataShardConfig())
    , ColumnShardConfigPtr(new NKikimrConfig::TColumnShardConfig())
    , SchemeShardConfigPtr(new NKikimrConfig::TSchemeShardConfig())
    , MeteringConfigPtr(new NKikimrConfig::TMeteringConfig())
    , AuditConfigPtr(new NKikimrConfig::TAuditConfig())
    , CompactionConfigPtr(new NKikimrConfig::TCompactionConfig())
    , DomainsConfigPtr(new NKikimrConfig::TDomainsConfig())
    , BootstrapConfigPtr(new NKikimrConfig::TBootstrap())
    , AwsCompatibilityConfigPtr(new NKikimrConfig::TAwsCompatibilityConfig())
    , S3ProxyResolverConfigPtr(new NKikimrConfig::TS3ProxyResolverConfig())
    , GraphConfigPtr(new NKikimrConfig::TGraphConfig())
    , BackgroundCleaningConfigPtr(new NKikimrConfig::TBackgroundCleaningConfig())
    , StreamingConfig(*StreamingConfigPtr.get())
    , PQConfig(*PQConfigPtr.get())
    , PQClusterDiscoveryConfig(*PQClusterDiscoveryConfigPtr.get())
    , NetClassifierConfig(*NetClassifierConfigPtr.get())
    , NetClassifierDistributableConfig(*NetClassifierDistributableConfigPtr.get())
    , SqsConfig(*SqsConfigPtr.get())
    , AuthConfig(*AuthConfigPtr.get())
    , KeyConfig(*KeyConfigPtr.get())
    , PDiskKeyConfig(*PDiskKeyConfigPtr.get())
    , FeatureFlags(*FeatureFlagsPtr.get())
    , HiveConfig(*HiveConfigPtr.get())
    , DataShardConfig(*DataShardConfigPtr.get())
    , ColumnShardConfig(*ColumnShardConfigPtr.get())
    , SchemeShardConfig(*SchemeShardConfigPtr.get())
    , MeteringConfig(*MeteringConfigPtr.get())
    , AuditConfig(*AuditConfigPtr.get())
    , CompactionConfig(*CompactionConfigPtr.get())
    , DomainsConfig(*DomainsConfigPtr.get())
    , BootstrapConfig(*BootstrapConfigPtr.get())
    , AwsCompatibilityConfig(*AwsCompatibilityConfigPtr.get())
    , S3ProxyResolverConfig(*S3ProxyResolverConfigPtr.get())
    , BackgroundCleaningConfig(*BackgroundCleaningConfigPtr.get())
    , GraphConfig(*GraphConfigPtr.get())
    , KikimrShouldContinue(kikimrShouldContinue)

{}

TIntrusivePtr<IRandomProvider> TAppData::RandomProvider = CreateDefaultRandomProvider();
TIntrusivePtr<ITimeProvider> TAppData::TimeProvider = CreateDefaultTimeProvider();

}
