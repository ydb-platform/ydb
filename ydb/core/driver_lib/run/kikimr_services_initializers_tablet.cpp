#include "auto_config_initializer.h"
#include "config_helpers.h"
#include "config.h"
#include "kikimr_services_initializers.h"
#include "service_initializer.h"

#include <ydb/core/actorlib_impl/destruct_actor.h>

#include <ydb/core/audit/audit_log_service.h>
#include <ydb/core/audit/audit_config/audit_config.h>
#include <ydb/core/audit/heartbeat_actor/heartbeat_actor.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/config_units.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/event_filter.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/feature_flags_service.h>
#include <ydb/core/base/hive.h>
#include <ydb/core/base/location.h>
#include <ydb/core/base/pool_stats_collector.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/base/tablet_pipecache.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/user_registry.h>

#include <ydb/core/blobstorage/backpressure/unisched.h>
#include <ydb/core/blobstorage/nodewarden/node_warden.h>
#include <ydb/core/blobstorage/other/mon_get_blob_page.h>
#include <ydb/core/blobstorage/ddisk/persistent_buffer_mon.h>
#include <ydb/core/blobstorage/vdisk/common/blobstorage_event_filter.h>

#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/client/server/grpc_proxy_status.h>
#include <ydb/core/client/server/msgbus_server.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/client/server/ic_nodes_cache_service.h>

#include <ydb/core/cms/cms.h>
#include <ydb/core/cms/console/configs_dispatcher.h>
#include <ydb/core/cms/console/configs_dispatcher_proxy.h>
#include <ydb/core/cms/console/configs_cache.h>
#include <ydb/core/cms/console/console.h>
#include <ydb/core/cms/console/feature_flags_configurator.h>
#include <ydb/core/cms/console/immediate_controls_configurator.h>
#include <ydb/core/cms/console/jaeger_tracing_configurator.h>
#include <ydb/core/cms/console/log_settings_configurator.h>
#include <ydb/core/cms/console/validators/core_validators.h>
#include <ydb/core/cms/http.h>

#include <ydb/core/control/immediate_control_board_actor.h>

#include <ydb/core/driver_lib/run/grpc_servers_manager.h>
#include <ydb/core/driver_lib/version/version.h>

#include <ydb/core/discovery/discovery.h>

#include <ydb/core/grpc_services/grpc_mon.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/db_metadata_cache.h>

#include <ydb/core/log_backend/log_backend.h>

#include <ydb/core/kesus/proxy/proxy.h>
#include <ydb/core/kesus/tablet/tablet.h>

#include <ydb/core/keyvalue/keyvalue.h>

#include <ydb/core/memory_controller/memory_controller.h>
#include <ydb/core/test_tablet/test_tablet.h>
#include <ydb/core/load_test/nbs_dbg_like_load_tablet.h>
#include <ydb/core/test_tablet/state_server_interface.h>

#include <ydb/core/blob_depot/blob_depot.h>

#include <ydb/core/counters_info/counters_info.h>
#include <ydb/core/health_check/health_check.h>


#include <ydb/core/kafka_proxy/actors/kafka_metrics_actor.h>
#include <ydb/core/kafka_proxy/actors/kafka_metadata_actor.h>
#include <ydb/core/kafka_proxy/kafka_metrics.h>
#include <ydb/core/kafka_proxy/kafka_proxy.h>
#include <ydb/core/kafka_proxy/kafka_transactions_coordinator.h>

#include <ydb/core/kqp/common/kqp.h>
#include <ydb/core/kqp/proxy_service/kqp_proxy_service.h>
#include <ydb/core/kqp/rm_service/kqp_rm_service.h>
#include <ydb/services/scheme_secret/service.h>
#include <ydb/core/kqp/compile_service/kqp_warmup_compile_actor.h>
#include <ydb/core/kqp/runtime/scheduler/kqp_compute_scheduler_service.h>

#include <ydb/core/load_test/service_actor.h>


#include <ydb/core/metering/metering.h>

#include <ydb/core/mind/address_classification/net_classifier.h>
#include <ydb/core/mind/bscontroller/bsc.h>
#include <ydb/core/mind/configured_tablet_bootstrapper.h>
#include <ydb/core/mind/dynamic_nameserver.h>
#include <ydb/core/mind/labels_maintainer.h>
#include <ydb/core/mind/lease_holder.h>
#include <ydb/core/mind/node_broker.h>
#include <ydb/core/mind/tenant_node_enumeration.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/mind/tenant_slot_broker.h>

#if defined(YDB_EMBEDDED_NBS_ENABLED)
#include <ydb/core/nbs/cloud/blockstore/bootstrap/bootstrap.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/ss_proxy/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/config/protos/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/volume/volume.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct.h>
#endif

#include <ydb/core/mon/mon.h>
#include <ydb/core/mon_alloc/monitor.h>
#include <ydb/core/mon_alloc/profiler.h>
#include <ydb/core/mon_alloc/stats.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/core/persqueue/pq.h>
#include <ydb/core/persqueue/deferred_publish/registry_actor.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/node_limits.pb.h>
#include <ydb/core/protos/compile_service_config.pb.h>
#include <ydb/core/protos/memory_controller_config.pb.h>

#include <ydb/core/public_http/http_service.h>

#include <ydb/core/quoter/quoter_service.h>

#include <ydb/core/raw_socket/sock_ssl.h>
#include <ydb/core/raw_socket/sock64.h>

#include <ydb/core/scheme/scheme_type_registry.h>

#include <ydb/core/security/ticket_parser.h>
#include <ydb/core/security/ldap_auth_provider/ldap_auth_provider.h>
#include <ydb/core/security/token_manager/token_manager.h>
#include <ydb/core/security/ticket_parser_settings.h>
#include <ydb/core/security/token_manager/token_manager_settings.h>

#include <ydb/core/sys_view/processor/processor.h>
#include <ydb/core/sys_view/service/sysview_service.h>
#include <ydb/core/statistics/service/service.h>
#include <ydb/core/statistics/aggregator/aggregator.h>

#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet/node_tablet_monitor.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>
#include <ydb/core/tablet/tablet_list_renderer.h>
#include <ydb/core/tablet/tablet_monitoring_proxy.h>
#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/core/tracing/tablet_info.h>

#include <ydb/core/tx/coordinator/coordinator.h>
#include <ydb/core/tx/columnshard/blob_cache.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/columnshard/columnshard.h>
#include <ydb/core/tx/columnshard/overload_manager/overload_manager_service.h>
#include <ydb/core/tx/mediator/mediator.h>
#include <ydb/core/tx/replication/controller/controller.h>
#include <ydb/core/tx/replication/service/service.h>
#include <ydb/core/tx/scheme_board/scheme_board.h>
#include <ydb/core/tx/schemeshard/schemeshard.h>
#include <ydb/core/tx/sequenceproxy/sequenceproxy.h>
#include <ydb/core/tx/sequenceshard/sequenceshard.h>
#include <ydb/core/tx/time_cast/time_cast.h>
#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/tx_allocator/txallocator.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/long_tx_service/public/events.h>
#include <ydb/core/tx/long_tx_service/long_tx_service.h>

#include <ydb/core/util/failure_injection.h>
#include <ydb/core/util/memory_tracker.h>
#include <ydb/core/util/sig.h>

#include <ydb/core/viewer/viewer.h>

#include <ydb/library/aws_init/aws.h>

#include <ydb/public/lib/deprecated/client/msgbus_client.h>

#include <ydb/core/ymq/actor/serviceid.h>

#include <ydb/core/fq/libs/checkpoint_storage/storage_service.h>
#include <ydb/core/fq/libs/init/init.h>
#include <ydb/core/fq/libs/logs/log.h>

#include <ydb/library/folder_service/folder_service.h>
#include <ydb/library/folder_service/proto/config.pb.h>

#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <yql/essentials/minikql/comp_nodes/mkql_factories.h>
#include <yql/essentials/parser/pg_wrapper/interface/comp_factory.h>
#include <ydb/library/yql/utils/actor_log/log.h>

#include <ydb/services/metadata/ds_table/service.h>
#include <ydb/services/metadata/service.h>

#include <ydb/core/tx/conveyor/service/service.h>
#include <ydb/core/tx/conveyor/usage/config.h>
#include <ydb/core/tx/conveyor/usage/service.h>
#include <ydb/core/tx/conveyor_composite/service/service.h>
#include <ydb/core/tx/conveyor_composite/usage/config.h>
#include <ydb/core/tx/conveyor_composite/usage/service.h>
#include <ydb/core/tx/columnshard/data_accessor/cache_policy/policy.h>
#include <ydb/core/tx/columnshard/column_fetching/cache_policy.h>
#include <ydb/core/tx/general_cache/usage/service.h>
#include <ydb/core/tx/priorities/usage/config.h>
#include <ydb/core/tx/priorities/usage/service.h>

#include <ydb/core/tx/limiter/grouped_memory/usage/config.h>
#include <ydb/core/tx/limiter/grouped_memory/usage/service.h>

#include <ydb/core/backup/controller/tablet.h>

#include <ydb/services/udf_store/service.h>

#include <ydb/library/actors/protos/services_common.pb.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/actorsystem.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/log_settings.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/mon_stats.h>
#include <ydb/library/actors/core/probes.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/core/scheduler_basic.h>
#include <ydb/library/actors/core/io_dispatcher.h>
#include <ydb/library/actors/dnsresolver/dnsresolver.h>
#include <ydb/library/actors/helpers/selfping_actor.h>
#include <ydb/library/actors/http/http_proxy.h>
#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_metrics_aggregator.h>
#include <ydb/library/actors/interconnect/interconnect_mon.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_proxy_wrapper.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/handshake_broker.h>
#include <ydb/library/actors/interconnect/load.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/actors/interconnect/poller/poller_tcp.h>
#include <ydb/library/actors/interconnect/poller/uring_poller_actor.h>
#include <ydb/library/actors/interconnect/rdma/cq_actor/cq_actor.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/interconnect/rdma/rdma.h>
#include <ydb/core/retro_tracing_impl/distributed_collector/distributed_retro_collector.h>
#include <ydb/library/actors/retro_tracing/collector/retro_collector.h>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>
#include <ydb/library/slide_limiter/service/service.h>
#include <ydb/library/slide_limiter/usage/config.h>
#include <ydb/library/slide_limiter/usage/service.h>

#include <ydb/core/graph/api/service.h>
#include <ydb/core/graph/api/shard.h>

#include <library/cpp/containers/absl/btree_set.h>
#include <library/cpp/logger/global/global.h>
#include <library/cpp/logger/log.h>

#include <library/cpp/monlib/messagebus/mon_messagebus.h>

#include <library/cpp/svnversion/svnversion.h>

#include <library/cpp/lwtrace/mon/mon_lwtrace.h>

#include <util/digest/city.h>
#include <util/generic/algorithm.h>
#include <util/generic/size_literals.h>

#include <util/system/hostname.h>

namespace NKikimr::NKikimrServicesInitializers {
TTabletResolverInitializer::TTabletResolverInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TTabletResolverInitializer::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData) {
    TIntrusivePtr<TTabletResolverConfig> tabletResolverConfig(new TTabletResolverConfig());
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeTabletResolverID(), TActorSetupCmd(CreateTabletResolver(tabletResolverConfig), TMailboxType::ReadAsFilled, appData->SystemPoolId)));

}

// TTabletPipePerNodeCachesInitializer

TTabletPipePerNodeCachesInitializer::TTabletPipePerNodeCachesInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TTabletPipePerNodeCachesInitializer::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData)
{
    auto counters = GetServiceCounters(appData->Counters, "tablets");

    TIntrusivePtr<TPipePerNodeCacheConfig> leaderPipeConfig = new TPipePerNodeCacheConfig();
    leaderPipeConfig->PipeRefreshTime = TDuration::Zero();
    leaderPipeConfig->Counters = counters->GetSubgroup("type", "LEADER_PIPE_CACHE");

    TIntrusivePtr<TPipePerNodeCacheConfig> followerPipeConfig = new TPipePerNodeCacheConfig();
    followerPipeConfig->PipeRefreshTime = TDuration::Seconds(30);
    followerPipeConfig->PipeConfig.AllowFollower = true;
    followerPipeConfig->Counters = counters->GetSubgroup("type", "FOLLOWER_PIPE_CACHE");

    TIntrusivePtr<TPipePerNodeCacheConfig> persistentPipeConfig = new TPipePerNodeCacheConfig();
    persistentPipeConfig->PipeRefreshTime = TDuration::Zero();
    persistentPipeConfig->PipeConfig = TPipePerNodeCacheConfig::DefaultPersistentPipeConfig();
    persistentPipeConfig->Counters = counters->GetSubgroup("type", "PERSISTENT_PIPE_CACHE");

    setup->LocalServices.emplace_back(
        MakePipePerNodeCacheID(false),
        TActorSetupCmd(CreatePipePerNodeCache(leaderPipeConfig), TMailboxType::ReadAsFilled, appData->UserPoolId));
    setup->LocalServices.emplace_back(
        MakePipePerNodeCacheID(true),
        TActorSetupCmd(CreatePipePerNodeCache(followerPipeConfig), TMailboxType::ReadAsFilled, appData->UserPoolId));
    setup->LocalServices.emplace_back(
        MakePipePerNodeCacheID(EPipePerNodeCache::Persistent),
        TActorSetupCmd(CreatePipePerNodeCache(persistentPipeConfig), TMailboxType::ReadAsFilled, appData->UserPoolId));
}

// TTabletMonitoringProxyInitializer

TTabletMonitoringProxyInitializer::TTabletMonitoringProxyInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TTabletMonitoringProxyInitializer::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData) {
    NTabletMonitoringProxy::TTabletMonitoringProxyConfig proxyConfig;
    proxyConfig.SetRetryLimitCount(Config.GetMonitoringConfig().GetTabletMonitoringRetries());

    TActorSetupCmd tabletMonitoringProxySetup(NTabletMonitoringProxy::CreateTabletMonitoringProxy(std::move(proxyConfig)), TMailboxType::ReadAsFilled, appData->UserPoolId);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(NTabletMonitoringProxy::MakeTabletMonitoringProxyID(), std::move(tabletMonitoringProxySetup)));

}

// TTabletCountersAggregatorInitializer

TTabletCountersAggregatorInitializer::TTabletCountersAggregatorInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TTabletCountersAggregatorInitializer::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData) {
    {
        TActorSetupCmd tabletCountersAggregatorSetup(CreateTabletCountersAggregator(false), TMailboxType::ReadAsFilled, appData->UserPoolId);
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeTabletCountersAggregatorID(NodeId, false), std::move(tabletCountersAggregatorSetup)));
    }
    {
        TActorSetupCmd tabletCountersAggregatorSetup(CreateTabletCountersAggregator(true), TMailboxType::ReadAsFilled, appData->UserPoolId);
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeTabletCountersAggregatorID(NodeId, true), std::move(tabletCountersAggregatorSetup)));
    }
}

//TGRpcProxyStatusInitializer

TGRpcProxyStatusInitializer::TGRpcProxyStatusInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TGRpcProxyStatusInitializer::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData) {
    TActorSetupCmd gRpcProxyStatusSetup(CreateGRpcProxyStatus(), TMailboxType::ReadAsFilled, appData->UserPoolId);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeGRpcProxyStatusID(NodeId), std::move(gRpcProxyStatusSetup)));

}


// This code is shared between default kikimr bootstrapper and alternative bootstrapper

static TIntrusivePtr<TTabletSetupInfo> CreateTablet(
    const TString& typeName,
    const TIntrusivePtr<TTabletStorageInfo>& tabletInfo,
    const TAppData* appData,
    const TIntrusivePtr<ITabletFactory>& customTablets = nullptr)
{
    TIntrusivePtr<TTabletSetupInfo> tabletSetup;
    if (customTablets) {
        tabletSetup = customTablets->CreateTablet(typeName, tabletInfo, *appData);
        if (tabletSetup) {
            return tabletSetup;
        }
    }

    TTabletTypes::EType tabletType = TTabletTypes::StrToType(typeName);

    const ui32 workPoolId = SelectTabletWorkPoolId(tabletType, appData);

    tabletSetup = MakeTabletSetupInfo(tabletType, tabletInfo->BootType, workPoolId, appData->SystemPoolId);

    if (tabletInfo->TabletType == TTabletTypes::TypeInvalid) {
        tabletInfo->TabletType = tabletType;
    }

    return tabletSetup;
}

// TBootstrapperInitializer

TBootstrapperInitializer::TBootstrapperInitializer(
        const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TBootstrapperInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {
    if (!Config.HasBootstrapConfig()) {
        return;
    }

    if (appData->FeatureFlags.GetEnableConfiguredBootstrapper()) {
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            TActorId(),
            TActorSetupCmd(CreateConfiguredTabletBootstrapper(Config.GetBootstrapConfig(), /* manageAllTablets */ true), TMailboxType::HTSwap, appData->SystemPoolId)));
        return;
    }

    NKikimrConfig::TBootstrap dynamicConfig;
    dynamicConfig.CopyFrom(Config.GetBootstrapConfig());
    dynamicConfig.ClearTablet();

    for (const auto &boot : Config.GetBootstrapConfig().GetTablet()) {
        if (boot.GetAllowDynamicConfiguration()) {
            dynamicConfig.AddTablet()->CopyFrom(boot);
        } else if (Find(boot.GetNode(), NodeId) != boot.GetNode().end()) {
            setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
                MakeBootstrapperID(boot.GetInfo().GetTabletID(), NodeId),
                TActorSetupCmd(CreateTabletBootstrapper(boot, appData), TMailboxType::HTSwap, appData->SystemPoolId)));
        }
    }

    if (dynamicConfig.TabletSize()) {
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            TActorId(),
            TActorSetupCmd(CreateConfiguredTabletBootstrapper(dynamicConfig, /* manageAllTablets */ false), TMailboxType::HTSwap, appData->SystemPoolId)));
    }
}

// alternative bootstrapper

TTabletsInitializer::TTabletsInitializer(
        const TKikimrRunConfig& runConfig,
        TIntrusivePtr<ITabletFactory> customTablets)
    : IKikimrServicesInitializer(runConfig)
    , CustomTablets(customTablets)
{
}

void TTabletsInitializer::InitializeServices(
        TActorSystemSetup* setup,
        const TAppData* appData)
{
    if (!Config.HasTabletsConfig() || Config.GetTabletsConfig().TabletSize() == 0) {
        return;
    }

    for (const auto& tabletConfig: Config.GetTabletsConfig().GetTablet()) {
        for (ui32 bootstrapperNode: tabletConfig.GetNode()) {
            if (bootstrapperNode == setup->NodeId) {
                auto tabletInfo = TabletStorageInfoFromProto(tabletConfig.GetInfo());

                auto tabletType = tabletConfig.GetType();
                auto tabletSetup = CreateTablet(tabletType, tabletInfo, appData, CustomTablets);
                if (!tabletSetup) {
                    ythrow yexception()
                        << "unknown tablet type: " << tabletConfig.GetType();
                }

                setup->LocalServices.push_back(std::make_pair(
                    MakeBootstrapperID(tabletInfo->TabletID, bootstrapperNode),
                    TActorSetupCmd(
                        CreateBootstrapper(
                            tabletInfo.Get(),
                            new TBootstrapperInfo(tabletSetup.Get()),
                            tabletConfig.GetStandBy()),
                        TMailboxType::ReadAsFilled,
                        appData->SystemPoolId)));
            }
        }
    }
}

// TMediatorTimeCastProxyInitializer

TMediatorTimeCastProxyInitializer::TMediatorTimeCastProxyInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TMediatorTimeCastProxyInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            MakeMediatorTimecastProxyID(),
            TActorSetupCmd(CreateMediatorTimecastProxy(), TMailboxType::ReadAsFilled, appData->SystemPoolId)));
}

// TMiniKQLCompileServiceInitializer

TMiniKQLCompileServiceInitializer::TMiniKQLCompileServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TMiniKQLCompileServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                           const NKikimr::TAppData* appData) {
    const auto compileInFlight = Config.GetBootstrapConfig().GetCompileServiceConfig().GetInflightLimit();
    IActor* compileService = CreateMiniKQLCompileService(compileInFlight);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeMiniKQLCompileServiceID(),
                                                                       TActorSetupCmd(compileService,
                                                                                      TMailboxType::ReadAsFilled,
                                                                                      appData->UserPoolId)));
}

// TMessageBusServicesInitializer

TMessageBusServicesInitializer::TMessageBusServicesInitializer(const TKikimrRunConfig& runConfig,
                                                               NMsgBusProxy::IMessageBusServer& busServer)
    : IKikimrServicesInitializer(runConfig)
    , BusServer(busServer) {
}

void TMessageBusServicesInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                        const NKikimr::TAppData* appData) {
    if (!IsServiceInitialized(setup, NMsgBusProxy::CreateMsgBusProxyId())
        && Config.HasMessageBusConfig() && Config.GetMessageBusConfig().GetStartBusProxy()) {
        if (IActor *proxy = BusServer.CreateProxy()) {
            setup->LocalServices.emplace_back(NMsgBusProxy::CreateMsgBusProxyId(),
                                                         TActorSetupCmd(proxy, TMailboxType::ReadAsFilled, appData->UserPoolId));

            if (appData->PQConfig.GetEnabled()) {
                setup->LocalServices.emplace_back(
                        NMsgBusProxy::CreatePersQueueMetaCacheV2Id(),
                        TActorSetupCmd(
                                NMsgBusProxy::NPqMetaCacheV2::CreatePQMetaCache(appData->Counters),
                                TMailboxType::ReadAsFilled, appData->UserPoolId
                        )
                );
            }
        }
    }
}

// TSecurityServicesInitializer

TSecurityServicesInitializer::TSecurityServicesInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories)
    : IKikimrServicesInitializer(runConfig)
    , Factories(factories)
{
}

void TSecurityServicesInitializer::InitializeTokenManager(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    const auto& authConfig = appData->AuthConfig;
    if (!IsServiceInitialized(setup, MakeTokenManagerID()) && authConfig.GetTokenManager().GetEnable()) {
        IActor* tokenManager = nullptr;
        TTokenManagerSettings settings {
            .Config = Config.GetAuthConfig().GetTokenManager(),
            .HttpProxyId = {}
        };
        if (Factories && Factories->CreateTokenManager) {
            tokenManager = Factories->CreateTokenManager(settings);
        } else {
            tokenManager = CreateTokenManager(settings);
        }
        if (tokenManager) {
            setup->LocalServices.push_back(std::make_pair<TActorId, TActorSetupCmd>(MakeTokenManagerID(), TActorSetupCmd(tokenManager, TMailboxType::HTSwap, appData->UserPoolId)));
        }
    }
}

void TSecurityServicesInitializer::InitializeLdapAuthProvider(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    const auto& authConfig = appData->AuthConfig;
    if (!IsServiceInitialized(setup, MakeLdapAuthProviderID()) && authConfig.HasLdapAuthentication()) {
        IActor* ldapAuthProvider = CreateLdapAuthProvider(authConfig.GetLdapAuthentication());
        if (ldapAuthProvider) {
            setup->LocalServices.push_back(std::make_pair<TActorId, TActorSetupCmd>(MakeLdapAuthProviderID(), TActorSetupCmd(ldapAuthProvider, TMailboxType::HTSwap, appData->UserPoolId)));
        }
    }
}

void TSecurityServicesInitializer::InitializeTicketParser(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    if (!IsServiceInitialized(setup, MakeTicketParserID())) {
        IActor* ticketParser = nullptr;
        auto grpcConfig = Config.GetGRpcConfig();
        TTicketParserSettings settings {
            .AuthConfig = Config.GetAuthConfig(),
            .CertificateAuthValues = {
                .ClientCertificateAuthorization = Config.GetClientCertificateAuthorization(),
                .ServerCertificateFilePath = grpcConfig.HasPathToCertificateFile() ? grpcConfig.GetPathToCertificateFile() : grpcConfig.GetCert(),
                .Domain = Config.GetAuthConfig().GetCertificateAuthenticationDomain()
            }
        };
        if (Factories && Factories->CreateTicketParser) {
            ticketParser = Factories->CreateTicketParser(settings);
        } else {
            ticketParser = CreateTicketParser(settings);
        }
        if (ticketParser) {
            setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeTicketParserID(),
                TActorSetupCmd(ticketParser, TMailboxType::HTSwap, appData->UserPoolId)));
        }
    }
}

void TSecurityServicesInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                      const NKikimr::TAppData* appData) {
    InitializeTokenManager(setup, appData);
    InitializeLdapAuthProvider(setup, appData);
    InitializeTicketParser(setup, appData);
}

// TGRpcServicesInitializer


} // namespace NKikimr::NKikimrServicesInitializers
