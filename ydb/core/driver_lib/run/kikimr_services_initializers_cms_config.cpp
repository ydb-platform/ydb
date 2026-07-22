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
TCmsServiceInitializer::TCmsServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TCmsServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                const NKikimr::TAppData* appData)
{
    auto http = NCms::CreateCmsHttp();
    setup->LocalServices.emplace_back(TActorId(),
                                      TActorSetupCmd(http, TMailboxType::HTSwap, appData->UserPoolId));
}

TTxProxyInitializer::TTxProxyInitializer(const TKikimrRunConfig &runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

TVector<ui64> TTxProxyInitializer::CollectAllAllocatorsFromAllDomains(const TAppData *appData) {
    TVector<ui64> allocators;
    if (const auto& domain = appData->DomainsInfo->Domain) {
        for (auto tabletId: domain->TxAllocators) {
            allocators.push_back(tabletId);
        }
    }
    return allocators;
}

void TTxProxyInitializer::InitializeServices(TActorSystemSetup *setup,
                                             const TAppData *appData)
{
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            MakeTxProxyID(),
            TActorSetupCmd(CreateTxProxy(CollectAllAllocatorsFromAllDomains(appData)), TMailboxType::ReadAsFilled, appData->UserPoolId)));
}

TLongTxServiceInitializer::TLongTxServiceInitializer(const TKikimrRunConfig &runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TLongTxServiceInitializer::InitializeServices(TActorSystemSetup *setup,
                                                   const TAppData *appData)
{
    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> longTxGroup = tabletGroup->GetSubgroup("type", "LONG_TX");

    auto counters = MakeIntrusive<NLongTxService::TLongTxServiceCounters>(longTxGroup);

    NLongTxService::TLongTxServiceSettings settings{
        .Counters = counters,
    };

    auto* actor = NLongTxService::CreateLongTxService(settings);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NLongTxService::MakeLongTxServiceID(NodeId),
            TActorSetupCmd(actor, TMailboxType::ReadAsFilled, appData->UserPoolId)));
}

TSequenceProxyServiceInitializer::TSequenceProxyServiceInitializer(const TKikimrRunConfig &runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TSequenceProxyServiceInitializer::InitializeServices(TActorSystemSetup *setup,
                                                   const TAppData *appData)
{
    auto* actor = NSequenceProxy::CreateSequenceProxy();
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NSequenceProxy::MakeSequenceProxyServiceID(),
            TActorSetupCmd(actor, TMailboxType::ReadAsFilled, appData->UserPoolId)));
}

TLeaseHolderInitializer::TLeaseHolderInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TLeaseHolderInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                const NKikimr::TAppData* appData)
{
    // Lease holder is required for dynamic nodes only.
    if (Config.GetDynamicNodeConfig().HasNodeInfo()) {
        TInstant expire = TInstant::MicroSeconds(Config.GetDynamicNodeConfig().GetNodeInfo().GetExpire());
        auto holder = NNodeBroker::CreateLeaseHolder(expire);
        setup->LocalServices.emplace_back(TActorId(),
                                          TActorSetupCmd(holder, TMailboxType::HTSwap, appData->UserPoolId));
    }
}

TSqsServiceInitializer::TSqsServiceInitializer(const TKikimrRunConfig& runConfig, const std::shared_ptr<TModuleFactories>& factories)
    : IKikimrServicesInitializer(runConfig)
    , Factories(factories)
{
}

void TSqsServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    if (Config.GetSqsConfig().GetEnableSqs()) {
        {
            IActor* actor = NSQS::CreateSqsService();
            setup->LocalServices.emplace_back(
                NSQS::MakeSqsServiceID(NodeId),
                TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId));
        }

        {
            IActor* actor = NSQS::CreateSqsProxyService();
            setup->LocalServices.emplace_back(
                NSQS::MakeSqsProxyServiceID(NodeId),
                TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId));
        }

        Factories->SqsAuthFactory->Initialize(
            setup->LocalServices, *appData, Config.GetSqsConfig());
    }
}


THttpProxyServiceInitializer::THttpProxyServiceInitializer(const TKikimrRunConfig& runConfig, const std::shared_ptr<TModuleFactories>& factories)
    : IKikimrServicesInitializer(runConfig)
    , Factories(factories)
{
}

void THttpProxyServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    Factories->DataStreamsAuthFactory->Initialize(
        setup->LocalServices, *appData, Config.GetHttpProxyConfig(), Config.GetGRpcConfig());
}


TConfigsDispatcherInitializer::TConfigsDispatcherInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
    , ConfigsDispatcherInitInfo(runConfig.ConfigsDispatcherInitInfo)
{
}

void TConfigsDispatcherInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    IActor* actor = NConsole::CreateConfigsDispatcher(ConfigsDispatcherInitInfo);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NConsole::MakeConfigsDispatcherID(NodeId),
            TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId)));

    IActor* proxyActor = NConsole::CreateConfigsDispatcherProxy();
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NConsole::MakeConfigsDispatcherProxyID(NodeId),
            TActorSetupCmd(proxyActor, TMailboxType::HTSwap, appData->UserPoolId)));

    setup->LocalServices.emplace_back(
        MakeFeatureFlagsServiceID(),
        TActorSetupCmd(NConsole::CreateFeatureFlagsConfigurator(), TMailboxType::HTSwap, appData->UserPoolId));
}

TConfigsCacheInitializer::TConfigsCacheInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
    , PathToConfigCacheFile(runConfig.PathToConfigCacheFile)
{
}

void TConfigsCacheInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    if (PathToConfigCacheFile && appData->FeatureFlags.GetEnableConfigurationCache()) {
        IActor* actor = NConsole::CreateConfigsCacheActor(PathToConfigCacheFile);
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            NConsole::MakeConfigsCacheActorID(NodeId),
            TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

// TTabletInfoInitializer

TTabletInfoInitializer::TTabletInfoInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TTabletInfoInitializer::InitializeServices(
    NActors::TActorSystemSetup* setup,
    const NKikimr::TAppData* appData) {
    TActorSetupCmd tabletInfoSetup(NTabletInfo::CreateTabletInfo(), TMailboxType::ReadAsFilled, appData->UserPoolId);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(NTabletInfo::MakeTabletInfoID(), std::move(tabletInfoSetup)));
}

TConfigValidatorsInitializer::TConfigValidatorsInitializer(const TKikimrRunConfig& runConfig)
   : IKikimrServicesInitializer(runConfig)
{
}

void TConfigValidatorsInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    Y_UNUSED(setup);
    Y_UNUSED(appData);
    NConsole::RegisterCoreValidators();
}

TSysViewServiceInitializer::TSysViewServiceInitializer(const TKikimrRunConfig& runConfig)
   : IKikimrServicesInitializer(runConfig)
{
}

void TSysViewServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NSysView::TExtCountersConfig config;
    for (ui32 i = 0; i < setup->GetExecutorsCount(); ++i) {
        config.Pools.push_back(NSysView::TExtCountersConfig::TPool{
            setup->GetPoolName(i),
            setup->GetThreads(i)});
    }

    // external counters only for dynamic nodes
    bool hasExternalCounters = Config.GetDynamicNodeConfig().HasNodeInfo();

    auto actor = NSysView::CreateSysViewService(std::move(config), hasExternalCounters);

    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        NSysView::MakeSysViewServiceID(NodeId),
        TActorSetupCmd(actor.Release(), TMailboxType::HTSwap, appData->UserPoolId)));
}

TStatServiceInitializer::TStatServiceInitializer(const TKikimrRunConfig& runConfig)
   : IKikimrServicesInitializer(runConfig)
{
}

void TStatServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    auto actor = NStat::CreateStatService();

    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        NStat::MakeStatServiceID(NodeId),
        TActorSetupCmd(actor.Release(), TMailboxType::HTSwap, appData->UserPoolId)));
}

TMeteringWriterInitializer::TMeteringWriterInitializer(const TKikimrRunConfig &runConfig)
    : IKikimrServicesInitializer(runConfig)
    , KikimrRunConfig(runConfig)
{
}

void TMeteringWriterInitializer::InitializeServices(TActorSystemSetup* setup, const TAppData* appData)
{
    auto fileBackend = CreateMeteringLogBackendWithUnifiedAgent(KikimrRunConfig, appData->Counters);
    if (!fileBackend)
            return;

    auto actor = NMetering::CreateMeteringWriter(std::move(fileBackend));

    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        NMetering::MakeMeteringServiceID(),
        TActorSetupCmd(actor.Release(), TMailboxType::HTSwap, appData->IOPoolId)));
}

TAuditWriterInitializer::TAuditWriterInitializer(const TKikimrRunConfig &runConfig)
    : IKikimrServicesInitializer(runConfig)
    , KikimrRunConfig(runConfig)
{
}

void TAuditWriterInitializer::InitializeServices(TActorSystemSetup* setup, const TAppData* appData)
{
    auto logBackends = CreateAuditLogBackends(KikimrRunConfig, appData->Counters);

    if (logBackends.size() == 0)
        return;

    auto actor = NAudit::CreateAuditWriter(std::move(logBackends));

    setup->LocalServices.emplace_back(
        NAudit::MakeAuditServiceID(),
        TActorSetupCmd(std::move(actor), TMailboxType::HTSwap, appData->IOPoolId));

    if (appData->AuditConfig.GetHeartbeat().GetIntervalSeconds()) {
        auto heartbeatActor = NAudit::CreateHeartbeatActor(appData->AuditConfig);
        setup->LocalServices.emplace_back(
            NActors::TActorId(), // We don't need external communication with this actor
            TActorSetupCmd(std::move(heartbeatActor), TMailboxType::HTSwap, appData->UserPoolId));
    }
}

TSchemeBoardMonitoringInitializer::TSchemeBoardMonitoringInitializer(const TKikimrRunConfig &runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TSchemeBoardMonitoringInitializer::InitializeServices(TActorSystemSetup* setup, const TAppData* appData) {
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        MakeSchemeBoardMonitoringId(),
        TActorSetupCmd(CreateSchemeBoardMonitoring(), TMailboxType::HTSwap, appData->UserPoolId)));
}

TYqlLogsInitializer::TYqlLogsInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TYqlLogsInitializer::InitializeServices(TActorSystemSetup* setup, const TAppData* appData) {
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        MakeYqlLogsUpdaterId(),
        TActorSetupCmd(CreateYqlLogsUpdater(Config.GetLogConfig()), TMailboxType::HTSwap, appData->UserPoolId)
    ));
}

THealthCheckInitializer::THealthCheckInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void THealthCheckInitializer::InitializeServices(TActorSystemSetup* setup, const TAppData* appData) {
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        NHealthCheck::MakeHealthCheckID(),
        TActorSetupCmd(NHealthCheck::CreateHealthCheckService(), TMailboxType::HTSwap, appData->UserPoolId)));
}

TCountersInfoProviderInitializer::TCountersInfoProviderInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TCountersInfoProviderInitializer::InitializeServices(TActorSystemSetup* setup, const TAppData* appData) {
    IActor* actor = NKikimr::NCountersInfo::CreateCountersInfoProviderService(appData->Counters);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        NKikimr::NCountersInfo::MakeCountersInfoProviderServiceID(NodeId),
        TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId)));
}

TFederatedQueryInitializer::TFederatedQueryInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories, NFq::IYqSharedResources::TPtr yqSharedResources)
    : IKikimrServicesInitializer(runConfig)
    , Factories(std::move(factories))
    , YqSharedResources(std::move(yqSharedResources))
{
}

void TFederatedQueryInitializer::SetIcPort(ui32 icPort) {
    IcPort = icPort;
}

void TFederatedQueryInitializer::InitializeServices(TActorSystemSetup* setup, const TAppData* appData) {
    const auto& protoConfig = Config.GetFederatedQueryConfig();
    if (!protoConfig.GetEnabled()) {
        return;
    }

    TString tenant = "default_yq_tenant_name";
    for (const auto& slot : Config.GetTenantPoolConfig().GetSlots()) {
        if (slot.GetTenantName()) {
            tenant = slot.GetTenantName();
            break;
        }
    }

    auto actorRegistrator = [&](NActors::TActorId serviceActorId, NActors::IActor* actor) {
        setup->LocalServices.push_back(
            std::pair<TActorId, TActorSetupCmd>(
                serviceActorId,
                TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId)));
    };

    if (Config.HasPublicHttpConfig()) {
        NKikimr::NPublicHttp::Initialize(setup->LocalServices, *appData, Config.GetPublicHttpConfig());
    }

    NFq::Init(
        protoConfig,
        NodeId,
        actorRegistrator,
        appData,
        tenant,
        Factories->PqCmConnections,
        YqSharedResources,
        Factories->FolderServiceFactory,
        IcPort,
        Factories->AdditionalComputationNodeFactories
        );
}


} // namespace NKikimr::NKikimrServicesInitializers
