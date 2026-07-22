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

#include <ydb/services/workload_manager/service/service.h>
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

TWorkloadManagerServiceInitializer::TWorkloadManagerServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TWorkloadManagerServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    auto workloadManager = NWorkloadManager::CreateService(NWorkloadManager::GetWorkloadManagerCounters(appData->Counters));
    setup->LocalServices.push_back(std::make_pair(
        NWorkloadManager::MakeServiceId(NodeId),
        TActorSetupCmd(workloadManager, TMailboxType::HTSwap, appData->UserPoolId)));
}

TKqpServiceInitializer::TKqpServiceInitializer(
        const TKikimrRunConfig& runConfig,
        std::shared_ptr<TModuleFactories> factories,
        IGlobalObjectStorage& globalObjects)
    : IKikimrServicesInitializer(runConfig)
    , Factories(std::move(factories))
    , GlobalObjects(globalObjects)
{}

void TKqpServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    bool enableKqp = true;
    TVector<NKikimrKqp::TKqpSetting> settings;
    if (Config.HasKQPConfig()) {
        auto& kqpConfig = Config.GetKQPConfig();
        if (kqpConfig.HasEnable()) {
            enableKqp = kqpConfig.GetEnable();
        }

        for (auto& setting : kqpConfig.GetSettings()) {
            settings.push_back(setting);
        }
    }

    if (enableKqp) {
        NKikimrKqp::TKqpSetting enableSpilling;
        enableSpilling.SetName("_KqpEnableSpilling");
        enableSpilling.SetValue(appData->EnableKqpSpilling ? "true" : "false");
        settings.emplace_back(std::move(enableSpilling));

        auto kqpProxySharedResources = std::make_shared<NKqp::TKqpProxySharedResources>();

        const TString warmupDomainName = appData->DomainsInfo->Domain ? appData->DomainsInfo->Domain->Name : TString();
        TDuration warmupDeadline;
        if (NKqp::IsCompileCacheWarmupEnabled(Config.GetTableServiceConfig(), appData->TenantName, warmupDomainName)) {
            auto warmupProto = Config.GetTableServiceConfig().GetCompileCacheWarmupConfig();
            warmupDeadline = TDuration::Seconds(std::max(
                warmupProto.GetHardDeadlineSeconds(), warmupProto.GetSoftDeadlineSeconds()));
        }

        // Create resource manager
        auto rm = NKqp::CreateKqpResourceManagerActor(Config.GetTableServiceConfig().GetResourceManager(), nullptr,
            {}, kqpProxySharedResources, NodeId, warmupDeadline);
        setup->LocalServices.push_back(std::make_pair(
            NKqp::MakeKqpRmServiceID(NodeId),
            TActorSetupCmd(rm, TMailboxType::HTSwap, appData->UserPoolId)));

        // We need to keep YqlLoggerScope alive as long as something may be trying to log
        GlobalObjects.AddGlobalObject(std::make_shared<NYql::NLog::YqlLoggerScope>(
            new NYql::NLog::TTlsLogBackend(new TNullLogBackend())));

        auto proxy = NKqp::CreateKqpProxyService(Config.GetLogConfig(), Config.GetTableServiceConfig(),
            Config.GetQueryServiceConfig(), Config.GetTliConfig(), std::move(settings), Factories->QueryReplayBackendFactory, std::move(kqpProxySharedResources),
            NKqp::MakeKqpFederatedQuerySetupFactory(setup, appData, Config), NYql::NDq::CreateS3ActorsFactory()
        );
        setup->LocalServices.push_back(std::make_pair(
            NKqp::MakeKqpProxyID(NodeId),
            TActorSetupCmd(proxy, TMailboxType::HTSwap, appData->UserPoolId)));

        auto describeSchemaSecretsService = NSecret::TDescribeSchemaSecretsServiceFactory().CreateService();
        setup->LocalServices.push_back(std::make_pair(
            NSecret::MakeDescribeSchemaSecretServiceId(NodeId),
            TActorSetupCmd(describeSchemaSecretsService, TMailboxType::HTSwap, appData->UserPoolId)));

        if (NKqp::IsCompileCacheWarmupEnabled(Config.GetTableServiceConfig(), appData->TenantName, warmupDomainName)) {
            auto warmupConfig = NKqp::ImportWarmupConfigFromProto(Config.GetTableServiceConfig().GetCompileCacheWarmupConfig());

            const ui32 cacheSize = Config.GetTableServiceConfig().GetCompileQueryCacheSize();
            if (cacheSize > 0 && warmupConfig.MaxQueriesToLoad > cacheSize) {
                warmupConfig.MaxQueriesToLoad = cacheSize;
            }

            TString database = appData->TenantName;
            const TString& cluster = warmupDomainName;

            TVector<NActors::TActorId> notifyActorIds = {
                NKqp::MakeKqpRmServiceID(NodeId),
                NKqp::MakeKqpProxyID(NodeId),
                MakeGRpcServersManagerId(NodeId),
                NGRpcService::CreateGrpcPublisherServiceActorId(),
            };
            auto warmupActor = NKqp::CreateKqpWarmupActor(warmupConfig, database, cluster, std::move(notifyActorIds));
            setup->LocalServices.push_back(std::make_pair(
                NKqp::MakeKqpWarmupActorId(NodeId),
                TActorSetupCmd(warmupActor, TMailboxType::HTSwap, appData->UserPoolId)));
        }
    }
}

TScanGroupedMemoryLimiterInitializer::TScanGroupedMemoryLimiterInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TScanGroupedMemoryLimiterInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NOlap::NGroupedMemoryManager::TConfig serviceConfig;
    if (Config.GetScanGroupedMemoryLimiterConfig().GetCountBuckets() == 0) {
        Config.MutableScanGroupedMemoryLimiterConfig()->SetCountBuckets(10);
    }
    Y_ABORT_UNLESS(serviceConfig.DeserializeFromProto(Config.GetScanGroupedMemoryLimiterConfig()));

    if (serviceConfig.IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup = tabletGroup->GetSubgroup("type", "TX_SCAN_GROUPED_MEMORY_LIMITER");

        auto service = NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::CreateService(serviceConfig, countersGroup);

        setup->LocalServices.push_back(std::make_pair(
            NOlap::NGroupedMemoryManager::TScanMemoryLimiterOperator::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

TCompGroupedMemoryLimiterInitializer::TCompGroupedMemoryLimiterInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TCompGroupedMemoryLimiterInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NOlap::NGroupedMemoryManager::TConfig serviceConfig;
    if (Config.GetCompGroupedMemoryLimiterConfig().GetCountBuckets() == 0) {
        Config.MutableCompGroupedMemoryLimiterConfig()->SetCountBuckets(1);
    }
    Y_ABORT_UNLESS(serviceConfig.DeserializeFromProto(Config.GetCompGroupedMemoryLimiterConfig()));

    if (serviceConfig.IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup = tabletGroup->GetSubgroup("type", "TX_COMP_GROUPED_MEMORY_LIMITER");

        auto service = NOlap::NGroupedMemoryManager::TCompMemoryLimiterOperator::CreateService(serviceConfig, countersGroup);

        setup->LocalServices.push_back(std::make_pair(NOlap::NGroupedMemoryManager::TCompMemoryLimiterOperator::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

TDeduplicationGroupedMemoryLimiterInitializer::TDeduplicationGroupedMemoryLimiterInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TDeduplicationGroupedMemoryLimiterInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NOlap::NGroupedMemoryManager::TConfig serviceConfig;
    if (Config.GetDeduplicationGroupedMemoryLimiterConfig().GetCountBuckets() == 0) {
        Config.MutableDeduplicationGroupedMemoryLimiterConfig()->SetCountBuckets(1);
    }
    Y_ABORT_UNLESS(serviceConfig.DeserializeFromProto(Config.GetDeduplicationGroupedMemoryLimiterConfig()));

    if (serviceConfig.IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup = tabletGroup->GetSubgroup("type", "TX_DEDU_GROUPED_MEMORY_LIMITER");

        auto service = NOlap::NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::CreateService(serviceConfig, countersGroup);

        setup->LocalServices.push_back(std::make_pair(NOlap::NGroupedMemoryManager::TDeduplicationMemoryLimiterOperator::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

TCompDiskLimiterInitializer::TCompDiskLimiterInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TCompDiskLimiterInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NLimiter::TConfig serviceConfig;
    Y_ABORT_UNLESS(serviceConfig.DeserializeFromProto<NLimiter::TCompDiskLimiterPolicy>(Config.GetCompDiskLimiterConfig()));

    if (serviceConfig.IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup = tabletGroup->GetSubgroup("type", "TX_COMP_DISK_LIMITER");

        auto service = NLimiter::TCompDiskOperator::CreateService(serviceConfig, countersGroup);

        setup->LocalServices.push_back(std::make_pair(
            NLimiter::TCompDiskOperator::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

TCompPrioritiesInitializer::TCompPrioritiesInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TCompPrioritiesInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NPrioritiesQueue::TConfig serviceConfig;
    if (Config.HasCompPrioritiesConfig()) {
        Y_ABORT_UNLESS(serviceConfig.DeserializeFromProto(Config.GetCompPrioritiesConfig()));
    }

    if (serviceConfig.IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_COMP_PRIORITIES");

        auto service = NPrioritiesQueue::TCompServiceOperator::CreateService(serviceConfig, conveyorGroup);

        setup->LocalServices.push_back(std::make_pair(
            NPrioritiesQueue::TCompServiceOperator::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

TGeneralCachePortionsMetadataInitializer::TGeneralCachePortionsMetadataInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TGeneralCachePortionsMetadataInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    auto serviceConfig = NGeneralCache::NPublic::TConfig::BuildFromProto(Config.GetPortionsMetadataCache());
    if (serviceConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse portions metadata cache config")("action", "default_usage")(
            "error", serviceConfig.GetErrorMessage())("default", NGeneralCache::NPublic::TConfig::BuildDefault().DebugString());
        serviceConfig = NGeneralCache::NPublic::TConfig::BuildDefault();
    }
    AFL_VERIFY(!serviceConfig.IsFail());

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_GENERAL_CACHE_PORTIONS_METADATA");

    auto service = NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>::CreateService(*serviceConfig, conveyorGroup);

    setup->LocalServices.push_back(
        std::make_pair(NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TPortionsMetadataCachePolicy>::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
}

TGeneralCacheColumnDataInitializer::TGeneralCacheColumnDataInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TGeneralCacheColumnDataInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    auto serviceConfig = NGeneralCache::NPublic::TConfig::BuildFromProto(Config.GetColumnDataCache());
    if (serviceConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse column data cache config")("action", "default_usage")(
            "error", serviceConfig.GetErrorMessage())("default", NGeneralCache::NPublic::TConfig::BuildDefault().DebugString());
        serviceConfig = NGeneralCache::NPublic::TConfig::BuildDefault();
    }
    AFL_VERIFY(!serviceConfig.IsFail());

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_GENERAL_CACHE_COLUMN_DATA");

    auto service = NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TColumnDataCachePolicy>::CreateService(*serviceConfig, conveyorGroup);

    setup->LocalServices.push_back(
        std::make_pair(NGeneralCache::TServiceOperator<NOlap::NGeneralCache::TColumnDataCachePolicy>::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
}


TCompositeConveyorInitializer::TCompositeConveyorInitializer(const TKikimrRunConfig& runConfig)
	: IKikimrServicesInitializer(runConfig) {
}

void TCompositeConveyorInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    const NKikimrConfig::TCompositeConveyorConfig protoConfig = [&]() {
        if (Config.HasCompositeConveyorConfig()) {
            return Config.GetCompositeConveyorConfig();
        }
        NKikimrConfig::TCompositeConveyorConfig result;
        if (Config.HasCompConveyorConfig()) {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            protoLink.SetWeight(1);
            if (Config.GetCompConveyorConfig().HasWorkersCountDouble()) {
                protoWorkersPool.SetWorkersCount(Config.GetCompConveyorConfig().GetWorkersCountDouble());
            } else if (Config.GetCompConveyorConfig().HasWorkersCount()) {
                protoWorkersPool.SetWorkersCount(Config.GetCompConveyorConfig().GetWorkersCount());
            } else if (Config.GetCompConveyorConfig().HasDefaultFractionOfThreadsCount()) {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(Config.GetCompConveyorConfig().GetDefaultFractionOfThreadsCount());
            } else {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(0.33);
            }
        } else {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Compaction));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.33);
            protoWorkersPool.SetMaxBatchSize(1);
        }

        if (Config.HasInsertConveyorConfig()) {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            protoLink.SetWeight(1);
            if (Config.GetInsertConveyorConfig().HasWorkersCountDouble()) {
                protoWorkersPool.SetWorkersCount(Config.GetInsertConveyorConfig().GetWorkersCountDouble());
            } else if (Config.GetInsertConveyorConfig().HasWorkersCount()) {
                protoWorkersPool.SetWorkersCount(Config.GetInsertConveyorConfig().GetWorkersCount());
            } else if (Config.GetCompConveyorConfig().HasDefaultFractionOfThreadsCount()) {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(Config.GetCompConveyorConfig().GetDefaultFractionOfThreadsCount());
            } else {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(0.2);
            }
        } else {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Insert));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.2);
            protoWorkersPool.SetMaxBatchSize(1);
        }
        if (Config.HasScanConveyorConfig()) {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            protoLink.SetWeight(1);
            if (Config.GetScanConveyorConfig().HasWorkersCountDouble()) {
                protoWorkersPool.SetWorkersCount(Config.GetScanConveyorConfig().GetWorkersCountDouble());
            } else if (Config.GetScanConveyorConfig().HasWorkersCount()) {
                protoWorkersPool.SetWorkersCount(Config.GetScanConveyorConfig().GetWorkersCount());
            } else if (Config.GetCompConveyorConfig().HasDefaultFractionOfThreadsCount()) {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(Config.GetCompConveyorConfig().GetDefaultFractionOfThreadsCount());
            } else {
                protoWorkersPool.SetDefaultFractionOfThreadsCount(0.4);
            }
        } else {
            NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
            protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
            NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
            protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Scan));
            protoLink.SetWeight(1);
            protoWorkersPool.SetDefaultFractionOfThreadsCount(0.4);
        }

        NKikimrConfig::TCompositeConveyorConfig::TCategory& protoCategory = *result.AddCategories();
        protoCategory.SetName(::ToString(NConveyorComposite::ESpecialTaskCategory::Deduplication));
        NKikimrConfig::TCompositeConveyorConfig::TWorkersPool& protoWorkersPool = *result.AddWorkerPools();
        NKikimrConfig::TCompositeConveyorConfig::TWorkerPoolCategoryLink& protoLink = *protoWorkersPool.AddLinks();
        protoLink.SetCategory(::ToString(NConveyorComposite::ESpecialTaskCategory::Deduplication));
        protoLink.SetWeight(1);
        protoWorkersPool.SetDefaultFractionOfThreadsCount(0.3);

        return result;
    }();

    auto serviceConfig = NConveyorComposite::NConfig::TConfig::BuildFromProto(protoConfig);
    if (serviceConfig.IsFail()) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD)("error", "cannot parse composite conveyor config")("action", "default_usage")(
            "error", serviceConfig.GetErrorMessage())("default", NConveyorComposite::NConfig::TConfig::BuildDefault().DebugString());
        serviceConfig = NConveyorComposite::NConfig::TConfig::BuildDefault();
    }
    AFL_VERIFY(!serviceConfig.IsFail());

    if (serviceConfig->IsEnabled()) {
        TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
        TIntrusivePtr<::NMonitoring::TDynamicCounters> conveyorGroup = tabletGroup->GetSubgroup("type", "TX_COMPOSITE_CONVEYOR");

        auto service = NConveyorComposite::TServiceOperator::CreateService(*serviceConfig, conveyorGroup);

        setup->LocalServices.push_back(std::make_pair(
            NConveyorComposite::TServiceOperator::MakeServiceId(NodeId), TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

TMetadataProviderInitializer::TMetadataProviderInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TMetadataProviderInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NMetadata::NProvider::TConfig serviceConfig;
    if (Config.HasMetadataProviderConfig()) {
        Y_ABORT_UNLESS(serviceConfig.DeserializeFromProto(Config.GetMetadataProviderConfig()));
    }

    if (serviceConfig.IsEnabled()) {
        auto service = NMetadata::NProvider::CreateService(serviceConfig);
        setup->LocalServices.push_back(std::make_pair(
            NMetadata::NProvider::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
    }
}

TMemoryLogInitializer::TMemoryLogInitializer(
        const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TMemoryLogInitializer::InitializeServices(
        NActors::TActorSystemSetup*,
        const NKikimr::TAppData*)
{
    if (!Config.HasMemoryLogConfig()) {
        return;
    }

    if (!Config.GetMemoryLogConfig().HasLogBufferSize()) {
        return;
    }
    if (Config.GetMemoryLogConfig().GetLogBufferSize() == 0ULL) {
        return;
    }

    LogBufferSize = Config.GetMemoryLogConfig().GetLogBufferSize();

    if (Config.GetMemoryLogConfig().HasLogGrainSize()) {
        LogGrainSize = Config.GetMemoryLogConfig().GetLogGrainSize();
    }

    if (LogGrainSize != 0) {
        TMemoryLog::CreateMemoryLogBuffer(LogBufferSize, LogGrainSize);
    } else {
        TMemoryLog::CreateMemoryLogBuffer(LogBufferSize);
    }
}


} // namespace NKikimr::NKikimrServicesInitializers
