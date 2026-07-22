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
TGRpcServicesInitializer::TGRpcServicesInitializer(
    const TKikimrRunConfig& runConfig,
    std::shared_ptr<TModuleFactories> factories
)
    : IKikimrServicesInitializer(runConfig)
    , Factories(factories)
{}

static void JoinServicesListTo(TVector<TString>& dst, const NKikimrConfig::TGRpcConfig& config) {
    if ((config.ServicesSize() == 0 || config.ServicesEnabledSize() == 0)
        && config.ServicesDisabledSize() == 0) { // sets are disjoint
        dst.insert(dst.end(), config.GetServices().begin(), config.GetServices().end());
        dst.insert(dst.end(), config.GetServicesEnabled().begin(), config.GetServicesEnabled().end());
        return;
    }
    absl::btree_set<TString> resultSet;
    resultSet.insert(config.GetServices().begin(), config.GetServices().end());
    resultSet.insert(config.GetServicesEnabled().begin(), config.GetServicesEnabled().end());
    for (const auto& service : config.GetServicesDisabled()) {
        resultSet.erase(service);
    }
    dst.insert(dst.end(), resultSet.begin(), resultSet.end());
}

void TGRpcServicesInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                  const NKikimr::TAppData* appData)
{
    bool hasASCfg = Config.HasActorSystemConfig();
    bool useAutoConfig = !hasASCfg || NeedToUseAutoConfig(Config.GetActorSystemConfig());

    if (useAutoConfig) {
        NAutoConfigInitializer::ApplyAutoConfig(Config.MutableGRpcConfig(), Config.GetActorSystemConfig());
    }

    if (!IsServiceInitialized(setup, NMsgBusProxy::CreateMsgBusProxyId())
        && Config.HasGRpcConfig() && Config.GetGRpcConfig().GetStartGRpcProxy()) {
        IActor * proxy = NMsgBusProxy::CreateMessageBusServerProxy(nullptr);
        Y_ABORT_UNLESS(proxy);
        setup->LocalServices.emplace_back(
            NMsgBusProxy::CreateMsgBusProxyId(),
            TActorSetupCmd(proxy, TMailboxType::ReadAsFilled, appData->UserPoolId));

        if (appData->PQConfig.GetEnabled()) {

            IActor * cache = NMsgBusProxy::NPqMetaCacheV2::CreatePQMetaCache(appData->Counters);
            Y_ABORT_UNLESS(cache);
            setup->LocalServices.emplace_back(
                NMsgBusProxy::CreatePersQueueMetaCacheV2Id(),
                TActorSetupCmd(cache, TMailboxType::ReadAsFilled, appData->UserPoolId));
        }
    }

    if (!IsServiceInitialized(setup, NGRpcService::CreateGRpcRequestProxyId(0))) {
        const size_t proxyCount = Config.HasGRpcConfig() ? Config.GetGRpcConfig().GetGRpcProxyCount() : 1UL;
        for (size_t i = 0; i < proxyCount; ++i) {
            auto grpcReqProxy = Config.HasGRpcConfig() && Config.GetGRpcConfig().GetSkipSchemeCheck()
                ? NGRpcService::CreateGRpcRequestProxySimple(Config)
                : NGRpcService::CreateGRpcRequestProxy(Config);
            setup->LocalServices.push_back(std::pair<TActorId,
                                           TActorSetupCmd>(NGRpcService::CreateGRpcRequestProxyId(i),
                                                           TActorSetupCmd(grpcReqProxy, TMailboxType::ReadAsFilled,
                                                                          appData->UserPoolId)));
        }
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
                TActorId(),
                TActorSetupCmd(
                    NConsole::CreateJaegerTracingConfigurator(appData->TracingConfigurator, Config.GetTracingConfig()),
                    TMailboxType::ReadAsFilled,
                    appData->UserPoolId)));
    }

    if (!IsServiceInitialized(setup, NKesus::MakeKesusProxyServiceId())) {
        if (IActor* proxy = NKesus::CreateKesusProxyService()) {
            setup->LocalServices.emplace_back(NKesus::MakeKesusProxyServiceId(),
                                              TActorSetupCmd(proxy, TMailboxType::ReadAsFilled, appData->UserPoolId));
        }
    }

    if (Config.HasGRpcConfig() && Config.GetGRpcConfig().GetStartGRpcProxy()) {
    // logical copy from TKikimrRunner::InitializeGrpc
        const auto &config = Config.GetGRpcConfig();

        if (appData->Mon) {
            setup->LocalServices.emplace_back(NGRpcService::GrpcMonServiceId(),
                TActorSetupCmd(NGRpcService::CreateGrpcMonService(), TMailboxType::ReadAsFilled, appData->UserPoolId)
                );
        }

        auto stringsFromProto = [](TVector<TString>& vec, const auto& proto) {
            if (!proto.empty()) {
                vec.reserve(proto.size());
                for (const TString& value : proto) {
                    vec.emplace_back(value);
                }
            }
        };

        TVector<TIntrusivePtr<NGRpcService::TGrpcEndpointDescription>> endpoints;
        const TString &address = config.GetHost() && config.GetHost() != "[::]" ? config.GetHost() : FQDNHostName();
        if (const ui32 port = config.GetPort()) {
            TIntrusivePtr<NGRpcService::TGrpcEndpointDescription> desc = new NGRpcService::TGrpcEndpointDescription();
            desc->Address = config.GetPublicHost() ? config.GetPublicHost() : address;
            desc->Port = config.GetPublicPort() ? config.GetPublicPort() : port;
            desc->Ssl = false;

            stringsFromProto(desc->AddressesV4, config.GetPublicAddressesV4());
            stringsFromProto(desc->AddressesV6, config.GetPublicAddressesV6());

            JoinServicesListTo(desc->ServedServices, config);
            if (config.HasEndpointId()) {
                desc->EndpointId = config.GetEndpointId();
            }
            endpoints.push_back(std::move(desc));
        }

        if (const ui32 sslPort = config.GetSslPort()) {
            TIntrusivePtr<NGRpcService::TGrpcEndpointDescription> desc = new NGRpcService::TGrpcEndpointDescription();
            desc->Address = config.GetPublicHost() ? config.GetPublicHost() : address;
            desc->Port = config.GetPublicSslPort() ? config.GetPublicSslPort() : sslPort;
            desc->Ssl = true;

            stringsFromProto(desc->AddressesV4, config.GetPublicAddressesV4());
            stringsFromProto(desc->AddressesV6, config.GetPublicAddressesV6());
            desc->TargetNameOverride = config.GetPublicTargetNameOverride();

            JoinServicesListTo(desc->ServedServices, config);
            if (config.HasEndpointId()) {
                desc->EndpointId = config.GetEndpointId();
            }
            endpoints.push_back(std::move(desc));
        }

        if (Config.GetKafkaProxyConfig().GetEnableKafkaProxy()) {
            const auto& kafkaConfig = Config.GetKafkaProxyConfig();
            TIntrusivePtr<NGRpcService::TGrpcEndpointDescription> desc = new NGRpcService::TGrpcEndpointDescription();
            desc->Address = config.GetPublicHost() ? config.GetPublicHost() : address;
            desc->Port = kafkaConfig.GetListeningPort();
            desc->Ssl = kafkaConfig.HasSslCertificate();

            desc->EndpointId = NGRpcService::KafkaEndpointId;
            endpoints.push_back(std::move(desc));

        }

        for (auto &sx : config.GetExtEndpoints()) {
            const TString &localAddress = sx.GetHost() ? (sx.GetHost() != "[::]" ? sx.GetHost() : FQDNHostName()) : address;
            if (const ui32 port = sx.GetPort()) {
                TIntrusivePtr<NGRpcService::TGrpcEndpointDescription> desc = new NGRpcService::TGrpcEndpointDescription();
                desc->Address = sx.GetPublicHost() ? sx.GetPublicHost() : localAddress;
                desc->Port = sx.GetPublicPort() ? sx.GetPublicPort() : port;
                desc->Ssl = false;

                stringsFromProto(desc->AddressesV4, sx.GetPublicAddressesV4());
                stringsFromProto(desc->AddressesV6, sx.GetPublicAddressesV6());

                JoinServicesListTo(desc->ServedServices, sx);
                if (sx.HasEndpointId()) {
                    desc->EndpointId = sx.GetEndpointId();
                }
                endpoints.push_back(std::move(desc));
            }

            if (const ui32 sslPort = sx.GetSslPort()) {
                TIntrusivePtr<NGRpcService::TGrpcEndpointDescription> desc = new NGRpcService::TGrpcEndpointDescription();
                desc->Address = sx.GetPublicHost() ? sx.GetPublicHost() : localAddress;
                desc->Port = sx.GetPublicSslPort() ? sx.GetPublicSslPort() : sslPort;
                desc->Ssl = true;

                stringsFromProto(desc->AddressesV4, sx.GetPublicAddressesV4());
                stringsFromProto(desc->AddressesV6, sx.GetPublicAddressesV6());
                desc->TargetNameOverride = sx.GetPublicTargetNameOverride();

                desc->ServedServices.insert(desc->ServedServices.end(), sx.GetServices().begin(), sx.GetServices().end());
                if (sx.HasEndpointId()) {
                    desc->EndpointId = sx.GetEndpointId();
                }
                endpoints.push_back(std::move(desc));
            }
        }


        const auto& kqpConfig = Config.GetKQPConfig();
        const bool kqpEnabled = ServicesMask.EnableKqp
            && (!kqpConfig.HasEnable() || kqpConfig.GetEnable());

        TDuration publishWarmupTimeout = TDuration::Zero();
        const TString publisherDomainName = appData->DomainsInfo->Domain ? appData->DomainsInfo->Domain->Name : TString();
        if (kqpEnabled && NKqp::IsCompileCacheWarmupEnabled(
                Config.GetTableServiceConfig(), appData->TenantName, publisherDomainName)) {
            publishWarmupTimeout = NKqp::ImportWarmupConfigFromProto(
                Config.GetTableServiceConfig().GetCompileCacheWarmupConfig()).HardDeadline;
        }

        setup->LocalServices.emplace_back(
           NGRpcService::CreateGrpcPublisherServiceActorId(),
           TActorSetupCmd(CreateGrpcPublisherServiceActor(std::move(endpoints), publishWarmupTimeout),
               TMailboxType::ReadAsFilled, appData->UserPoolId)
        );
    }
}

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
// TStatsCollectorInitializer

TStatsCollectorInitializer::TStatsCollectorInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TStatsCollectorInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {

    if (appData->Mon) {
        IActor* statsCollector = CreateStatsCollector(
                1, // seconds
                *setup,
                appData->Counters);
        setup->LocalServices.emplace_back(
            TActorId(),
            TActorSetupCmd(
                statsCollector,
                TMailboxType::HTSwap,
                appData->SystemPoolId));

        IActor* memStatsCollector = CreateMemStatsCollector(
                1, // seconds
                appData->Counters);
        setup->LocalServices.emplace_back(
            TActorId(),
            TActorSetupCmd(
                memStatsCollector,
                TMailboxType::HTSwap,
                appData->SystemPoolId));

        IActor* procStatCollector = CreateProcStatCollector(
                5, // seconds
                appData->Counters);
        setup->LocalServices.emplace_back(
            TActorId(),
            TActorSetupCmd(
                procStatCollector,
                TMailboxType::HTSwap,
                appData->SystemPoolId));
    }
}
#endif

// TSelfPingInitializer

TSelfPingInitializer::TSelfPingInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TSelfPingInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {

    const TDuration selfPingInterval = Config.HasActorSystemConfig() && Config.GetActorSystemConfig().HasSelfPingInterval()
        ? TDuration::MicroSeconds(Config.GetActorSystemConfig().GetSelfPingInterval())
        : TDuration::MilliSeconds(10);

    const auto counters = GetServiceCounters(appData->Counters, "utils");

    for (size_t poolId = 0; poolId < setup->GetExecutorsCount(); ++poolId) {
        const auto& poolName = setup->GetPoolName(poolId);
        auto poolGroup = counters->GetSubgroup("execpool", poolName);
        auto maxPingCounter = poolGroup->GetCounter("SelfPingMaxUs", false);
        auto avgPingCounter = poolGroup->GetCounter("SelfPingAvgUs", false);
        auto avgPingCounterWithSmallWindow = poolGroup->GetCounter("SelfPingAvgUsIn1s", false);
        auto cpuTimeCounter = poolGroup->GetCounter("CpuMatBenchNs", false);
        IActor* selfPingActor = CreateSelfPingActor(selfPingInterval, maxPingCounter, avgPingCounter, avgPingCounterWithSmallWindow, cpuTimeCounter);
        setup->LocalServices.push_back(std::make_pair(TActorId(),
                                                      TActorSetupCmd(selfPingActor,
                                                                     TMailboxType::HTSwap,
                                                                     poolId)));
    }
}

// TWhiteBoardServiceInitializer

TWhiteBoardServiceInitializer::TWhiteBoardServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TWhiteBoardServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                       const NKikimr::TAppData* appData) {
    IActor* tabletStateService = NNodeWhiteboard::CreateNodeWhiteboardService();
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(NNodeWhiteboard::MakeNodeWhiteboardServiceId(NodeId),
                                                                       TActorSetupCmd(tabletStateService,
                                                                                      TMailboxType::HTSwap,
                                                                                      appData->SystemPoolId)));
}

// TTabletMonitorInitializer

TTabletMonitorInitializer::TTabletMonitorInitializer(
        const TKikimrRunConfig& runConfig,
        const TIntrusivePtr<NNodeTabletMonitor::ITabletStateClassifier>& tabletStateClassifier,
        const TIntrusivePtr<NNodeTabletMonitor::ITabletListRenderer>& tabletListRenderer)
    : IKikimrServicesInitializer(runConfig)
    , TabletStateClassifier(tabletStateClassifier)
    , TabletListRenderer(tabletListRenderer) {
}

void TTabletMonitorInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                   const NKikimr::TAppData* appData) {
    IActor* nodeTabletMonitor = NNodeTabletMonitor::CreateNodeTabletMonitor(TabletStateClassifier, TabletListRenderer);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(NNodeTabletMonitor::MakeNodeTabletMonitorID(NodeId),
                                                                       TActorSetupCmd(nodeTabletMonitor,
                                                                                      TMailboxType::HTSwap,
                                                                                      appData->UserPoolId)));
}

// TViewerInitializer

TViewerInitializer::TViewerInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories)
    : IKikimrServicesInitializer(runConfig)
    , KikimrRunConfig(runConfig)
    , Factories(factories)
{}

void TViewerInitializer::InitializeServices(
    NActors::TActorSystemSetup* setup,
    const NKikimr::TAppData* appData
) {
    using namespace NViewer;
    IActor* viewer = CreateViewer(KikimrRunConfig);
    SetupPQVirtualHandlers(dynamic_cast<IViewer*>(viewer));
    SetupDBVirtualHandlers(dynamic_cast<IViewer*>(viewer));
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeViewerID(NodeId),
                                                                       TActorSetupCmd(viewer,
                                                                                      TMailboxType::HTSwap,
                                                                                      appData->BatchPoolId)));
}

// TLoadInitializer

TLoadInitializer::TLoadInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TLoadInitializer::InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) {
    IActor *bsActor = CreateLoadTestActor(appData->Counters);
    setup->LocalServices.emplace_back(MakeLoadServiceID(NodeId), TActorSetupCmd(bsActor, TMailboxType::HTSwap, appData->UserPoolId));
    // FIXME: correct service id
}

// TFailureInjectionInitializer

TFailureInjectionInitializer::TFailureInjectionInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TFailureInjectionInitializer::InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) {
    IActor *actor = CreateFailureInjectionActor(Config.GetFailureInjectionConfig(), *appData);
    setup->LocalServices.emplace_back(MakeBlobStorageFailureInjectionID(NodeId),
        TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId));
    // FIXME: correct service id
}

// TMonPersistentBufferInitializer

TMonPersistentBufferInitializer::TMonPersistentBufferInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TMonPersistentBufferInitializer::InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) {
    IActor *actor = CreateMonPersistentBufferActor(Config, *appData);
    setup->LocalServices.emplace_back(MakeMonPersistentBufferID(NodeId),
        TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId));
}

// TPersQueueL2CacheInitializer


} // namespace NKikimr::NKikimrServicesInitializers
