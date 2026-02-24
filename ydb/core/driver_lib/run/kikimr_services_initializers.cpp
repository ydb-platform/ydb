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
#include <ydb/core/kqp/finalize_script_service/kqp_finalize_script_service.h>
#include <ydb/core/kqp/federated_query/actors/kqp_federated_query_actors.h>
#include <ydb/core/kqp/compile_service/kqp_warmup_compile_actor.h>

#include <ydb/core/load_test/service_actor.h>

#include <ydb/core/pgproxy/pg_proxy.h>
#include <ydb/core/local_pgwire/local_pgwire.h>

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

#if defined(OS_LINUX)
#include <ydb/core/nbs/cloud/blockstore/bootstrap/bootstrap.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/api/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/ss_proxy/ss_proxy.h>
#include <ydb/core/nbs/cloud/blockstore/config/storage.pb.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/volume/volume.h>
#include <ydb/core/nbs/cloud/blockstore/libs/storage/partition_direct/partition_direct.h>
#endif

#include <ydb/core/mon/mon.h>
#include <ydb/core/mon_alloc/monitor.h>
#include <ydb/core/mon_alloc/profiler.h>
#include <ydb/core/mon_alloc/stats.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>

#include <ydb/core/persqueue/pq.h>

#include <ydb/library/services/services.pb.h>
#include <ydb/core/protos/console_config.pb.h>
#include <ydb/core/protos/node_limits.pb.h>
#include <ydb/core/protos/compile_service_config.pb.h>
#include <ydb/core/protos/memory_controller_config.pb.h>

#include <ydb/core/public_http/http_service.h>

#include <ydb/core/quoter/quoter_service.h>

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

#include <ydb/core/util/aws.h>
#include <ydb/core/util/failure_injection.h>
#include <ydb/core/util/memory_tracker.h>
#include <ydb/core/util/sig.h>

#include <ydb/core/viewer/viewer.h>

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

#include <ydb/services/ext_index/common/config.h>
#include <ydb/services/ext_index/service/executor.h>

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
#include <ydb/library/actors/interconnect/interconnect_mon.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_proxy_wrapper.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/handshake_broker.h>
#include <ydb/library/actors/interconnect/load.h>
#include <ydb/library/actors/interconnect/poller/poller_actor.h>
#include <ydb/library/actors/interconnect/poller/poller_tcp.h>
#include <ydb/library/actors/interconnect/rdma/cq_actor/cq_actor.h>
#include <ydb/library/actors/interconnect/rdma/mem_pool.h>
#include <ydb/library/actors/retro_tracing/retro_collector.h>
#include <ydb/library/actors/util/affinity.h>
#include <ydb/library/actors/wilson/wilson_uploader.h>
#include <ydb/library/slide_limiter/service/service.h>
#include <ydb/library/slide_limiter/usage/config.h>
#include <ydb/library/slide_limiter/usage/service.h>

#include <ydb/core/graph/api/service.h>
#include <ydb/core/graph/api/shard.h>

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

struct TAwsApiGuard {
    const TAwsClientConfig Config;

    TAwsApiGuard(const NKikimrConfig::TAwsClientConfig& config)
        : Config{
            .LogConfig{
                .LogLevel = config.GetLogConfig().GetLogLevel(),
                .FilenamePrefix = config.GetLogConfig().GetFilenamePrefix(),
            },
        }
    {
        InitAwsAPI(Config);
    }

    ~TAwsApiGuard() {
        ShutdownAwsAPI(Config);
    }
};

ui32 TFederatedQueryInitializer::IcPort = 0;

IKikimrServicesInitializer::IKikimrServicesInitializer(const TKikimrRunConfig& runConfig)
    : Config(runConfig.AppConfig)
    , NodeId(runConfig.NodeId)
    , ScopeId(runConfig.ScopeId)
    , TinyMode(runConfig.TinyMode)
{}

// TBasicServicesInitializer

void AddExecutorPool(
    TCpuManagerConfig& cpuManager,
    const NKikimrConfig::TActorSystemConfig::TExecutor& poolConfig,
    const NKikimrConfig::TActorSystemConfig& systemConfig,
    ui32 poolId,
    const NKikimr::TAppData* appData)
{
    const auto counters = GetServiceCounters(appData->Counters, "utils");
    NActorSystemConfigHelpers::AddExecutorPool(cpuManager, poolConfig, systemConfig, poolId, counters);
}

static TCpuManagerConfig CreateCpuManagerConfig(const NKikimrConfig::TActorSystemConfig& config,
                                                const NKikimr::TAppData* appData)
{
    TCpuManagerConfig cpuManager;
    cpuManager.Shared.United = config.GetUseUnitedPool();
    cpuManager.PingInfoByPool.resize(config.GetExecutor().size());
    for (int poolId = 0; poolId < config.GetExecutor().size(); poolId++) {
        AddExecutorPool(cpuManager, config.GetExecutor(poolId), config, poolId, appData);
    }
    return cpuManager;
}

static bool IsServiceInitialized(NActors::TActorSystemSetup* setup, TActorId service)
{
    for (auto &pr : setup->LocalServices)
        if (pr.first == service)
            return true;
    return false;
}

TBasicServicesInitializer::TBasicServicesInitializer(const TKikimrRunConfig& runConfig, std::shared_ptr<TModuleFactories> factories)
    : IKikimrServicesInitializer(runConfig)
    , Factories(std::move(factories))
{
}

static ui32 GetInterconnectThreadPoolId(const NKikimr::TAppData* appData) {
    Y_DEBUG_ABORT_UNLESS(appData != nullptr);
    auto item = appData->ServicePools.find("Interconnect");
    if (item != appData->ServicePools.end())
        return item->second;
    else
        return appData->SystemPoolId;
}

static TInterconnectSettings GetInterconnectSettings(const NKikimrConfig::TInterconnectConfig& config, ui32 numNodes, ui32 numDataCenters) {
    TInterconnectSettings result;

    if (config.HasSelfKickDelayDuration() || config.HasSelfKickDelay()) {
        Cerr << "SelfKickDelayDuration/SelfKickDelay option is deprecated" << Endl;
    }

    if (config.HasHandshakeTimeoutDuration()) {
        result.Handshake = DurationFromProto(config.GetHandshakeTimeoutDuration());
    } else if (config.HasHandshakeTimeout()) {
        result.Handshake = TDuration::MilliSeconds(config.GetHandshakeTimeout());
    }

    if (config.HasHeartbeatIntervalDuration() || config.HasHeartbeatInterval()) {
        Cerr << "HeartbeatIntervalDuration/HeartbeatInterval option is deprecated" << Endl;
    }

    if (config.HasDeadPeerTimeoutDuration()) {
        result.DeadPeer = DurationFromProto(config.GetDeadPeerTimeoutDuration());
    } else if (config.HasDeadPeerTimeout()) {
        result.DeadPeer = TDuration::MilliSeconds(config.GetDeadPeerTimeout());
    }

    if (config.HasSendBufferDieLimitInMB()) {
        result.SendBufferDieLimitInMB = config.GetSendBufferDieLimitInMB();
    } else {
        result.SendBufferDieLimitInMB = 512;
    }

    if (config.HasCloseOnIdleTimeoutDuration()) {
        result.CloseOnIdle = DurationFromProto(config.GetCloseOnIdleTimeoutDuration());
    } else if (config.HasCloseOnIdleTimeout()) {
        result.CloseOnIdle = TDuration::Seconds(config.GetCloseOnIdleTimeout());
    }

    auto mode = config.GetCounterMergeMode();
    if (config.HasMergePerPeerCounters() && !config.HasCounterMergeMode()) {
        mode = !config.GetMergePerPeerCounters()
            ? NKikimrConfig::TInterconnectConfig::NO_MERGE : numDataCenters > 1
            ? NKikimrConfig::TInterconnectConfig::PER_DATA_CENTER
            : NKikimrConfig::TInterconnectConfig::PER_PEER;
    }
    switch (mode) {
        case NKikimrConfig::TInterconnectConfig::AUTO:
            if (numNodes > 100) {
                if (numDataCenters > 1) {
                    result.MergePerDataCenterCounters = true;
                } else {
                    result.MergePerPeerCounters = true;
                }
            }
            break;
        case NKikimrConfig::TInterconnectConfig::PER_PEER:
            result.MergePerPeerCounters = true;
            break;
        case NKikimrConfig::TInterconnectConfig::PER_DATA_CENTER:
            result.MergePerDataCenterCounters = true;
            break;
        case NKikimrConfig::TInterconnectConfig::NO_MERGE:
            break;
    }

    switch (config.GetEncryptionMode()) {
        case NKikimrConfig::TInterconnectConfig::DISABLED:
            result.EncryptionMode = EEncryptionMode::DISABLED;
            break;
        case NKikimrConfig::TInterconnectConfig::OPTIONAL:
            result.EncryptionMode = EEncryptionMode::OPTIONAL;
            break;
        case NKikimrConfig::TInterconnectConfig::REQUIRED:
            result.EncryptionMode = EEncryptionMode::REQUIRED;
            break;
    }
    result.TlsAuthOnly = config.GetTlsAuthOnly();
    if (const auto& forbidden = config.GetForbiddenSignatureAlgorithms(); !forbidden.empty()) {
        result.ForbiddenSignatureAlgorithms = {forbidden.begin(), forbidden.end()};
    }

    if (config.HasTCPSocketBufferSize())
        result.TCPSocketBufferSize = config.GetTCPSocketBufferSize();

    if (config.HasMaxTimePerEventInMks()) {
        Cerr << "MaxTimePerEventInMks option is deprecated" << Endl;
    }

    if (config.HasTotalInflightAmountOfData()) {
        result.TotalInflightAmountOfData = config.GetTotalInflightAmountOfData();
    }

    if (config.HasPingPeriodDuration()) {
        result.PingPeriod = DurationFromProto(config.GetPingPeriodDuration());
    }

    if (config.HasForceConfirmPeriodDuration()) {
        result.ForceConfirmPeriod = DurationFromProto(config.GetForceConfirmPeriodDuration());
    }

    if (config.HasLostConnectionDuration()) {
        result.LostConnection = DurationFromProto(config.GetLostConnectionDuration());
    }

    if (config.HasBatchPeriodDuration()) {
        result.BatchPeriod = DurationFromProto(config.GetBatchPeriodDuration());
    } else {
        result.BatchPeriod = TDuration();
    }

    result.BindOnAllAddresses = config.GetBindOnAllAddresses();

    auto readFile = [](std::optional<TString> value, std::optional<TString> path, const char *name) {
        if (value) {
            return *value;
        } else if (path) {
            try {
                return TFileInput(*path).ReadAll();
            } catch (const std::exception& ex) {
                ythrow yexception()
                    << "failed to read " << name << " file '" << *path << "': " << ex.what();
            }
        }
        return TString();
    };
    result.Certificate = readFile(config.HasCertificate() ? std::make_optional(config.GetCertificate()) : std::nullopt,
        config.HasPathToCertificateFile() ? std::make_optional(config.GetPathToCertificateFile()) : std::nullopt,
        "certificate");
    result.PrivateKey = readFile(config.HasPrivateKey() ? std::make_optional(config.GetPrivateKey()) : std::nullopt,
        config.HasPathToPrivateKeyFile() ? std::make_optional(config.GetPathToPrivateKeyFile()) : std::nullopt,
        "private key");
    result.CaFilePath = config.GetPathToCaFile();
    result.CipherList = config.GetCipherList();

    if (config.HasMessagePendingTimeout()) {
        result.MessagePendingTimeout = DurationFromProto(config.GetMessagePendingTimeout());
    }
    if (config.HasMessagePendingSize()) {
        result.MessagePendingSize = config.GetMessagePendingSize();
    }

    if (config.HasPreallocatedBufferSize()) {
        result.PreallocatedBufferSize = config.GetPreallocatedBufferSize();
    }
    if (config.HasNumPreallocatedBuffers()) {
        result.NumPreallocatedBuffers = config.GetNumPreallocatedBuffers();
    }

    result.EnableExternalDataChannel = config.GetEnableExternalDataChannel();

    if (config.HasValidateIncomingPeerViaDirectLookup()) {
        result.ValidateIncomingPeerViaDirectLookup = config.GetValidateIncomingPeerViaDirectLookup();
    }
    result.SocketBacklogSize = config.GetSocketBacklogSize();

    if (config.HasFirstErrorSleep()) {
        result.FirstErrorSleep = DurationFromProto(config.GetFirstErrorSleep());
    }
    if (config.HasMaxErrorSleep()) {
        result.MaxErrorSleep = DurationFromProto(config.GetMaxErrorSleep());
    }
    if (config.HasErrorSleepRetryMultiplier()) {
        result.ErrorSleepRetryMultiplier = config.GetErrorSleepRetryMultiplier();
    }

    if (config.HasEventDelayMicrosec()) {
        result.EventDelay = TDuration::MicroSeconds(config.GetEventDelayMicrosec());
    }

    switch (config.GetSocketSendOptimization()) {
        case NKikimrConfig::TInterconnectConfig::IC_SO_DISABLED:
            result.SocketSendOptimization = ESocketSendOptimization::DISABLED;
            break;
        case NKikimrConfig::TInterconnectConfig::IC_SO_MSG_ZEROCOPY:
            result.SocketSendOptimization = ESocketSendOptimization::IC_MSG_ZEROCOPY;
            break;
    }

    if (config.HasRdmaChecksum()) {
        result.RdmaChecksum = config.GetRdmaChecksum();
    }

    return result;
}


void TBasicServicesInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                   const NKikimr::TAppData* appData) {
    bool hasASCfg = Config.HasActorSystemConfig();
    bool useAutoConfig = !hasASCfg || NeedToUseAutoConfig(Config.GetActorSystemConfig());
    if (useAutoConfig) {
        bool isDynamicNode = appData->DynamicNameserviceConfig->MinDynamicNodeId <= NodeId;
        NAutoConfigInitializer::ApplyAutoConfig(Config.MutableActorSystemConfig(), isDynamicNode, TinyMode);
    }

    Y_ABORT_UNLESS(Config.HasActorSystemConfig());
    auto& systemConfig = Config.GetActorSystemConfig();
    Y_ABORT_UNLESS(systemConfig.HasScheduler());
    Y_ABORT_UNLESS(systemConfig.ExecutorSize());
    const ui32 systemPoolId = appData->SystemPoolId;
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters = appData->Counters;

    setup->NodeId = NodeId;
    setup->CpuManager = CreateCpuManagerConfig(systemConfig, appData);
    setup->MonitorStuckActors = systemConfig.GetMonitorStuckActors();

    auto schedulerConfig = NActorSystemConfigHelpers::CreateSchedulerConfig(systemConfig.GetScheduler());
    schedulerConfig.MonCounters = GetServiceCounters(counters, "utils");
    setup->Scheduler.Reset(CreateSchedulerThread(schedulerConfig));
    setup->LocalServices.emplace_back(MakeIoDispatcherActorId(), TActorSetupCmd(CreateIoDispatcherActor(
        schedulerConfig.MonCounters->GetSubgroup("subsystem", "io_dispatcher")), TMailboxType::HTSwap, systemPoolId));

    NLwTraceMonPage::DashboardRegistry().Register(NActors::LWTraceDashboards(setup));

    if (Config.HasNameserviceConfig()) {
        const auto& nsConfig = Config.GetNameserviceConfig();
        const TActorId resolverId = NDnsResolver::MakeDnsResolverActorId();
        const TActorId nameserviceId = GetNameserviceActorId();

        TIntrusivePtr<TTableNameserverSetup> table = NNodeBroker::BuildNameserverTable(nsConfig);

        const ui32 numNodes = table->StaticNodeTable.size();
        TSet<TString> dataCenters;
        for (const auto& [nodeId, info] : table->StaticNodeTable) {
            dataCenters.insert(info.Location.GetDataCenterId());
        }

        NDnsResolver::TOnDemandDnsResolverOptions resolverOptions;
        resolverOptions.MonCounters = GetServiceCounters(counters, "utils")->GetSubgroup("subsystem", "dns_resolver");
        resolverOptions.ForceTcp = nsConfig.GetForceTcp();
        resolverOptions.KeepSocket = nsConfig.GetKeepSocket();
        switch (nsConfig.GetDnsResolverType()) {
            case NKikimrConfig::TStaticNameserviceConfig::ARES:
                resolverOptions.Type = NDnsResolver::EDnsResolverType::Ares;
                break;
            case NKikimrConfig::TStaticNameserviceConfig::LIBC:
                resolverOptions.Type = NDnsResolver::EDnsResolverType::Libc;
                break;
        }
        resolverOptions.AddTrailingDot = nsConfig.GetAddTrailingDot();
        IActor *resolver = NDnsResolver::CreateOnDemandDnsResolver(resolverOptions);

        setup->LocalServices.emplace_back(
            resolverId,
            TActorSetupCmd(resolver, TMailboxType::HTSwap, systemPoolId));

        IActor *nameservice;

        switch (nsConfig.GetType()) {
            case NKikimrConfig::TStaticNameserviceConfig::NS_FIXED:
                nameservice = NActors::CreateNameserverTable(table, appData->IOPoolId);
                break;
            case NKikimrConfig::TStaticNameserviceConfig::NS_DEFAULT:
            case NKikimrConfig::TStaticNameserviceConfig::NS_NODE_BROKER:
                if (Config.GetDynamicNodeConfig().HasNodeInfo()) {
                    auto& info = Config.GetDynamicNodeConfig().GetNodeInfo();
                    nameservice = NNodeBroker::CreateDynamicNameserver(table, info, *appData->DomainsInfo, appData->IOPoolId);
                } else {
                    nameservice = NNodeBroker::CreateDynamicNameserver(table, appData->IOPoolId);
                }
                break;
            case NKikimrConfig::TStaticNameserviceConfig::NS_EXTERNAL:
                nameservice = NActors::CreateDynamicNameserver(table, TDuration::Seconds(3), appData->IOPoolId);
                break;
        }

        setup->LocalServices.emplace_back(
            nameserviceId,
            TActorSetupCmd(nameservice, TMailboxType::HTSwap, systemPoolId));

        if (Config.HasInterconnectConfig() && Config.GetInterconnectConfig().GetStartTcp()) {
            const auto& icConfig = Config.GetInterconnectConfig();

            TChannelsConfig channels;
            auto settings = GetInterconnectSettings(icConfig, numNodes, dataCenters.size());
            ui32 interconnectPoolId = GetInterconnectThreadPoolId(appData);

            for (const auto& channel : icConfig.GetChannel()) {
                const auto index = channel.GetIndex();
                ui32 weight = 0;
                Y_ABORT_UNLESS(!(channel.HasQuota() && channel.HasWeight()), "Only one field should be set: Weight or Quota, Weight is preffered");
                if (channel.HasWeight()) {
                    weight = channel.GetWeight();
                } else if (channel.HasQuota()) {
                    weight = channel.GetQuota();
                }

                Y_ABORT_UNLESS(index < 1U << IEventHandle::ChannelBits, "Channel index is too large: got %" PRIu32 ", should be less than %" PRIu32, index, 1U << IEventHandle::ChannelBits);
                Y_ABORT_UNLESS(weight > 0U && weight <= std::numeric_limits<ui16>::max(), "Channel weight is out of allowed range: got %" PRIu32 ", should be > 0 and < %" PRIu32, weight, std::numeric_limits<ui16>::max());

                channels.insert({ui16(index), TChannelSettings{ui16(weight)}});
            }

            // create poller actor (whether platform supports it)
            setup->LocalServices.emplace_back(MakePollerActorId(), TActorSetupCmd(CreatePollerActor(), TMailboxType::ReadAsFilled, systemPoolId));

            auto destructorQueueSize = std::make_shared<std::atomic<TAtomicBase>>(0);

            TIntrusivePtr<TInterconnectProxyCommon> icCommon;
            icCommon.Reset(new TInterconnectProxyCommon);

            NMonitoring::TDynamicCounterPtr interconectCounters = GetServiceCounters(counters, "interconnect");

            if (icConfig.GetUseRdma()) {
                NInterconnect::NRdma::ECqMode rdmaCqMode = NInterconnect::NRdma::ECqMode::EVENT;
                if (icConfig.HasRdmaCqMode()) {
                    switch (icConfig.GetRdmaCqMode()) {
                        case NKikimrConfig::TInterconnectConfig::CQ_EVENT:
                            rdmaCqMode = NInterconnect::NRdma::ECqMode::EVENT;
                            break;
                        case NKikimrConfig::TInterconnectConfig::CQ_POLLING:
                            rdmaCqMode = NInterconnect::NRdma::ECqMode::POLLING;
                            break;
                    }
                }
                setup->LocalServices.emplace_back(NInterconnect::NRdma::MakeCqActorId(),
                    TActorSetupCmd(NInterconnect::NRdma::CreateCqActor(-1, icConfig.GetRdmaMaxWr(), rdmaCqMode, interconectCounters.Get()),
                        TMailboxType::ReadAsFilled, interconnectPoolId));

                // Interconnect uses rdma mem pool directly
                const auto counters = GetServiceCounters(appData->Counters, "utils");
                NInterconnect::NRdma::TMemPoolSettings memPoolSettings;
                memPoolSettings.SizeLimitMb = icConfig.GetRdmaMemPoolSizeLimitMb();
                icCommon->RdmaMemPool = NInterconnect::NRdma::CreateSlotMemPool(counters.Get(), memPoolSettings);
                // Clients via wrapper to handle allocation fail
                setup->RcBufAllocator = std::make_shared<TRdmaAllocatorWithFallback>(icCommon->RdmaMemPool);
            }
            icCommon->NameserviceId = nameserviceId;
            icCommon->MonCounters = interconectCounters;
            icCommon->ChannelsConfig = channels;
            icCommon->Settings = settings;
            icCommon->DestructorId = GetDestructActorID();
            icCommon->DestructorQueueSize = destructorQueueSize;
            icCommon->HandshakeBallastSize = icConfig.GetHandshakeBallastSize();
            icCommon->LocalScopeId = ScopeId.GetInterconnectScopeId();
            icCommon->Cookie = icConfig.GetSuppressConnectivityCheck() ? TString() : CreateGuidAsString();

            if (icConfig.HasOutgoingHandshakeInflightLimit()) {
                icCommon->OutgoingHandshakeInflightLimit = icConfig.GetOutgoingHandshakeInflightLimit();

                // create handshake broker actor
                setup->LocalServices.emplace_back(MakeHandshakeBrokerOutId(), TActorSetupCmd(
                        CreateHandshakeBroker(*icCommon->OutgoingHandshakeInflightLimit),
                        TMailboxType::ReadAsFilled, systemPoolId));
            }

#define CHANNEL(NAME) {TInterconnectChannels::NAME, #NAME}
            icCommon->ChannelName = {
                CHANNEL(IC_COMMON),
                CHANNEL(IC_BLOBSTORAGE),
                CHANNEL(IC_BLOBSTORAGE_ASYNC_DATA),
                CHANNEL(IC_BLOBSTORAGE_SYNCER),
                CHANNEL(IC_BLOBSTORAGE_DISCOVER),
                CHANNEL(IC_BLOBSTORAGE_SMALL_MSG),
                CHANNEL(IC_TABLETS_SMALL),
                CHANNEL(IC_TABLETS_MEDIUM),
                CHANNEL(IC_TABLETS_LARGE),
            };

            if (icConfig.GetEnforceScopeValidation()) {
                icCommon->EventFilter = std::make_shared<TEventFilter>();
                RegisterBlobStorageEventScopes(icCommon->EventFilter);
                RegisterStateStorageEventScopes(icCommon->EventFilter);
            }

            if (const auto& whiteboardId = NNodeWhiteboard::MakeNodeWhiteboardServiceId(NodeId)) {
                icCommon->InitWhiteboard = [whiteboardId](ui16 port, TActorSystem *actorSystem) {
                    actorSystem->Send(whiteboardId, new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateAddEndpoint("ic", Sprintf(":%d", port)));
                };
                icCommon->UpdateWhiteboard = [whiteboardId](const TWhiteboardSessionStatus& data) {
                    auto update = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvNodeStateUpdate>();
                    auto& record = update->Record;
                    record.SetPeerNodeId(data.PeerNodeId);
                    record.SetPeerName(data.PeerName);
                    record.SetConnected(data.Connected);
                    record.SetConnectTime(data.ConnectTime);
                    record.SetConnectStatus(static_cast<NKikimrWhiteboard::EFlag>(static_cast<int>(data.ConnectStatus) + 1/*GREY = 0, GREEN = 1 ....*/));
                    record.SetClockSkewUs(data.ClockSkewUs);
                    record.SetPingTimeUs(data.PingTimeUs);
                    record.SetUtilization(data.Utilization);
                    record.MutableScopeId()->SetX1(data.ScopeId.first);
                    record.MutableScopeId()->SetX2(data.ScopeId.second);
                    record.SetBytesWritten(data.BytesWrittenToSocket);
                    if (data.SessionClosed) {
                        record.SetSessionState(NKikimrWhiteboard::TNodeStateInfo::CLOSED);
                    } else if (data.SessionPendingConnection) {
                        record.SetSessionState(NKikimrWhiteboard::TNodeStateInfo::PENDING_CONNECTION);
                    } else if (data.SessionConnected) {
                        record.SetSessionState(NKikimrWhiteboard::TNodeStateInfo::CONNECTED);
                    }
                    record.SetSameScope(data.SameScope);
                    if (data.PeerBridgePileName) {
                        record.SetPeerBridgePileName(data.PeerBridgePileName);
                    }
                    data.ActorSystem->Send(whiteboardId, update.release());
                };
                class TNetworkUtilizationUpdater : public TActorBootstrapped<TNetworkUtilizationUpdater> {
                    TIntrusivePtr<TInterconnectProxyCommon> Common;

                public:
                    TNetworkUtilizationUpdater(TIntrusivePtr<TInterconnectProxyCommon> common)
                        : Common(std::move(common))
                    {}

                    void Bootstrap() {
                        auto ev = std::make_unique<NNodeWhiteboard::TEvWhiteboard::TEvSystemStateUpdate>();
                        ev->Record.SetNetworkUtilization(Common->CalculateNetworkUtilization());
                        Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(SelfId().NodeId()), ev.release());
                        Become(&TThis::StateFunc, TDuration::Seconds(10), new TEvents::TEvWakeup);
                    }

                    STRICT_STFUNC(StateFunc, cFunc(TEvents::TSystem::Wakeup, Bootstrap));
                };
                setup->LocalServices.emplace_back(TActorId(), TActorSetupCmd(new TNetworkUtilizationUpdater(icCommon),
                    TMailboxType::ReadAsFilled, systemPoolId));
            }

            if (const auto& mon = appData->Mon) {
                icCommon->RegisterMonPage = [mon](const TString& path, const TString& title, TActorSystem *actorSystem, const TActorId& actorId) {
                    NMonitoring::TIndexMonPage *page = mon->RegisterIndexPage("actors", "Actors")->RegisterIndexPage("interconnect", "Interconnect");
                    mon->RegisterActorPage(page, path, title, false, actorSystem, actorId, /*useAuth=*/true, /*sortPages=*/false);
                };
                setup->LocalServices.emplace_back(NInterconnect::MakeInterconnectMonActorId(NodeId), TActorSetupCmd(
                    NInterconnect::CreateInterconnectMonActor(icCommon), TMailboxType::ReadAsFilled, systemPoolId));
            }

            if (nsConfig.HasClusterUUID()) {
                icCommon->ClusterUUID = nsConfig.GetClusterUUID();
            }

            for (const auto& item: nsConfig.GetAcceptUUID()) {
                icCommon->AcceptUUID.emplace_back(item);
            }

            if (!nsConfig.GetSuppressVersionCheck() && !Config.GetFeatureFlags().GetSuppressCompatibilityCheck()) {
                icCommon->VersionInfo = VERSION;
                CheckVersionTag();

                icCommon->CompatibilityInfo = TString();
                bool success = CompatibilityInfo.MakeStored(NKikimrConfig::TCompatibilityRule::Interconnect).SerializeToString(&*icCommon->CompatibilityInfo);
                Y_ABORT_UNLESS(success);
                icCommon->ValidateCompatibilityInfo = [&](const TString& peer, TString& errorReason) {
                    NKikimrConfig::TStoredCompatibilityInfo peerPB;
                    if (!peerPB.ParseFromString(peer)) {
                        errorReason = "Cannot parse given CompatibilityInfo";
                        return false;
                    }
                    return CompatibilityInfo.CheckCompatibility(&peerPB, NKikimrConfig::TCompatibilityRule::Interconnect, errorReason);
                };

                icCommon->ValidateCompatibilityOldFormat = [&](const NActors::TInterconnectProxyCommon::TVersionInfo& peer, TString& errorReason) {
                    return CompatibilityInfo.CheckCompatibility(peer, NKikimrConfig::TCompatibilityRule::Interconnect, errorReason);
                };
            }

            setup->LocalServices.emplace_back(GetDestructActorID(), TActorSetupCmd(new TDestructActor,
                TMailboxType::ReadAsFilled, interconnectPoolId));

            if (nsConfig.GetType() != NKikimrConfig::TStaticNameserviceConfig::NS_EXTERNAL) {
                Y_ABORT_UNLESS(!table->StaticNodeTable.empty());
            }

            if (Config.HasBridgeConfig()) {
                // when we work in Bridge mode, we need special actor to ensure connectivity check between nodes
                const auto& bridge = Config.GetBridgeConfig();
                TString name;

                if (Config.GetDynamicNodeConfig().HasNodeInfo()) {
                    const auto& nodeInfo = Config.GetDynamicNodeConfig().GetNodeInfo();
                    Y_ABORT_UNLESS(nodeInfo.HasLocation());
                    const auto& bridgePileName = TNodeLocation(nodeInfo.GetLocation()).GetBridgePileName();
                    Y_ABORT_UNLESS(bridgePileName);
                    name = *bridgePileName;
                } else {
                    const auto it = table->StaticNodeTable.find(NodeId);
                    Y_ABORT_UNLESS(it != table->StaticNodeTable.end());
                    const auto& entry = it->second;
                    const auto& bridgePileName = entry.Location.GetBridgePileName();
                    Y_ABORT_UNLESS(bridgePileName);
                    name = *bridgePileName;
                }

                size_t i;
                for (i = 0; i < bridge.PilesSize(); ++i) {
                    if (bridge.GetPiles(i).GetName() == name) {
                        break;
                    }
                }
                Y_ABORT_UNLESS(i != bridge.PilesSize());
                const auto selfBridgePileId = TBridgePileId::FromPileIndex(i);

                const TActorId actorId = MakeDistconfBridgeConnectionCheckerActorId();
                setup->LocalServices.emplace_back(actorId, TActorSetupCmd(
                    CreateDistconfBridgeConnectionCheckerActor(selfBridgePileId),
                    TMailboxType::ReadAsFilled,
                    interconnectPoolId));
                icCommon->ConnectionCheckerActorIds.push_back(actorId);
            }

            ui32 maxNode = 0;
            for (const auto &node : table->StaticNodeTable) {
                maxNode = Max(maxNode, node.first);
            }
            setup->Interconnect.ProxyActors.resize(maxNode + 1);
            setup->Interconnect.ProxyWrapperFactory = CreateProxyWrapperFactory(icCommon, interconnectPoolId);

            std::unordered_set<ui32> staticIds;

            for (const auto& node : table->StaticNodeTable) {
                const ui32 destId = node.first;
                if (destId != NodeId) {
                    staticIds.insert(destId);
                    setup->Interconnect.ProxyActors[destId] = TActorSetupCmd(new TInterconnectProxyTCP(destId, icCommon),
                        TMailboxType::ReadAsFilled, interconnectPoolId);
                } else {
                    TFederatedQueryInitializer::SetIcPort(node.second.second);
                    icCommon->TechnicalSelfHostName = node.second.Host;
                    TString address;
                    if (node.second.first)
                        address = node.second.first;
                    auto listener = std::make_unique<TInterconnectListenerTCP>(
                        address, node.second.second, icCommon);
                    if (int err = listener->Bind()) {
                        ythrow yexception()
                            << "Failed to set up IC listener on port " << node.second.second
                            << " errno# " << err << " (" << strerror(err) << ")";
                    }
                    setup->LocalServices.emplace_back(MakeInterconnectListenerActorId(false), TActorSetupCmd(listener.release(),
                        TMailboxType::ReadAsFilled, interconnectPoolId));
                }
            }

            // Prepare listener for dynamic node.
            if (Config.GetDynamicNodeConfig().HasNodeInfo()) {
                auto &info = Config.GetDynamicNodeConfig().GetNodeInfo();
                icCommon->TechnicalSelfHostName = info.GetHost();

                TString address;
                if (info.GetAddress()) {
                    address = info.GetAddress();
                }
                auto listener = std::make_unique<TInterconnectListenerTCP>(address, info.GetPort(), icCommon);
                if (int err = listener->Bind()) {
                    ythrow yexception()
                        << "Failed to set up IC listener on port " << info.GetPort()
                        << " errno# " << err << " (" << strerror(err) << ")";
                }
                setup->LocalServices.emplace_back(MakeInterconnectListenerActorId(true), TActorSetupCmd(listener.release(),
                    TMailboxType::ReadAsFilled, interconnectPoolId));
            }

            if (!IsServiceInitialized(setup, MakeInterconnectListenerActorId(false)) && !IsServiceInitialized(setup, MakeInterconnectListenerActorId(true))) {
                if (Config.HasFederatedQueryConfig() && Config.GetFederatedQueryConfig().GetEnabled()) {
                    auto& nodesManagerConfig = Config.GetFederatedQueryConfig().GetNodesManager();
                    if (nodesManagerConfig.GetEnabled()) {
                        TFederatedQueryInitializer::SetIcPort(nodesManagerConfig.GetPort());
                        icCommon->TechnicalSelfHostName = nodesManagerConfig.GetHost();
                        auto listener = std::make_unique<TInterconnectListenerTCP>("", nodesManagerConfig.GetPort(), icCommon);
                        if (int err = listener->Bind()) {
                            ythrow yexception()
                                << "Failed to set up IC listener on port " << nodesManagerConfig.GetPort()
                                << " errno# " << err << " (" << strerror(err) << ")";
                        }
                        setup->LocalServices.emplace_back(MakeInterconnectListenerActorId(true), TActorSetupCmd(listener.release(),
                            TMailboxType::ReadAsFilled, interconnectPoolId));
                    }
                }
            }

            // create load responder for interconnect
            // TODO(alexvru): pool?
            setup->LocalServices.emplace_back(NInterconnect::MakeLoadResponderActorId(NodeId),
                TActorSetupCmd(NInterconnect::CreateLoadResponderActor(), TMailboxType::ReadAsFilled, systemPoolId));
        }
    }

    if (Config.HasTracingConfig() && Config.GetTracingConfig().HasBackend()) {
        const auto& tracingConfig = Config.GetTracingConfig();
        const auto& tracingBackend = tracingConfig.GetBackend();

        std::unique_ptr<NWilson::IGrpcSigner> grpcSigner;
        if (tracingBackend.HasAuthConfig() && Factories && Factories->WilsonGrpcSignerFactory) {
            grpcSigner = Factories->WilsonGrpcSignerFactory(tracingBackend.GetAuthConfig());
            if (!grpcSigner) {
                Cerr << "Failed to initialize wilson grpc signer due to misconfiguration. Config provided: "
                        << tracingBackend.GetAuthConfig().DebugString() << Endl;
            }
        }

        std::unique_ptr<NActors::IActor> wilsonUploader;
        switch (tracingBackend.GetBackendCase()) {
            case NKikimrConfig::TTracingConfig::TBackendConfig::BackendCase::kOpentelemetry: {
                const auto& opentelemetry = tracingBackend.GetOpentelemetry();
                if (!(opentelemetry.HasCollectorUrl() && opentelemetry.HasServiceName())) {
                    Cerr << "Both collector_url and service_name should be present in opentelemetry backend config" << Endl;
                    break;
                }

                const auto& headersProto = opentelemetry.GetHeaders();
                TMap<TString, TString> headers;

                for (const auto& header : headersProto) {
                    headers.insert({header.first, header.second});
                }

                NWilson::TWilsonUploaderParams uploaderParams {
                    .CollectorUrl = opentelemetry.GetCollectorUrl(),
                    .ServiceName = opentelemetry.GetServiceName(),
                    .GrpcSigner = std::move(grpcSigner),
                    .Headers = headers,
                };

                if (tracingConfig.HasUploader()) {
                    const auto& uploaderConfig = tracingConfig.GetUploader();

#ifdef GET_FIELD_FROM_CONFIG
#error Macro collision
#endif
#define GET_FIELD_FROM_CONFIG(field) \
                    if (uploaderConfig.Has##field()) { \
                        uploaderParams.field = uploaderConfig.Get##field(); \
                    }

                    GET_FIELD_FROM_CONFIG(MaxExportedSpansPerSecond)
                    GET_FIELD_FROM_CONFIG(MaxSpansInBatch)
                    GET_FIELD_FROM_CONFIG(MaxBytesInBatch)
                    GET_FIELD_FROM_CONFIG(MaxBatchAccumulationMilliseconds)
                    GET_FIELD_FROM_CONFIG(SpanExportTimeoutSeconds)
                    GET_FIELD_FROM_CONFIG(MaxExportRequestsInflight)

#undef GET_FIELD_FROM_CONFIG
                }

                if (const auto& mon = appData->Mon) {
                    uploaderParams.RegisterMonPage = [mon](TActorSystem *actorSystem, const TActorId& actorId) {
                        NMonitoring::TIndexMonPage *actorsMonPage = mon->RegisterIndexPage("actors", "Actors");
                        mon->RegisterActorPage(actorsMonPage, "wilson_uploader", "Wilson Trace Uploader", false, actorSystem, actorId);
                    };
                }
                uploaderParams.Counters = GetServiceCounters(counters, "utils");

                wilsonUploader.reset(std::move(uploaderParams).CreateUploader());
                break;
            }

            case NKikimrConfig::TTracingConfig::TBackendConfig::BackendCase::BACKEND_NOT_SET: {
                Cerr << "No backend option was provided in tracing config" << Endl;
                break;
            }
        }
        if (wilsonUploader) {
            setup->LocalServices.emplace_back(
                NWilson::MakeWilsonUploaderId(),
                TActorSetupCmd(wilsonUploader.release(), TMailboxType::ReadAsFilled, appData->BatchPoolId));
        }
    }

    { // create retro collector
        setup->LocalServices.emplace_back(
                NRetroTracing::MakeRetroCollectorId(),
                TActorSetupCmd(NRetroTracing::CreateRetroCollector(), TMailboxType::ReadAsFilled,
                        appData->BatchPoolId));
    }

#if defined(OS_LINUX)
    if (Config.HasNbsConfig() && Config.GetNbsConfig().GetEnabled()) {
        auto ssProxy = NYdb::NBS::NStorage::CreateSSProxy(Config.GetNbsConfig().GetNbsStorageConfig());

        setup->LocalServices.emplace_back(
            NYdb::NBS::NStorage::MakeSSProxyServiceId(),
            TActorSetupCmd(
                ssProxy.release(),
                TMailboxType::Revolving,
                appData->UserPoolId));
    }
#endif
}

// TImmediateControlBoardInitializer

TImmediateControlBoardInitializer::TImmediateControlBoardInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TImmediateControlBoardInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        MakeIcbId(NodeId),
        TActorSetupCmd(CreateImmediateControlActor(appData->Icb, appData->Dcb, appData->Counters), TMailboxType::ReadAsFilled, appData->UserPoolId)
    ));
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        TActorId(),
        TActorSetupCmd(NConsole::CreateImmediateControlsConfigurator(appData->Icb,
                                                                     Config.GetImmediateControlsConfig()),
                       TMailboxType::ReadAsFilled, appData->UserPoolId)
    ));
}


// TBSNodeWardenInitializer

TBSNodeWardenInitializer::TBSNodeWardenInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TBSNodeWardenInitializer::InitializeServices(NActors::TActorSystemSetup* setup,
                                                  const NKikimr::TAppData* appData) {
    TIntrusivePtr<TNodeWardenConfig> nodeWardenConfig(new TNodeWardenConfig(new TRealPDiskServiceFactory()));
    if (Config.HasBlobStorageConfig()) {
        const auto& bsc = Config.GetBlobStorageConfig();
        nodeWardenConfig->FeatureFlags = Config.GetFeatureFlags();
        nodeWardenConfig->BlobStorageConfig.CopyFrom(bsc);
        if (Config.HasNameserviceConfig()) {
            nodeWardenConfig->NameserviceConfig.CopyFrom(Config.GetNameserviceConfig());
        }
        if (Config.HasVDiskConfig()) {
            nodeWardenConfig->AllVDiskKinds->Merge(Config.GetVDiskConfig());
        }
        if (Config.HasDriveModelConfig()) {
            nodeWardenConfig->AllDriveModels->Merge(Config.GetDriveModelConfig());
        }
        if (bsc.HasCacheFilePath()) {
            std::unordered_map<char, TString> vars{{'n', ToString(NodeId)}};
            for (const auto& node : Config.GetNameserviceConfig().GetNode()) {
                if (node.GetNodeId() == NodeId) {
                    vars['h'] = node.GetHost();
                    vars['p'] = ToString(node.GetPort());
                    break;
                }
            }
            if (Config.HasDynamicNodeConfig()) {
                const auto& dyn = Config.GetDynamicNodeConfig();
                if (dyn.HasNodeInfo()) {
                    const auto& ni = dyn.GetNodeInfo();
                    vars['h'] = ni.GetHost();
                    vars['p'] = ToString(ni.GetPort());
                }
            }
            nodeWardenConfig->CacheAccessor = CreateFileCacheAccessor(bsc.GetCacheFilePath(), vars);
        }
        nodeWardenConfig->CachePDisks = bsc.GetCachePDisks();
        nodeWardenConfig->CacheVDisks = bsc.GetCacheVDisks();
        nodeWardenConfig->EnableVDiskCooldownTimeout = true;
    }
    if (Config.HasDomainsConfig()) {
        nodeWardenConfig->DomainsConfig.emplace(Config.GetDomainsConfig());
    }
    if (Config.HasSelfManagementConfig()) {
        nodeWardenConfig->SelfManagementConfig.emplace(Config.GetSelfManagementConfig());
    }
    if (Config.HasBridgeConfig()) {
        nodeWardenConfig->BridgeConfig.emplace(Config.GetBridgeConfig());
    }
    if (Config.HasDynamicNodeConfig()) {
        nodeWardenConfig->DynamicNodeConfig.emplace(Config.GetDynamicNodeConfig());
    }

    if (Config.HasConfigDirPath()) {
        nodeWardenConfig->ConfigDirPath = Config.GetConfigDirPath();
    }

    if (Config.HasStoredConfigYaml()) {
        nodeWardenConfig->YamlConfig.emplace(Config.GetStoredConfigYaml());
    }

    nodeWardenConfig->StartupConfigYaml = Config.GetStartupConfigYaml();
    nodeWardenConfig->StartupStorageYaml = Config.HasStartupStorageYaml()
        ? std::make_optional(Config.GetStartupStorageYaml())
        : std::nullopt;

    ObtainTenantKey(&nodeWardenConfig->TenantKey, Config.GetKeyConfig());
    ObtainStaticKey(&nodeWardenConfig->StaticKey);
    ObtainPDiskKey(&nodeWardenConfig->PDiskKey, Config.GetPDiskKeyConfig());

    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeBlobStorageNodeWardenID(NodeId),
                                                                       TActorSetupCmd(CreateBSNodeWarden(nodeWardenConfig.Release()),
                                                                                      TMailboxType::ReadAsFilled, appData->SystemPoolId)));

    setup->LocalServices.emplace_back(MakeUniversalSchedulerActorId(), TActorSetupCmd(CreateUniversalSchedulerActor(),
        TMailboxType::ReadAsFilled, appData->SystemPoolId));
}

// TStateStorageServiceInitializer

template<typename TCreateFunc>
void StartLocalStateStorageReplicas(TCreateFunc createFunc, TStateStorageInfo *info, ui32 poolId, TActorSystemSetup &setup) {
    ui32 index = 0;
    for (auto &ringGroup : info->RingGroups) {
        for (auto &ring : ringGroup.Rings) {
            for (TActorId replica : ring.Replicas) {
                if (replica.NodeId() == setup.NodeId) {
                    setup.LocalServices.emplace_back(
                        replica,
                        TActorSetupCmd(createFunc(info, index), TMailboxType::ReadAsFilled, poolId));
                }
                ++index;
            }
        }
    }
}

TStateStorageServiceInitializer::TStateStorageServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TStateStorageServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    TIntrusivePtr<TStateStorageInfo> ssrInfo;
    TIntrusivePtr<TStateStorageInfo> ssbInfo;
    TIntrusivePtr<TStateStorageInfo> sbrInfo;

    std::unique_ptr<IActor> proxyActor;

    for (const NKikimrConfig::TDomainsConfig::TStateStorage &ssconf : Config.GetDomainsConfig().GetStateStorage()) {
        Y_ABORT_UNLESS(ssconf.GetSSId() == 1);

        BuildStateStorageInfos(ssconf, ssrInfo, ssbInfo, sbrInfo);

        StartLocalStateStorageReplicas(CreateStateStorageReplica, ssrInfo.Get(), appData->SystemPoolId, *setup);
        StartLocalStateStorageReplicas(CreateStateStorageBoardReplica, ssbInfo.Get(), appData->SystemPoolId, *setup);
        StartLocalStateStorageReplicas(CreateSchemeBoardReplica, sbrInfo.Get(), appData->SystemPoolId, *setup);

        proxyActor.reset(CreateStateStorageProxy(ssrInfo, ssbInfo, sbrInfo));
    }
    if (!proxyActor) {
        proxyActor.reset(CreateStateStorageProxyStub());
    }

    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeStateStorageProxyID(),
        TActorSetupCmd(proxyActor.release(), TMailboxType::ReadAsFilled, appData->SystemPoolId)));

    setup->LocalServices.emplace_back(
        TActorId(),
        TActorSetupCmd(CreateTenantNodeEnumerationPublisher(), TMailboxType::HTSwap, appData->SystemPoolId)
    );
}

// TLocalServiceInitializer

TLocalServiceInitializer::TLocalServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TLocalServiceInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {
    // choose pool id for important tablets
    ui32 importantPoolId = appData->UserPoolId;
    if (Config.GetFeatureFlags().GetImportantTabletsUseSystemPool()) {
        importantPoolId = appData->SystemPoolId;
    }

    // setup local
    TLocalConfig::TPtr localConfig(new TLocalConfig());

    std::unordered_map<TTabletTypes::EType, NKikimrLocal::TTabletAvailability> tabletAvailabilities;
    for (const auto& availability : Config.GetDynamicNodeConfig().GetTabletAvailability()) {
        tabletAvailabilities.emplace(availability.GetType(), availability);
    }

    auto addToLocalConfig = [&localConfig, &tabletAvailabilities, tabletPool = appData->SystemPoolId](TTabletTypes::EType tabletType,
                                                                                                      TTabletSetupInfo::TTabletCreationFunc op,
                                                                                                      NActors::TMailboxType::EType mailboxType,
                                                                                                      ui32 poolId) {
        auto availIt = tabletAvailabilities.find(tabletType);
        auto localIt = localConfig->TabletClassInfo.emplace(tabletType, new TTabletSetupInfo(op, mailboxType, poolId, TMailboxType::ReadAsFilled, tabletPool)).first;
        if (availIt != tabletAvailabilities.end()) {
            localIt->second.MaxCount = availIt->second.GetMaxCount();
            localIt->second.Priority = availIt->second.GetPriority();
        }
    };

    addToLocalConfig(TTabletTypes::SchemeShard, &CreateFlatTxSchemeShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::DataShard, &CreateDataShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::KeyValue, &CreateKeyValueFlat, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::PersQueue, &CreatePersQueue, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::PersQueueReadBalancer, &CreatePersQueueReadBalancer, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::Coordinator, &CreateFlatTxCoordinator, TMailboxType::Revolving, importantPoolId);
    addToLocalConfig(TTabletTypes::Mediator, &CreateTxMediator, TMailboxType::Revolving, importantPoolId);
    addToLocalConfig(TTabletTypes::Kesus, &NKesus::CreateKesusTablet, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::Hive, &CreateDefaultHive, TMailboxType::ReadAsFilled, importantPoolId);
    addToLocalConfig(TTabletTypes::SysViewProcessor, &NSysView::CreateSysViewProcessor, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::TestShard, &NTestShard::CreateTestShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::ColumnShard, &CreateColumnShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::SequenceShard, &NSequenceShard::CreateSequenceShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::ReplicationController, &NReplication::CreateController, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::BlobDepot, &NBlobDepot::CreateBlobDepot, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::StatisticsAggregator, &NStat::CreateStatisticsAggregator, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::GraphShard, &NGraph::CreateGraphShard, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::BackupController, &NBackup::CreateBackupController, TMailboxType::ReadAsFilled, appData->UserPoolId);
#if defined(OS_LINUX)
    addToLocalConfig(TTabletTypes::BlockStoreVolumeDirect, &NYdb::NBS::NStorage::CreateVolumeTablet, TMailboxType::ReadAsFilled, appData->UserPoolId);
    addToLocalConfig(TTabletTypes::BlockStorePartitionDirect, &NYdb::NBS::NBlockStore::NStorage::NPartitionDirect::CreatePartitionTablet, TMailboxType::ReadAsFilled, appData->UserPoolId);
#endif

    TTenantPoolConfig::TPtr tenantPoolConfig = new TTenantPoolConfig(Config.GetTenantPoolConfig(), localConfig);
    if (!tenantPoolConfig->IsEnabled && !tenantPoolConfig->StaticSlots.empty())
        Y_ABORT("Tenant slots are not allowed in disabled pool");

    setup->LocalServices.push_back(std::make_pair(MakeTenantPoolRootID(),
        TActorSetupCmd(CreateTenantPool(tenantPoolConfig), TMailboxType::ReadAsFilled, 0)));

    setup->LocalServices.push_back(std::make_pair(
        TActorId(),
        TActorSetupCmd(CreateLabelsMaintainer(Config.GetMonitoringConfig()),
                       TMailboxType::ReadAsFilled, 0)));

    setup->LocalServices.emplace_back(NTestShard::MakeStateServerInterfaceActorId(), TActorSetupCmd(
        NTestShard::CreateStateServerInterfaceActor(nullptr), TMailboxType::ReadAsFilled, 0));

    NKesus::AddKesusProbesList();
}

// TSharedCacheInitializer

TSharedCacheInitializer::TSharedCacheInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TSharedCacheInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {
    NKikimrSharedCache::TSharedCacheConfig config;
    if (Config.HasBootstrapConfig() && Config.GetBootstrapConfig().HasSharedCacheConfig()) {
        config.MergeFrom(Config.GetBootstrapConfig().GetSharedCacheConfig());
    }
    if (Config.HasSharedCacheConfig()) {
        config.MergeFrom(Config.GetSharedCacheConfig());
    }

    auto* actor = NSharedCache::CreateSharedPageCache(config, appData->Counters);
    setup->LocalServices.emplace_back(NSharedCache::MakeSharedPageCacheId(0),
        TActorSetupCmd(actor, TMailboxType::ReadAsFilled, appData->UserPoolId));
}

// TBlobCacheInitializer

TBlobCacheInitializer::TBlobCacheInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TBlobCacheInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData) {

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> blobCacheGroup = tabletGroup->GetSubgroup("type", "BLOB_CACHE");

    std::optional<ui64> maxCacheSize;
    if (Config.HasBlobCacheConfig()) {
        if (Config.GetBlobCacheConfig().HasMaxSizeBytes()) {
            maxCacheSize = Config.GetBlobCacheConfig().GetMaxSizeBytes();
        }
    }
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(NBlobCache::MakeBlobCacheServiceId(),
        TActorSetupCmd(NBlobCache::CreateBlobCache(maxCacheSize, blobCacheGroup), TMailboxType::ReadAsFilled, appData->UserPoolId)));
}

// TLoggerInitializer

TLoggerInitializer::TLoggerInitializer(const TKikimrRunConfig& runConfig,
                                       TIntrusivePtr<NActors::NLog::TSettings> logSettings,
                                       std::shared_ptr<TLogBackend> logBackend)
    : IKikimrServicesInitializer(runConfig)
    , LogSettings(logSettings)
    , LogBackend(logBackend)
    , PathToConfigCacheFile(runConfig.PathToConfigCacheFile)
{
}

void TLoggerInitializer::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData) {
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> utilsCounters = GetServiceCounters(appData->Counters, "utils");

    // log settings must be initialized before calling this method
    NActors::TLoggerActor *loggerActor = new NActors::TLoggerActor(LogSettings, LogBackend, utilsCounters);
    NActors::TActorSetupCmd loggerActorCmd(loggerActor, NActors::TMailboxType::HTSwap, appData->IOPoolId);
    std::pair<NActors::TActorId, NActors::TActorSetupCmd> loggerActorPair(LogSettings->LoggerActorId, std::move(loggerActorCmd));
    setup->LocalServices.push_back(std::move(loggerActorPair));

    IActor *configurator;
    if (PathToConfigCacheFile && !appData->FeatureFlags.GetEnableConfigurationCache()) {
        configurator = NConsole::CreateLogSettingsConfigurator(PathToConfigCacheFile);
    } else {
        configurator = NConsole::CreateLogSettingsConfigurator();
    }

    setup->LocalServices.emplace_back(TActorId(),
                                      TActorSetupCmd(configurator, TMailboxType::HTSwap, appData->UserPoolId));
}

// TSchedulerActorInitializer

TSchedulerActorInitializer::TSchedulerActorInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TSchedulerActorInitializer::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData) {
    auto& systemConfig = Config.GetActorSystemConfig();
    NActors::IActor *schedulerActor = CreateSchedulerActor(NActorSystemConfigHelpers::CreateSchedulerConfig(systemConfig.GetScheduler()));
    if (schedulerActor) {
        NActors::TActorSetupCmd schedulerActorCmd(schedulerActor, NActors::TMailboxType::ReadAsFilled, appData->SystemPoolId);
        setup->LocalServices.emplace_back(MakeSchedulerActorId(), std::move(schedulerActorCmd));
    }
}

// TProfilerInitializer

TProfilerInitializer::TProfilerInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TProfilerInitializer::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData) {
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> utilsCounters = GetServiceCounters(appData->Counters, "utils");

    TActorSetupCmd profilerSetup(CreateProfilerActor(utilsCounters, "/var/tmp"), TMailboxType::HTSwap, 0);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(MakeProfilerID(NodeId), std::move(profilerSetup)));
}

// TResourceBrokerInitializer

TResourceBrokerInitializer::TResourceBrokerInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TResourceBrokerInitializer::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData) {
    NKikimrResourceBroker::TResourceBrokerConfig config = NResourceBroker::MakeDefaultConfig();

    if (Config.HasBootstrapConfig() && Config.GetBootstrapConfig().HasCompactionBroker()) {
        Y_ABORT("Legacy CompactionBroker configuration is no longer supported");
    }

    if (Config.HasBootstrapConfig() && Config.GetBootstrapConfig().HasResourceBroker()) {
        NResourceBroker::MergeConfigUpdates(config, Config.GetBootstrapConfig().GetResourceBroker());
    }

    if (Config.HasResourceBrokerConfig()) {
        NResourceBroker::MergeConfigUpdates(config, Config.GetResourceBrokerConfig());
    }

    auto counters = GetServiceCounters(appData->Counters, "tablets");
    TActorSetupCmd actorSetup = { NResourceBroker::CreateResourceBrokerActor(config, counters),
                                  TMailboxType::ReadAsFilled, appData->UserPoolId };
    setup->LocalServices.push_back(std::make_pair(NResourceBroker::MakeResourceBrokerID(), std::move(actorSetup)));
}

// TRestartsCountPublisher

void TRestartsCountPublisher::PublishRestartsCount(const ::NMonitoring::TDynamicCounters::TCounterPtr& counter,
                                                      const TString& restartsCountFile) {
    if (restartsCountFile.size()) {
        try {
            TUnbufferedFileInput fileInput(restartsCountFile);
            const TString content = fileInput.ReadAll();
            *counter = FromString<ui32>(content);
        } catch (yexception) {
            *counter = 0;
        }
        TUnbufferedFileOutput fileOutput(restartsCountFile);
        fileOutput.Write(ToString(*counter+1));
    }
}

TRestartsCountPublisher::TRestartsCountPublisher(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TRestartsCountPublisher::InitializeServices(
            NActors::TActorSystemSetup* setup,
            const NKikimr::TAppData* appData) {
    Y_UNUSED(setup);
    const TIntrusivePtr<::NMonitoring::TDynamicCounters> utilsCounters = GetServiceCounters(appData->Counters, "utils");

    if (Config.HasRestartsCountConfig()) {
        const auto& restartsCountConfig = Config.GetRestartsCountConfig();
        if (restartsCountConfig.HasRestartsCountFile()) {
            PublishRestartsCount(utilsCounters->GetCounter("RestartsCount", false), restartsCountConfig.GetRestartsCountFile());
        }
    }
}

// TTabletResolverInitializer

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

    ui32 workPoolId = appData->UserPoolId;
    if (appData->FeatureFlags.GetImportantTabletsUseSystemPool()) {
        switch (tabletType) {
            case TTabletTypes::Coordinator:
            case TTabletTypes::Mediator:
            case TTabletTypes::Hive:
                workPoolId = appData->SystemPoolId;
                break;
            default:
                break;
        }
    }

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
    if (Config.HasBootstrapConfig()) {
        setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
            TActorId(),
            TActorSetupCmd(CreateConfiguredTabletBootstrapper(Config.GetBootstrapConfig()), TMailboxType::HTSwap, appData->SystemPoolId)));
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

TGRpcServicesInitializer::TGRpcServicesInitializer(
    const TKikimrRunConfig& runConfig,
    std::shared_ptr<TModuleFactories> factories
)
    : IKikimrServicesInitializer(runConfig)
    , Factories(factories)
{}

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

            desc->ServedServices.insert(desc->ServedServices.end(), config.GetServices().begin(), config.GetServices().end());
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

            desc->ServedServices.insert(desc->ServedServices.end(), config.GetServices().begin(), config.GetServices().end());
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

                desc->ServedServices.insert(desc->ServedServices.end(), sx.GetServices().begin(), sx.GetServices().end());
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

        setup->LocalServices.emplace_back(
           NGRpcService::CreateGrpcPublisherServiceActorId(),
           TActorSetupCmd(CreateGrpcPublisherServiceActor(std::move(endpoints)), TMailboxType::ReadAsFilled, appData->UserPoolId)
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

// TPersQueueL2CacheInitializer

TPersQueueL2CacheInitializer::TPersQueueL2CacheInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TPersQueueL2CacheInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    static const ui64 DEFAULT_PQ_L2_MAX_SIZE_MB =
        NKikimrNodeLimits::TNodeLimitsConfig_TPersQueueNodeConfig::default_instance().GetSharedCacheSizeMb();
    static const TDuration DEFAULT_PQ_L2_KEEP_TIMEOUT = TDuration::Seconds(10);

    NPQ::TCacheL2Parameters params;
    params.MaxSizeMB = DEFAULT_PQ_L2_MAX_SIZE_MB;
    params.KeepTime = DEFAULT_PQ_L2_KEEP_TIMEOUT;

    if (Config.HasBootstrapConfig() && Config.GetBootstrapConfig().HasNodeLimits()) {
        auto nodeLimits = Config.GetBootstrapConfig().GetNodeLimits();
        if (nodeLimits.HasPersQueueNodeConfig()) {
            auto cfg = nodeLimits.GetPersQueueNodeConfig();
            if (cfg.HasSharedCacheSizeMb())
                params.MaxSizeMB = cfg.GetSharedCacheSizeMb();
            if (cfg.HasCacheKeepTimeSec())
                params.KeepTime = TDuration::Seconds(cfg.GetCacheKeepTimeSec());
        }
    }

    if (Config.HasPQConfig() && Config.GetPQConfig().HasPersQueueNodeConfig()) {
        auto cfg = Config.GetPQConfig().GetPersQueueNodeConfig();
        if (cfg.HasSharedCacheSizeMb())
            params.MaxSizeMB = cfg.GetSharedCacheSizeMb();
        if (cfg.HasCacheKeepTimeSec())
            params.KeepTime = TDuration::Seconds(cfg.GetCacheKeepTimeSec());
    }

    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> pqCacheGroup = tabletGroup->GetSubgroup("type", "PQ_CACHE");

    IActor* actor = NPQ::CreateNodePersQueueL2Cache(params, pqCacheGroup);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        NPQ::MakePersQueueL2CacheID(),
        TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId)));
}

// TNetClassifierInitializer

TNetClassifierInitializer::TNetClassifierInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TNetClassifierInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    IActor* actor = NNetClassifier::CreateNetClassifier();

    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        NNetClassifier::MakeNetClassifierID(),
        TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId)));
}

// TPersQueueClusterTracker

TPersQueueClusterTrackerInitializer::TPersQueueClusterTrackerInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TPersQueueClusterTrackerInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    IActor* actor = NPQ::NClusterTracker::CreateClusterTracker();
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        NPQ::NClusterTracker::MakeClusterTrackerID(),
        TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId)));
}

// TPersQueueDirectReadCache

TPersQueueDirectReadCacheInitializer::TPersQueueDirectReadCacheInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TPersQueueDirectReadCacheInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    IActor* actor = NPQ::CreatePQDReadCacheService(appData->Counters);
    setup->LocalServices.push_back(std::pair<TActorId, TActorSetupCmd>(
        NPQ::MakePQDReadCacheServiceActorId(),
        TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId)));
}

TMemProfMonitorInitializer::TMemProfMonitorInitializer(const TKikimrRunConfig& runConfig, TIntrusiveConstPtr<NMemory::IProcessMemoryInfoProvider> processMemoryInfoProvider)
    : IKikimrServicesInitializer(runConfig)
    , ProcessMemoryInfoProvider(std::move(processMemoryInfoProvider))
{}

void TMemProfMonitorInitializer::InitializeServices(
        NActors::TActorSystemSetup* setup,
        const NKikimr::TAppData* appData)
{
    TString filePathPrefix;

    if (Config.HasMonitoringConfig()) {
        filePathPrefix = Config.GetMonitoringConfig().GetMemAllocDumpPathPrefix();
    }

    IActor* monitorActor = CreateMemProfMonitor(
        TDuration::Seconds(1),
        ProcessMemoryInfoProvider,
        appData->Counters,
        filePathPrefix);

    setup->LocalServices.emplace_back(
        MakeMemProfMonitorID(NodeId),
        TActorSetupCmd(
            monitorActor,
            TMailboxType::HTSwap,
            appData->BatchPoolId));
}

TMemoryTrackerInitializer::TMemoryTrackerInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TMemoryTrackerInitializer::InitializeServices(
    NActors::TActorSystemSetup* setup,
    const NKikimr::TAppData* appData)
{
    auto* actor = NMemory::CreateMemoryTrackerActor(TDuration::MilliSeconds(20), appData->Counters);
    setup->LocalServices.emplace_back(
        TActorId(),
        TActorSetupCmd(actor, TMailboxType::HTSwap, appData->UserPoolId)
    );
}

TMemoryControllerInitializer::TMemoryControllerInitializer(const TKikimrRunConfig& runConfig, TIntrusiveConstPtr<NMemory::IProcessMemoryInfoProvider> processMemoryInfoProvider)
    : IKikimrServicesInitializer(runConfig)
    , ProcessMemoryInfoProvider(std::move(processMemoryInfoProvider))
{}

void TMemoryControllerInitializer::InitializeServices(
    NActors::TActorSystemSetup* setup,
    const NKikimr::TAppData* appData)
{
    auto* actor = NMemory::CreateMemoryController(TDuration::Seconds(1), ProcessMemoryInfoProvider,
        Config.GetMemoryControllerConfig(), NKikimrConfigHelpers::CreateMemoryControllerResourceBrokerConfig(Config),
        appData->Counters);
    setup->LocalServices.emplace_back(
        NMemory::MakeMemoryControllerId(0),
        TActorSetupCmd(actor, TMailboxType::HTSwap, appData->BatchPoolId)
    );
}

TQuoterServiceInitializer::TQuoterServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{}

void TQuoterServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    Y_UNUSED(appData);
    setup->LocalServices.push_back(std::make_pair(
        MakeQuoterServiceID(),
        TActorSetupCmd(CreateQuoterService(), TMailboxType::HTSwap, appData->SystemPoolId))
    );
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

        TDuration warmupDeadline;
        if (Config.GetTableServiceConfig().HasCompileCacheWarmupConfig() && !appData->TenantName.empty()) {
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

        auto federatedQuerySetupFactory = NKqp::MakeKqpFederatedQuerySetupFactory(setup, appData, Config);

        auto s3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
        auto proxy = NKqp::CreateKqpProxyService(Config.GetLogConfig(), Config.GetTableServiceConfig(),
            Config.GetQueryServiceConfig(), Config.GetTliConfig(), std::move(settings), Factories->QueryReplayBackendFactory, std::move(kqpProxySharedResources),
            federatedQuerySetupFactory, s3ActorsFactory
        );
        setup->LocalServices.push_back(std::make_pair(
            NKqp::MakeKqpProxyID(NodeId),
            TActorSetupCmd(proxy, TMailboxType::HTSwap, appData->UserPoolId)));

        // Create finalize script service
        auto finalize = NKqp::CreateKqpFinalizeScriptService(
            Config.GetQueryServiceConfig(), federatedQuerySetupFactory, s3ActorsFactory
        );
        setup->LocalServices.push_back(std::make_pair(
            NKqp::MakeKqpFinalizeScriptServiceId(NodeId),
            TActorSetupCmd(finalize, TMailboxType::HTSwap, appData->UserPoolId)));

        auto describeSchemaSecretsService = NKqp::TDescribeSchemaSecretsServiceFactory().CreateService();
        setup->LocalServices.push_back(std::make_pair(
            NKqp::MakeKqpDescribeSchemaSecretServiceId(NodeId),
            TActorSetupCmd(describeSchemaSecretsService, TMailboxType::HTSwap, appData->UserPoolId)));

        if (Config.GetTableServiceConfig().HasCompileCacheWarmupConfig() && !appData->TenantName.empty()) {
            auto warmupConfig = NKqp::ImportWarmupConfigFromProto(Config.GetTableServiceConfig().GetCompileCacheWarmupConfig());

            TString database = appData->TenantName;
            TString cluster = appData->DomainsInfo->Domain ? appData->DomainsInfo->Domain->Name : TString();

            TVector<NActors::TActorId> notifyActorIds = {
                NKqp::MakeKqpRmServiceID(NodeId),
                MakeGRpcServersManagerId(NodeId),
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

TExternalIndexInitializer::TExternalIndexInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TExternalIndexInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    NCSIndex::TConfig serviceConfig;
    if (Config.HasExternalIndexConfig()) {
        Y_ABORT_UNLESS(serviceConfig.DeserializeFromProto(Config.GetExternalIndexConfig()));
    }

    if (serviceConfig.IsEnabled()) {
        auto service = NCSIndex::CreateService(serviceConfig);
        setup->LocalServices.push_back(std::make_pair(
            NCSIndex::MakeServiceId(NodeId),
            TActorSetupCmd(service, TMailboxType::HTSwap, appData->UserPoolId)));
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

TReplicationServiceInitializer::TReplicationServiceInitializer(const TKikimrRunConfig& runConfig)
   : IKikimrServicesInitializer(runConfig)
{
}

void TReplicationServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    setup->LocalServices.emplace_back(
        NReplication::MakeReplicationServiceId(NodeId),
        TActorSetupCmd(NReplication::CreateReplicationService(), TMailboxType::HTSwap, appData->UserPoolId)
    );
}

TLocalPgWireServiceInitializer::TLocalPgWireServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TLocalPgWireServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    setup->LocalServices.emplace_back(
        NLocalPgWire::CreateLocalPgWireProxyId(),
        TActorSetupCmd(NLocalPgWire::CreateLocalPgWireProxy(), TMailboxType::HTSwap, appData->UserPoolId)
    );

    NPG::TListenerSettings settings;
    settings.Port = Config.GetLocalPgWireConfig().GetListeningPort();
    if (Config.GetLocalPgWireConfig().HasSslCertificate()) {
        settings.SslCertificatePem = Config.GetLocalPgWireConfig().GetSslCertificate();
    }

    if (Config.GetLocalPgWireConfig().HasAddress()) {
        settings.Address = Config.GetLocalPgWireConfig().GetAddress();
    }

    if (Config.GetLocalPgWireConfig().HasTcpNotDelay()) {
        settings.TcpNotDelay = Config.GetLocalPgWireConfig().GetTcpNotDelay();
    }

    setup->LocalServices.emplace_back(
        TActorId(),
        TActorSetupCmd(NPG::CreatePGListener(MakePollerActorId(), NLocalPgWire::CreateLocalPgWireProxyId(), settings),
            TMailboxType::HTSwap, appData->UserPoolId)
    );
}

TKafkaProxyServiceInitializer::TKafkaProxyServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TKafkaProxyServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    if (Config.GetKafkaProxyConfig().GetEnableKafkaProxy()) {
        NKafka::TListenerSettings settings;
        settings.Port = Config.GetKafkaProxyConfig().GetListeningPort();
        settings.SslCertificatePem = Config.GetKafkaProxyConfig().GetSslCertificate();
        settings.CertificateFile = Config.GetKafkaProxyConfig().GetCert();
        settings.PrivateKeyFile = Config.GetKafkaProxyConfig().GetKey();
        settings.TcpNotDelay = true;

        setup->LocalServices.emplace_back(
            NKafka::MakeKafkaDiscoveryCacheID(),
            TActorSetupCmd(CreateDiscoveryCache(NGRpcService::KafkaEndpointId),
                TMailboxType::HTSwap, appData->UserPoolId)
        );

        setup->LocalServices.emplace_back(
            NKafka::MakeTransactionsServiceID(NodeId),
            TActorSetupCmd(NKafka::CreateTransactionsCoordinator(),
                TMailboxType::HTSwap, appData->UserPoolId
            )
        );
        const auto &grpcConfig = Config.GetGRpcConfig();
        const TString &address = grpcConfig.GetHost() && grpcConfig.GetHost() != "[::]" ? grpcConfig.GetHost() : FQDNHostName();
        auto& kafkaMutableConfig = *Config.MutableKafkaProxyConfig();
        kafkaMutableConfig.SetPublicHost(grpcConfig.GetPublicHost() ? grpcConfig.GetPublicHost() : address);

        setup->LocalServices.emplace_back(
            TActorId(),
            TActorSetupCmd(NKafka::CreateKafkaListener(MakePollerActorId(), settings, Config.GetKafkaProxyConfig()),
                           TMailboxType::HTSwap, appData->UserPoolId)
        );

        IActor* metricsActor = CreateKafkaMetricsActor(NKafka::TKafkaMetricsSettings{appData->Counters});
        setup->LocalServices.emplace_back(
            NKafka::MakeKafkaMetricsServiceID(),
            TActorSetupCmd(metricsActor,
                TMailboxType::HTSwap, appData->UserPoolId)
        );
    }
}


TIcNodeCacheServiceInitializer::TIcNodeCacheServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TIcNodeCacheServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    if (appData->FeatureFlags.GetEnableIcNodeCache()) {
        setup->LocalServices.emplace_back(
            NIcNodeCache::CreateICNodesInfoCacheServiceId(),
            TActorSetupCmd(NIcNodeCache::CreateICNodesInfoCacheService(appData->Counters),
                           TMailboxType::HTSwap, appData->UserPoolId)
        );
    }
}

TDatabaseMetadataCacheInitializer::TDatabaseMetadataCacheInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TDatabaseMetadataCacheInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    setup->LocalServices.emplace_back(
        MakeDatabaseMetadataCacheId(NodeId),
        TActorSetupCmd(CreateDatabaseMetadataCache(appData->TenantName, appData->Counters), TMailboxType::HTSwap, appData->UserPoolId));
}

TGraphServiceInitializer::TGraphServiceInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig)
{
}

void TGraphServiceInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    setup->LocalServices.emplace_back(
        NGraph::MakeGraphServiceId(),
        TActorSetupCmd(NGraph::CreateGraphService(appData->TenantName), TMailboxType::HTSwap, appData->UserPoolId));
}

TAwsApiInitializer::TAwsApiInitializer(IGlobalObjectStorage& globalObjects)
    : GlobalObjects(globalObjects)
{
}

void TAwsApiInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    Y_UNUSED(setup);
    GlobalObjects.AddGlobalObject(std::make_shared<TAwsApiGuard>(appData->AwsClientConfig));
}

TOverloadManagerInitializer::TOverloadManagerInitializer(const TKikimrRunConfig& runConfig)
    : IKikimrServicesInitializer(runConfig) {
}

void TOverloadManagerInitializer::InitializeServices(NActors::TActorSystemSetup* setup, const NKikimr::TAppData* appData) {
    TIntrusivePtr<::NMonitoring::TDynamicCounters> tabletGroup = GetServiceCounters(appData->Counters, "tablets");
    TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup = tabletGroup->GetSubgroup("type", "CS_OVERLOAD_MANAGER");

    setup->LocalServices.push_back(std::make_pair(NColumnShard::NOverload::TOverloadManagerServiceOperator::MakeServiceId(),
        TActorSetupCmd(NColumnShard::NOverload::TOverloadManagerServiceOperator::CreateService(countersGroup), TMailboxType::HTSwap, appData->UserPoolId)));
}

#if defined(OS_LINUX)

TNbsServiceInitializer::TNbsServiceInitializer(const TKikimrRunConfig &runConfig)
     : IKikimrServicesInitializer(runConfig) {
}

void TNbsServiceInitializer::InitializeServices(NActors::TActorSystemSetup *setup, const NKikimr::TAppData *appData) {
    Y_UNUSED(setup);
    Y_UNUSED(appData);

    const auto& config = Config.GetNbsConfig();
    NYdb::NBS::NBlockStore::CreateNbsService(config);
}

#endif

} // namespace NKikimr::NKikimrServicesInitializers
