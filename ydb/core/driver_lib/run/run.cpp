#include "auto_config_initializer.h"
#include "run.h"
#include "service_initializer.h"
#include "kikimr_services_initializers.h"

#include <ydb/core/memory_controller/memory_controller.h>
#include <ydb/library/actors/core/events.h>
#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/event_local.h>
#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/core/mon.h>
#include <ydb/library/actors/core/mon_stats.h>
#include <ydb/library/actors/core/monotonic_provider.h>
#include <ydb/library/actors/core/process_stats.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/core/log_settings.h>

#include <ydb/library/actors/core/executor_pool_basic.h>
#include <ydb/library/actors/core/executor_pool_io.h>
#include <ydb/library/actors/core/scheduler_basic.h>

#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/poller_tcp.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_proxy.h>
#include <ydb/library/actors/interconnect/interconnect_tcp_server.h>
#include <ydb/library/actors/interconnect/interconnect_mon.h>
#include <ydb/core/config/init/dummy.h>

#include <ydb/core/control/immediate_control_board_actor.h>

#include <ydb/library/actors/protos/services_common.pb.h>
#include <ydb/core/cms/console/grpc_library_helper.h>
#include <ydb/core/keyvalue/keyvalue.h>
#include <ydb/core/formats/clickhouse_block.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/grpc_services/grpc_mon.h>
#include <ydb/core/log_backend/log_backend.h>
#include <ydb/core/mon/sync_http_mon.h>
#include <ydb/core/mon/async_http_mon.h>
#include <ydb/core/mon/crossref.h>
#include <ydb/core/mon_alloc/profiler.h>

#include <ydb/library/actors/util/affinity.h>

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/base/tabletid.h>
#include <ydb/core/base/channel_profiles.h>
#include <ydb/core/base/domain.h>
#include <ydb/core/base/feature_flags.h>
#include <ydb/core/base/nameservice.h>
#include <ydb/core/base/tablet_types.h>
#include <ydb/core/base/resource_profile.h>
#include <ydb/core/base/event_filter.h>
#include <ydb/core/base/statestorage_impl.h>
#include <ydb/library/services/services.pb.h>
#include <ydb/core/protos/alloc.pb.h>
#include <ydb/core/protos/http_config.pb.h>
#include <ydb/core/protos/datashard_config.pb.h>

#include <ydb/core/mind/local.h>
#include <ydb/core/mind/tenant_pool.h>
#include <ydb/core/base/hive.h>

#include <ydb/core/base/tablet_resolver.h>
#include <ydb/core/security/login_page.h>
#include <ydb/core/tablet/bootstrapper.h>
#include <ydb/core/tablet/resource_broker.h>
#include <ydb/core/tablet/node_tablet_monitor.h>
#include <ydb/core/tablet/tablet_list_renderer.h>
#include <ydb/core/tablet/tablet_monitoring_proxy.h>
#include <ydb/core/tablet/tablet_counters_aggregator.h>

#include <ydb/core/tx/tx.h>
#include <ydb/core/tx/datashard/datashard.h>
#include <ydb/core/tx/tx_proxy/proxy.h>
#include <ydb/core/tx/time_cast/time_cast.h>

#include <ydb/core/tablet_flat/tablet_flat_executed.h>

#include <ydb/core/mind/bscontroller/bsc.h>

#include <ydb/core/blobstorage/other/mon_get_blob_page.h>
#include <ydb/core/blobstorage/other/mon_blob_range_page.h>
#include <ydb/core/blobstorage/other/mon_vdisk_stream.h>

#include <ydb/public/lib/deprecated/client/msgbus_client.h>
#include <ydb/core/client/minikql_compile/mkql_compile_service.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/client/server/http_ping.h>

#include <ydb/library/grpc/server/actors/logger.h>

#include <ydb/services/auth/grpc_service.h>
#include <ydb/services/cms/grpc_service.h>
#include <ydb/services/dynamic_config/grpc_service.h>
#include <ydb/services/datastreams/grpc_service.h>
#include <ydb/services/discovery/grpc_service.h>
#include <ydb/services/fq/grpc_service.h>
#include <ydb/services/fq/private_grpc.h>
#include <ydb/services/fq/ydb_over_fq.h>
#include <ydb/services/kesus/grpc_service.h>
#include <ydb/services/keyvalue/grpc_service.h>
#include <ydb/services/local_discovery/grpc_service.h>
#include <ydb/services/maintenance/grpc_service.h>
#include <ydb/services/monitoring/grpc_service.h>
#include <ydb/services/persqueue_cluster_discovery/grpc_service.h>
#include <ydb/services/deprecated/persqueue_v0/persqueue.h>
#include <ydb/services/persqueue_v1/persqueue.h>
#include <ydb/services/persqueue_v1/topic.h>
#include <ydb/services/rate_limiter/grpc_service.h>
#include <ydb/services/replication/grpc_service.h>
#include <ydb/services/ydb/ydb_clickhouse_internal.h>
#include <ydb/services/ydb/ydb_dummy.h>
#include <ydb/services/ydb/ydb_export.h>
#include <ydb/services/ydb/ydb_import.h>
#include <ydb/services/backup/grpc_service.h>
#include <ydb/services/ydb/ydb_logstore.h>
#include <ydb/services/ydb/ydb_operation.h>
#include <ydb/services/ydb/ydb_query.h>
#include <ydb/services/ydb/ydb_scheme.h>
#include <ydb/services/ydb/ydb_scripting.h>
#include <ydb/services/ydb/ydb_table.h>
#include <ydb/services/ydb/ydb_object_storage.h>
#include <ydb/services/tablet/ydb_tablet.h>

#include <ydb/core/fq/libs/init/init.h>

#include <library/cpp/logger/global/global.h>
#include <library/cpp/sighandler/async_signals_handler.h>
#include <library/cpp/svnversion/svnversion.h>
#include <library/cpp/malloc/api/malloc.h>

#include <ydb/core/util/sig.h>
#include <ydb/core/util/stlog.h>

#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/tablet/node_tablet_monitor.h>

#include <ydb/library/actors/util/memory_track.h>
#include <ydb/library/actors/prof/tag.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/library/yql/minikql/invoke_builtins/mkql_builtins.h>

#include <util/charset/wide.h>
#include <util/folder/dirut.h>
#include <util/system/file.h>
#include <util/system/getpid.h>
#include <util/system/hostname.h>

#include <ydb/core/tracing/tablet_info.h>

namespace NKikimr {

class TDomainsInitializer : public IAppDataInitializer {
    const NKikimrConfig::TAppConfig& Config;

public:
    TDomainsInitializer(const TKikimrRunConfig& runConfig)
        : Config(runConfig.AppConfig)
    {
    }

    virtual void Initialize(NKikimr::TAppData* appData) override
    {
        appData->DomainsConfig = Config.GetDomainsConfig();
        // setup domain info
        appData->DomainsInfo = new TDomainsInfo();
        for (const NKikimrConfig::TDomainsConfig::TDomain &domain : Config.GetDomainsConfig().GetDomain()) {
            const ui32 domainId = domain.GetDomainId();
            const ui64 schemeRoot = domain.HasSchemeRoot() ? domain.GetSchemeRoot() : 0;
            const ui64 planResolution = domain.HasPlanResolution() ? domain.GetPlanResolution() : 500;
            const TString domainName = domain.HasName() ? domain.GetName() : Sprintf("domain-%" PRIu32, domainId);
            TDomainsInfo::TDomain::TStoragePoolKinds poolTypes;
            for (auto &type : domain.GetStoragePoolTypes()) {
                Y_ABORT_UNLESS(!poolTypes.contains(type.GetKind()), "duplicated slot type");
                poolTypes[type.GetKind()] = type.GetPoolConfig();
            }

            bool isExplicitTabletIds = domain.ExplicitCoordinatorsSize() + domain.ExplicitMediatorsSize() + domain.ExplicitAllocatorsSize();
            Y_ABORT_UNLESS(domain.SSIdSize() == 0 || (domain.SSIdSize() == 1 && domain.GetSSId(0) == 1));
            Y_ABORT_UNLESS(domain.HiveUidSize() == 0 || (domain.HiveUidSize() == 1 && domain.GetHiveUid(0) == 1));

            TDomainsInfo::TDomain::TPtr domainPtr = nullptr;
            if (isExplicitTabletIds) {
                domainPtr = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(domainName, domainId, schemeRoot,
                    planResolution, domain.GetExplicitCoordinators(), domain.GetExplicitMediators(),
                    domain.GetExplicitAllocators(), poolTypes);
            } else { // compatibility code
                std::vector<ui64> coordinators, mediators, allocators;
                for (ui64 x : domain.GetCoordinator()) {
                    coordinators.push_back(TDomainsInfo::MakeTxCoordinatorID(domainId, x));
                }
                for (ui64 x : domain.GetMediator()) {
                    mediators.push_back(TDomainsInfo::MakeTxMediatorID(domainId, x));
                }
                for (ui64 x : domain.GetProxy()) {
                    allocators.push_back(TDomainsInfo::MakeTxAllocatorID(domainId, x));
                }
                domainPtr = TDomainsInfo::TDomain::ConstructDomainWithExplicitTabletIds(domainName, domainId, schemeRoot,
                    planResolution, coordinators, mediators, allocators, poolTypes);
            }

            appData->DomainsInfo->AddDomain(domainPtr.Release());
        }

        for (const NKikimrConfig::TDomainsConfig::THiveConfig &hiveConfig : Config.GetDomainsConfig().GetHiveConfig()) {
            appData->DomainsInfo->AddHive(hiveConfig.GetHive());
        }

        for (const NKikimrConfig::TDomainsConfig::TNamedCompactionPolicy &policy : Config.GetDomainsConfig().GetNamedCompactionPolicy()) {
            appData->DomainsInfo->AddCompactionPolicy(policy.GetName(), new NLocalDb::TCompactionPolicy(policy.GetPolicy()));
        }

        const auto& securityConfig(Config.GetDomainsConfig().GetSecurityConfig());
        appData->EnforceUserTokenRequirement = securityConfig.GetEnforceUserTokenRequirement();
        appData->EnforceUserTokenCheckRequirement = securityConfig.GetEnforceUserTokenCheckRequirement();
        if (securityConfig.AdministrationAllowedSIDsSize() > 0) {
            TVector<TString> administrationAllowedSIDs(securityConfig.GetAdministrationAllowedSIDs().begin(), securityConfig.GetAdministrationAllowedSIDs().end());
            appData->AdministrationAllowedSIDs = std::move(administrationAllowedSIDs);
        }
        if (securityConfig.DefaultUserSIDsSize() > 0) {
            TVector<TString> defaultUserSIDs(securityConfig.GetDefaultUserSIDs().begin(), securityConfig.GetDefaultUserSIDs().end());
            appData->DefaultUserSIDs = std::move(defaultUserSIDs);
        }
        if (securityConfig.HasAllAuthenticatedUsers()) {
            const TString& allUsersGroup = Strip(securityConfig.GetAllAuthenticatedUsers());
            if (allUsersGroup) {
                appData->AllAuthenticatedUsers = allUsersGroup;
            }
        }
        if (securityConfig.RegisterDynamicNodeAllowedSIDsSize() > 0) {
            const auto& allowedSids = securityConfig.GetRegisterDynamicNodeAllowedSIDs();
            TVector<TString> registerDynamicNodeAllowedSIDs(allowedSids.cbegin(), allowedSids.cend());
            appData->RegisterDynamicNodeAllowedSIDs = std::move(registerDynamicNodeAllowedSIDs);
        }

        appData->FeatureFlags = Config.GetFeatureFlags();
        appData->AllowHugeKeyValueDeletes = Config.GetFeatureFlags().GetAllowHugeKeyValueDeletes();
        appData->EnableKqpSpilling = Config.GetTableServiceConfig().GetSpillingServiceConfig().GetLocalFileConfig().GetEnable();

        appData->CompactionConfig = Config.GetCompactionConfig();
        appData->BackgroundCleaningConfig = Config.GetBackgroundCleaningConfig();
    }
};


class TChannelProfilesInitializer : public IAppDataInitializer {
    const NKikimrConfig::TAppConfig& Config;

public:
    TChannelProfilesInitializer(const TKikimrRunConfig& runConfig)
        : Config(runConfig.AppConfig)
    {
    }

    virtual void Initialize(NKikimr::TAppData* appData) override
    {
        if (!Config.HasChannelProfileConfig()) {
            return;
        }
        // setup channel profiles
        appData->ChannelProfiles = new TChannelProfiles();
        ui32 idx = 0;
        Y_ABORT_UNLESS(Config.GetChannelProfileConfig().ProfileSize() > 0);
        for (const NKikimrConfig::TChannelProfileConfig::TProfile &profile : Config.GetChannelProfileConfig().GetProfile()) {
            const ui32 profileId = profile.GetProfileId();
            Y_ABORT_UNLESS(profileId == idx, "Duplicate, missing or out of order profileId %" PRIu32 " (expected %" PRIu32 ")",
                profileId, idx);
            ++idx;
            const ui32 channels = profile.ChannelSize();
            Y_ABORT_UNLESS(channels >= 2);

            appData->ChannelProfiles->Profiles.emplace_back();
            TChannelProfiles::TProfile &outProfile = appData->ChannelProfiles->Profiles.back();
            for (const NKikimrConfig::TChannelProfileConfig::TProfile::TChannel &channel : profile.GetChannel()) {
                Y_ABORT_UNLESS(channel.HasErasureSpecies());
                Y_ABORT_UNLESS(channel.HasPDiskCategory());
                TString name = channel.GetErasureSpecies();
                TBlobStorageGroupType::EErasureSpecies erasure = TBlobStorageGroupType::ErasureSpeciesByName(name);
                if (erasure == TBlobStorageGroupType::ErasureSpeciesCount) {
                    ythrow yexception() << "wrong erasure species \"" << name << "\"";
                }
                const ui64 pDiskCategory = channel.GetPDiskCategory();
                const NKikimrBlobStorage::TVDiskKind::EVDiskKind vDiskCategory = static_cast<NKikimrBlobStorage::TVDiskKind::EVDiskKind>(channel.GetVDiskCategory());

                const TString kind = channel.GetStoragePoolKind();
                outProfile.Channels.push_back(TChannelProfiles::TProfile::TChannel(erasure, pDiskCategory, vDiskCategory, kind));
            }
        }
    }

};


class TProxySchemeCacheInitializer : public IAppDataInitializer {
    const NKikimrConfig::TAppConfig& Config;

public:
    TProxySchemeCacheInitializer(const TKikimrRunConfig& runConfig)
        : Config(runConfig.AppConfig)
    {
    }

    virtual void Initialize(NKikimr::TAppData* appData) override
    {
        if (Config.HasBootstrapConfig()) {
            if (Config.GetBootstrapConfig().HasProxySchemeCacheNodes())
                appData->ProxySchemeCacheNodes = Config.GetBootstrapConfig().GetProxySchemeCacheNodes();
            if (Config.GetBootstrapConfig().HasProxySchemeCacheDistNodes())
                appData->ProxySchemeCacheDistrNodes = Config.GetBootstrapConfig().GetProxySchemeCacheDistNodes();
        }
    }
};


class TDynamicNameserviceInitializer : public IAppDataInitializer {
    const NKikimrConfig::TAppConfig& Config;

public:
    TDynamicNameserviceInitializer(const TKikimrRunConfig& runConfig)
        : Config(runConfig.AppConfig)
    {
    }

    virtual void Initialize(NKikimr::TAppData* appData) override
    {
        auto &dnConfig = Config.GetDynamicNameserviceConfig();
        TIntrusivePtr<TDynamicNameserviceConfig> config = new TDynamicNameserviceConfig;
        config->MaxStaticNodeId = dnConfig.GetMaxStaticNodeId();
        config->MinDynamicNodeId = dnConfig.GetMinDynamicNodeId();
        config->MaxDynamicNodeId = dnConfig.GetMaxDynamicNodeId();
        appData->DynamicNameserviceConfig = config;
    }
};


class TCmsInitializer : public IAppDataInitializer {
    const NKikimrConfig::TAppConfig& Config;

public:
    TCmsInitializer(const TKikimrRunConfig& runConfig)
        : Config(runConfig.AppConfig)
    {
    }

    virtual void Initialize(NKikimr::TAppData* appData) override
    {
        if (Config.HasCmsConfig())
            appData->DefaultCmsConfig = MakeHolder<NKikimrCms::TCmsConfig>(Config.GetCmsConfig());
    }
};

class TLabelsInitializer : public IAppDataInitializer {
    const NKikimrConfig::TAppConfig& Config;

public:
    TLabelsInitializer(const TKikimrRunConfig& runConfig)
        : Config(runConfig.AppConfig)
    {
    }

    virtual void Initialize(NKikimr::TAppData* appData) override
    {
        for (const auto& label : Config.GetLabels()) {
            appData->Labels[label.GetName()] = label.GetValue();
        }
    }
};

class TClusterNameInitializer : public IAppDataInitializer {
    const TKikimrRunConfig& Config;

public:
    TClusterNameInitializer(const TKikimrRunConfig& runConfig)
        : Config(runConfig)
    {
    }

    virtual void Initialize(NKikimr::TAppData* appData) override
    {
        appData->ClusterName = Config.ClusterName;
    }
};

class TYamlConfigInitializer : public IAppDataInitializer {
    const NKikimrConfig::TAppConfig& Config;

public:
    TYamlConfigInitializer(const TKikimrRunConfig& runConfig)
        : Config(runConfig.AppConfig)
    {
    }

    virtual void Initialize(NKikimr::TAppData* appData) override
    {
        appData->YamlConfigEnabled = Config.GetYamlConfigEnabled();
    }
};

TKikimrRunner::TKikimrRunner(std::shared_ptr<TModuleFactories> factories)
    : ModuleFactories(std::move(factories))
    , Counters(MakeIntrusive<::NMonitoring::TDynamicCounters>())
    , PollerThreads(MakeIntrusive<NInterconnect::TPollerThreads>())
    , ProcessMemoryInfoProvider(MakeIntrusive<NMemory::TProcessMemoryInfoProvider>())
{
}

TKikimrRunner::~TKikimrRunner() {
    if (!!ActorSystem) {
        // Stop ActorSystem first, so no one actor can call any grpc stuff.
        ActorSystem->Stop();
        // After that stop sending any requests to actors
        // by destroing grpc subsystem.
        for (auto& serv : GRpcServers) {
            serv.second.Destroy();
        }

        ActorSystem.Destroy();
    }
}

void TKikimrRunner::AddGlobalObject(std::shared_ptr<void> object) {
    GlobalObjects.push_back(std::move(object));
}

void TKikimrRunner::InitializeMonitoring(const TKikimrRunConfig& runConfig, bool includeHostName)
{
    const auto& appConfig = runConfig.AppConfig;
    NActors::TMon::TConfig monConfig;
    monConfig.Port = appConfig.HasMonitoringConfig() ? appConfig.GetMonitoringConfig().GetMonitoringPort() : 0;
    if (monConfig.Port) {
        monConfig.Title = appConfig.GetMonitoringConfig().GetMonitoringCaption();
        monConfig.Threads = appConfig.GetMonitoringConfig().GetMonitoringThreads();
        monConfig.Address = appConfig.GetMonitoringConfig().GetMonitoringAddress();
        monConfig.Certificate = appConfig.GetMonitoringConfig().GetMonitoringCertificate();
        if (appConfig.GetMonitoringConfig().HasMonitoringCertificateFile()) {
            monConfig.Certificate = TUnbufferedFileInput(appConfig.GetMonitoringConfig().GetMonitoringCertificateFile()).ReadAll();
        }
        monConfig.RedirectMainPageTo = appConfig.GetMonitoringConfig().GetRedirectMainPageTo();
        if (includeHostName) {
            if (appConfig.HasNameserviceConfig() && appConfig.GetNameserviceConfig().NodeSize() > 0) {
                for (const auto& it : appConfig.GetNameserviceConfig().GetNode()) {
                    if (it.HasNodeId() && it.GetNodeId() == runConfig.NodeId) {
                        if (it.HasHost()) {
                            monConfig.Host = it.GetHost();
                        }
                        break;
                    }
                }
            }
            if (monConfig.Host) {
                monConfig.Title += " - " + monConfig.Host;
            }
        }

        const auto& securityConfig(runConfig.AppConfig.GetDomainsConfig().GetSecurityConfig());
        if (securityConfig.MonitoringAllowedSIDsSize() > 0) {
            monConfig.AllowedSIDs.assign(securityConfig.GetMonitoringAllowedSIDs().begin(), securityConfig.GetMonitoringAllowedSIDs().end());
        }

        if (ModuleFactories && ModuleFactories->MonitoringFactory) {
            Monitoring = ModuleFactories->MonitoringFactory(std::move(monConfig), appConfig);
        } else {
            if (appConfig.GetFeatureFlags().GetEnableAsyncHttpMon()) {
                Monitoring = new NActors::TAsyncHttpMon(std::move(monConfig));
            } else {
                Monitoring = new NActors::TSyncHttpMon(std::move(monConfig));
            }
        }
        if (Monitoring) {
            Monitoring->RegisterCountersPage("counters", "Counters", Counters);
            Monitoring->Register(NHttp::CreatePing());
            ActorsMonPage = Monitoring->RegisterIndexPage("actors", "Actors");
        }
    }
}

void TKikimrRunner::InitializeMonitoringLogin(const TKikimrRunConfig&)
{
    if (Monitoring) {
        Monitoring->RegisterHandler("/login", MakeWebLoginServiceId());
        Monitoring->RegisterHandler("/logout", MakeWebLoginServiceId());
    }
}

void TKikimrRunner::InitializeControlBoard(const TKikimrRunConfig& runConfig)
{
    if (Monitoring) {
        Monitoring->RegisterActorPage(ActorsMonPage, "icb", "Immediate Control Board", false, ActorSystem.Get(), MakeIcbId(runConfig.NodeId));
    }
}

static TString ReadFile(const TString& fileName) {
    TFileInput f(fileName);
    return f.ReadAll();
}

void TKikimrRunner::InitializeGracefulShutdown(const TKikimrRunConfig& runConfig) {
    Y_UNUSED(runConfig);
    GracefulShutdownSupported = true;
}

void TKikimrRunner::InitializeKqpController(const TKikimrRunConfig& runConfig) {
    if (runConfig.ServicesMask.EnableKqp) {
        auto& tableServiceConfig = runConfig.AppConfig.GetTableServiceConfig();
        auto& featureFlags = runConfig.AppConfig.GetFeatureFlags();
        KqpShutdownController.Reset(new NKqp::TKqpShutdownController(NKqp::MakeKqpProxyID(runConfig.NodeId), tableServiceConfig, featureFlags.GetEnableGracefulShutdown()));
        KqpShutdownController->Initialize(ActorSystem.Get());
    }
}

void TKikimrRunner::InitializeGRpc(const TKikimrRunConfig& runConfig) {
    const auto& appConfig = runConfig.AppConfig;

    auto fillFn = [&](const NKikimrConfig::TGRpcConfig& grpcConfig, NYdbGrpc::TGRpcServer& server, NYdbGrpc::TServerOptions& opts) {
        const auto& services = grpcConfig.GetServices();
        const auto& rlServicesEnabled = grpcConfig.GetRatelimiterServicesEnabled();
        const auto& rlServicesDisabled = grpcConfig.GetRatelimiterServicesDisabled();

        class TServiceCfg {
        public:
            TServiceCfg(bool enabled)
                : ServiceEnabled(enabled)
            { }
            operator bool() const {
                return ServiceEnabled;
            }
            bool IsRlAllowed() const {
                return RlAllowed;
            }
            void SetRlAllowed(bool allowed) {
                RlAllowed = allowed;
            }
        private:
            bool ServiceEnabled = false;
            bool RlAllowed = false;
        };

        std::unordered_map<TString, TServiceCfg*> names;

        TServiceCfg hasLegacy = opts.SslData.Empty() && services.empty();
        names["legacy"] = &hasLegacy;
        TServiceCfg hasScripting = services.empty();
        names["scripting"] = &hasScripting;
        TServiceCfg hasCms = services.empty();
        names["cms"] = &hasCms;
        TServiceCfg hasMaintenance = services.empty();
        names["maintenance"] = &hasMaintenance;
        TServiceCfg hasKesus = services.empty();
        names["locking"] = names["kesus"] = &hasKesus;
        TServiceCfg hasMonitoring = services.empty();
        names["monitoring"] = &hasMonitoring;
        TServiceCfg hasDiscovery = services.empty();
        names["discovery"] = &hasDiscovery;
        TServiceCfg hasLocalDiscovery = false;
        names["local_discovery"] = &hasLocalDiscovery;
        TServiceCfg hasTableService = services.empty();
        names["table_service"] = &hasTableService;
        TServiceCfg hasSchemeService = false;
        TServiceCfg hasOperationService = false;
        TServiceCfg hasYql = false;
        names["yql"] = &hasYql;
        TServiceCfg hasYqlInternal = services.empty();
        names["yql_internal"] = &hasYqlInternal;
        TServiceCfg hasPQ = services.empty();
        names["pq"] = &hasPQ;
        TServiceCfg hasPQv1 = services.empty();
        names["pqv1"] = &hasPQv1;
        TServiceCfg hasTopic = services.empty();
        names["topic"] = &hasTopic;
        TServiceCfg hasPQCD = services.empty();
        names["pqcd"] = &hasPQCD;
        TServiceCfg hasObjectStorage = services.empty();
        names["object_storage"] = &hasObjectStorage;
        TServiceCfg hasClickhouseInternal = services.empty();
        names["clickhouse_internal"] = &hasClickhouseInternal;
        TServiceCfg hasRateLimiter = false;
        names["rate_limiter"] = &hasRateLimiter;
        TServiceCfg hasExport = services.empty();
        names["export"] = &hasExport;
        TServiceCfg hasImport = services.empty();
        names["import"] = &hasImport;
        TServiceCfg hasAnalytics = false;
        names["analytics"] = &hasAnalytics;
        TServiceCfg hasDataStreams = false;
        names["datastreams"] = &hasDataStreams;
        TServiceCfg hasYandexQuery = false;
        names["yq"] = &hasYandexQuery;
        TServiceCfg hasYandexQueryPrivate = false;
        names["yq_private"] = &hasYandexQueryPrivate;
        TServiceCfg hasLogStore = false;
        names["logstore"] = &hasLogStore;
        TServiceCfg hasAuth = services.empty();
        names["auth"] = &hasAuth;
        TServiceCfg hasQueryService = services.empty();
        names["query_service"] = &hasQueryService;
        TServiceCfg hasKeyValue = services.empty();
        names["keyvalue"] = &hasKeyValue;
        TServiceCfg hasReplication = services.empty();
        names["replication"] = &hasReplication;
        TServiceCfg hasTabletService = services.empty();
        names["tablet_service"] = &hasTabletService;

        std::unordered_set<TString> enabled;
        for (const auto& name : services) {
            enabled.insert(name);
        }
        for (const auto& name : grpcConfig.GetServicesEnabled()) {
            enabled.insert(name);
        }

        std::unordered_set<TString> disabled;
        for (const auto& name : grpcConfig.GetServicesDisabled()) {
            disabled.insert(name);
        }

        for (const auto& name : enabled) {
            auto itName = names.find(name);
            if (itName != names.end()) {
                *(itName->second) = true;
            } else if (!ModuleFactories || !ModuleFactories->GrpcServiceFactory.Has(name)) {
                Cerr << "Unknown grpc service \"" << name << "\" was not enabled!" << Endl;
            }
        }

        for (const auto& name : disabled) {
            auto itName = names.find(name);
            if (itName != names.end()) {
                *(itName->second) = false;
            } else if (!ModuleFactories || !ModuleFactories->GrpcServiceFactory.Has(name)) {
                Cerr << "Unknown grpc service \"" << name << "\" was not disabled!" << Endl;
            }
        }

        // dependencies
        if (hasRateLimiter) {
            hasKesus = true;
        }

        if (hasYql) {
            hasTableService = true;
            hasYqlInternal = true;
        }

        if (hasTableService || hasYqlInternal || hasPQ || hasKesus || hasPQv1 || hasExport || hasImport || hasTopic) {
            hasSchemeService = true;
            hasOperationService = true;
            // backward compatability
            hasExport = true;
        }

        if (hasExport || hasImport) {
            hasExport = true;
            hasImport = true;
        }

        if (hasTableService || hasYql) {
            hasQueryService = true;
        }

        // Enable RL for all services if enabled list is empty
        if (rlServicesEnabled.empty()) {
            for (auto& [name, cfg] : names) {
                Y_ABORT_UNLESS(cfg);
                cfg->SetRlAllowed(true);
            }
        } else {
            for (const auto& name : rlServicesEnabled) {
                auto itName = names.find(name);
                if (itName != names.end()) {
                    Y_ABORT_UNLESS(itName->second);
                    itName->second->SetRlAllowed(true);
                } else if (!ModuleFactories || !ModuleFactories->GrpcServiceFactory.Has(name)) {
                    Cerr << "Unknown grpc service \"" << name << "\" rl was not enabled" << Endl;
                }
            }
        }

        for (const auto& name : rlServicesDisabled) {
            auto itName = names.find(name);
            if (itName != names.end()) {
                Y_ABORT_UNLESS(itName->second);
                itName->second->SetRlAllowed(false);
            } else if (!ModuleFactories || !ModuleFactories->GrpcServiceFactory.Has(name)) {
                Cerr << "Unknown grpc service \"" << name << "\" rl was not disabled" << Endl;
            }
        }

        for (const auto& [name, isEnabled] : names) {
            if (*isEnabled) {
                enabled.insert(name);
            } else {
                disabled.insert(name);
            }
        }


        // TODO: for now we pass multiple proxies only to the table service
        const size_t proxyCount = Max(ui32{1}, grpcConfig.GetGRpcProxyCount());
        TVector<TActorId> grpcRequestProxies;
        grpcRequestProxies.reserve(proxyCount);
        for (size_t i = 0; i < proxyCount; ++i) {
            grpcRequestProxies.push_back(NGRpcService::CreateGRpcRequestProxyId(i));
        }

        if (hasLegacy) {
            // start legacy service
            auto grpcService = new NGRpcProxy::TGRpcService();
            auto future = grpcService->Prepare(ActorSystem.Get(), NMsgBusProxy::CreatePersQueueMetaCacheV2Id(), NMsgBusProxy::CreateMsgBusProxyId(), Counters);
            auto startCb = [grpcService](NThreading::TFuture<void> result) {
                if (result.HasException()) {
                    try {
                        result.GetValue();
                    }
                    catch (const std::exception& ex) {
                        Y_ABORT("Unable to prepare GRpc service: %s", ex.what());
                    }
                }
                else {
                    grpcService->Start();
                }
            };

            future.Subscribe(startCb);
            server.AddService(grpcService);
        }

        if (hasTableService) {
            server.AddService(new NGRpcService::TGRpcYdbTableService(ActorSystem.Get(), Counters, grpcRequestProxies,
                hasTableService.IsRlAllowed(), grpcConfig.GetHandlersPerCompletionQueue()));
        }

        if (hasClickhouseInternal) {
            server.AddService(new NGRpcService::TGRpcYdbClickhouseInternalService(ActorSystem.Get(), Counters,
                AppData->InFlightLimiterRegistry, grpcRequestProxies[0], hasClickhouseInternal.IsRlAllowed()));
        }

        if (hasObjectStorage) {
            server.AddService(new NGRpcService::TGRpcYdbObjectStorageService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasObjectStorage.IsRlAllowed()));
        }

        if (hasScripting) {
            server.AddService(new NGRpcService::TGRpcYdbScriptingService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasScripting.IsRlAllowed()));
        }

        if (hasSchemeService) {
            // RPC RL enabled
            // We have no way to disable or enable this service explicitly
            server.AddService(new NGRpcService::TGRpcYdbSchemeService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], true /*hasSchemeService.IsRlAllowed()*/));
        }

        if (hasOperationService) {
            server.AddService(new NGRpcService::TGRpcOperationService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasOperationService.IsRlAllowed()));
        }

        if (hasExport) {
            server.AddService(new NGRpcService::TGRpcYdbExportService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasExport.IsRlAllowed()));
        }

        if (hasImport) {
            server.AddService(new NGRpcService::TGRpcYdbImportService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasImport.IsRlAllowed()));
        }

        if (hasImport && hasExport && appConfig.GetFeatureFlags().GetEnableBackupService()) {
            server.AddService(new NGRpcService::TGRpcBackupService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasExport.IsRlAllowed()));
        }

        if (hasKesus) {
            server.AddService(new NKesus::TKesusGRpcService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasKesus.IsRlAllowed()));
        }

        if (hasPQ) {
            server.AddService(new NKikimr::NGRpcService::TGRpcPersQueueService(ActorSystem.Get(), Counters, NMsgBusProxy::CreatePersQueueMetaCacheV2Id()));
        }

        if (hasPQv1) {
            server.AddService(new NGRpcService::V1::TGRpcPersQueueService(ActorSystem.Get(), Counters, NMsgBusProxy::CreatePersQueueMetaCacheV2Id(),
                grpcRequestProxies[0], hasPQv1.IsRlAllowed()));
        }

        if (hasPQv1 || hasTopic) {
            server.AddService(new NGRpcService::V1::TGRpcTopicService(ActorSystem.Get(), Counters, NMsgBusProxy::CreatePersQueueMetaCacheV2Id(),
                grpcRequestProxies[0], hasTopic.IsRlAllowed() || hasPQv1.IsRlAllowed()));
        }

        if (hasPQCD) {
            // the service has its own flag since it should be capable of using custom grpc port
            const auto& pqcdConfig = AppData->PQClusterDiscoveryConfig;
            TMaybe<ui64> inflightLimit = Nothing();
            if (pqcdConfig.HasRequestInflightLimit() && pqcdConfig.GetRequestInflightLimit() > 0) {
                inflightLimit = pqcdConfig.GetRequestInflightLimit();
            }
            server.AddService(new NGRpcService::TGRpcPQClusterDiscoveryService(
                    ActorSystem.Get(), Counters, grpcRequestProxies[0], inflightLimit
            ));
        }

        if (hasCms) {
            server.AddService(new NGRpcService::TGRpcCmsService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasCms.IsRlAllowed()));

            server.AddService(new NGRpcService::TGRpcDynamicConfigService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasCms.IsRlAllowed()));
        }

        if (hasMaintenance) {
            server.AddService(new NGRpcService::TGRpcMaintenanceService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasMaintenance.IsRlAllowed()));
        }

        if (hasDiscovery) {
            server.AddService(new NGRpcService::TGRpcDiscoveryService(ActorSystem.Get(), Counters,grpcRequestProxies[0], hasDiscovery.IsRlAllowed()));
        }

        if (hasLocalDiscovery) {
            server.AddService(new NGRpcService::TGRpcLocalDiscoveryService(grpcConfig, ActorSystem.Get(), Counters, grpcRequestProxies[0]));
        }

        if (hasRateLimiter) {
            server.AddService(new NQuoter::TRateLimiterGRpcService(ActorSystem.Get(), Counters, grpcRequestProxies[0]));
        }

        if (hasMonitoring) {
            server.AddService(new NGRpcService::TGRpcMonitoringService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasMonitoring.IsRlAllowed()));
        }

        if (hasAuth) {
            server.AddService(new NGRpcService::TGRpcAuthService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasAuth.IsRlAllowed()));
        }

        if (hasDataStreams) {
            server.AddService(new NGRpcService::TGRpcDataStreamsService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasDataStreams.IsRlAllowed()));
        }

        if (hasYandexQuery) {
            server.AddService(new NGRpcService::TGRpcFederatedQueryService(ActorSystem.Get(), Counters, grpcRequestProxies[0]));
            server.AddService(new NGRpcService::TGRpcFqPrivateTaskService(ActorSystem.Get(), Counters, grpcRequestProxies[0]));

            if (!hasTableService && !hasSchemeService) {
                server.AddService(new NGRpcService::TGrpcTableOverFqService(ActorSystem.Get(), Counters, grpcRequestProxies[0]));
                server.AddService(new NGRpcService::TGrpcSchemeOverFqService(ActorSystem.Get(), Counters, grpcRequestProxies[0]));
            }
        }   /* REMOVE */ else /* THIS else as well and separate ifs */ if (hasYandexQueryPrivate) {
            server.AddService(new NGRpcService::TGRpcFqPrivateTaskService(ActorSystem.Get(), Counters, grpcRequestProxies[0]));
        }

        if (hasQueryService) {
            server.AddService(new NGRpcService::TGRpcYdbQueryService(ActorSystem.Get(), Counters,
                grpcRequestProxies, hasDataStreams.IsRlAllowed(), grpcConfig.GetHandlersPerCompletionQueue()));
        }

        if (hasLogStore) {
            server.AddService(new NGRpcService::TGRpcYdbLogStoreService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasLogStore.IsRlAllowed()));
        }

        if (hasKeyValue) {
            server.AddService(new NGRpcService::TKeyValueGRpcService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0]));
        }

        if (hasReplication) {
            server.AddService(new NGRpcService::TGRpcReplicationService(ActorSystem.Get(), Counters,
                grpcRequestProxies[0], hasReplication.IsRlAllowed()));
        }

        if (hasTabletService) {
            server.AddService(new NGRpcService::TGRpcYdbTabletService(ActorSystem.Get(), Counters, grpcRequestProxies,
                hasTabletService.IsRlAllowed(), grpcConfig.GetHandlersPerCompletionQueue()));
        }

        if (ModuleFactories) {
            for (const auto& service : ModuleFactories->GrpcServiceFactory.Create(enabled, disabled, ActorSystem.Get(), Counters, grpcRequestProxies[0])) {
                server.AddService(service);
            }
        }
    };

    if (appConfig.HasGRpcConfig() && appConfig.GetGRpcConfig().GetStartGRpcProxy()) {
        const auto& grpcConfig = appConfig.GetGRpcConfig();

        EnabledGrpcService = true;
        NYdbGrpc::TServerOptions opts;
        opts.SetHost(grpcConfig.GetHost());
        opts.SetPort(grpcConfig.GetPort());
        opts.SetWorkerThreads(grpcConfig.GetWorkerThreads());
        opts.SetWorkersPerCompletionQueue(grpcConfig.GetWorkersPerCompletionQueue());
        opts.SetGRpcMemoryQuotaBytes(grpcConfig.GetGRpcMemoryQuotaBytes());
        opts.SetEnableGRpcMemoryQuota(grpcConfig.GetEnableGRpcMemoryQuota());
        opts.SetMaxMessageSize(grpcConfig.HasMaxMessageSize() ? grpcConfig.GetMaxMessageSize() : NYdbGrpc::DEFAULT_GRPC_MESSAGE_SIZE_LIMIT);
        opts.SetMaxGlobalRequestInFlight(grpcConfig.GetMaxInFlight());
        opts.SetLogger(NYdbGrpc::CreateActorSystemLogger(*ActorSystem.Get(), NKikimrServices::GRPC_SERVER));

        if (appConfig.HasDomainsConfig() &&
            appConfig.GetDomainsConfig().HasSecurityConfig() &&
            appConfig.GetDomainsConfig().GetSecurityConfig().HasEnforceUserTokenRequirement()) {
            opts.SetUseAuth(appConfig.GetDomainsConfig().GetSecurityConfig().GetEnforceUserTokenRequirement());
        }

        if (grpcConfig.GetKeepAliveEnable()) {
            opts.SetKeepAliveEnable(true);
            opts.SetKeepAliveIdleTimeoutTriggerSec(grpcConfig.GetKeepAliveIdleTimeoutTriggerSec());
            opts.SetKeepAliveMaxProbeCount(grpcConfig.GetKeepAliveMaxProbeCount());
            opts.SetKeepAliveProbeIntervalSec(grpcConfig.GetKeepAliveProbeIntervalSec());
        } else {
            opts.SetKeepAliveEnable(false);
        }

        NConsole::SetGRpcLibraryFunction();

#define GET_PATH_TO_FILE(GRPC_CONFIG, PRIMARY_FIELD, SECONDARY_FIELD) \
        (GRPC_CONFIG.Has##PRIMARY_FIELD() ? GRPC_CONFIG.Get##PRIMARY_FIELD() : GRPC_CONFIG.Get##SECONDARY_FIELD())

        NYdbGrpc::TServerOptions sslOpts = opts;
        if (grpcConfig.HasSslPort() && grpcConfig.GetSslPort()) {
            const auto& pathToCaFile = GET_PATH_TO_FILE(grpcConfig, PathToCaFile, CA);
            const auto& pathToCertificateFile = GET_PATH_TO_FILE(grpcConfig, PathToCertificateFile, Cert);
            const auto& pathToPrivateKeyFile = GET_PATH_TO_FILE(grpcConfig, PathToPrivateKeyFile, Key);

            Y_ABORT_UNLESS(!pathToCaFile.Empty(), "CA not set");
            Y_ABORT_UNLESS(!pathToCertificateFile.Empty(), "Cert not set");
            Y_ABORT_UNLESS(!pathToPrivateKeyFile.Empty(), "Key not set");
            sslOpts.SetPort(grpcConfig.GetSslPort());
            NYdbGrpc::TSslData sslData;
            sslData.Root = ReadFile(pathToCaFile);
            sslData.Cert = ReadFile(pathToCertificateFile);
            sslData.Key = ReadFile(pathToPrivateKeyFile);
            sslData.DoRequestClientCertificate = appConfig.GetClientCertificateAuthorization().GetRequestClientCertificate();
            sslOpts.SetSslData(sslData);

            GRpcServers.push_back({ "grpcs", new NYdbGrpc::TGRpcServer(sslOpts) });

            fillFn(grpcConfig, *GRpcServers.back().second, sslOpts);
        }

        if (grpcConfig.GetPort()) {
            GRpcServers.push_back({ "grpc", new NYdbGrpc::TGRpcServer(opts) });

            fillFn(grpcConfig, *GRpcServers.back().second, opts);
        }

        for (auto &ex : grpcConfig.GetExtEndpoints()) {
            // todo: check uniq
            if (ex.GetPort()) {
                NYdbGrpc::TServerOptions xopts = opts;
                xopts.SetPort(ex.GetPort());
                if (ex.GetHost())
                    xopts.SetHost(ex.GetHost());

                GRpcServers.push_back({ "grpc", new NYdbGrpc::TGRpcServer(xopts) });
                fillFn(ex, *GRpcServers.back().second, xopts);
            }

            if (ex.HasSslPort() && ex.GetSslPort()) {
                NYdbGrpc::TServerOptions xopts = opts;
                xopts.SetPort(ex.GetSslPort());


                NYdbGrpc::TSslData sslData;

                auto pathToCaFile = GET_PATH_TO_FILE(ex, PathToCaFile, CA);
                if (pathToCaFile.Empty()) {
                    pathToCaFile = GET_PATH_TO_FILE(grpcConfig, PathToCaFile, CA);
                }
                sslData.Root = ReadFile(pathToCaFile);

                auto pathToCertificateFile = GET_PATH_TO_FILE(ex, PathToCertificateFile, Cert);
                if (pathToCertificateFile.Empty()) {
                    pathToCertificateFile = GET_PATH_TO_FILE(grpcConfig, PathToCertificateFile, Cert);
                }
                sslData.Cert = ReadFile(pathToCertificateFile);

                auto pathToPrivateKeyFile = GET_PATH_TO_FILE(ex, PathToPrivateKeyFile, Key);
                if (pathToPrivateKeyFile.Empty()) {
                    pathToPrivateKeyFile = GET_PATH_TO_FILE(grpcConfig, PathToPrivateKeyFile, Key);
                }
                sslData.Key = ReadFile(pathToPrivateKeyFile);
#undef GET_PATH_TO_FILE

                xopts.SetSslData(sslData);

                Y_ABORT_UNLESS(xopts.SslData->Root, "CA not set");
                Y_ABORT_UNLESS(xopts.SslData->Cert, "Cert not set");
                Y_ABORT_UNLESS(xopts.SslData->Key, "Key not set");

                GRpcServers.push_back({ "grpcs", new NYdbGrpc::TGRpcServer(xopts) });
                fillFn(ex, *GRpcServers.back().second, xopts);
            }
        }
    }
}

void TKikimrRunner::InitializeAllocator(const TKikimrRunConfig& runConfig) {
    const auto& cfg = runConfig.AppConfig;
    const auto& allocConfig = cfg.GetAllocatorConfig();
    for (const auto& a : allocConfig.GetParam()) {
        NMalloc::MallocInfo().SetParam(a.first.c_str(), a.second.c_str());
    }
}

void TKikimrRunner::InitializeAppData(const TKikimrRunConfig& runConfig)
{
    const auto& cfg = runConfig.AppConfig;

    bool useAutoConfig = !cfg.HasActorSystemConfig() || (cfg.GetActorSystemConfig().HasUseAutoConfig() && cfg.GetActorSystemConfig().GetUseAutoConfig());
    NAutoConfigInitializer::TASPools pools = NAutoConfigInitializer::GetASPools(cfg.GetActorSystemConfig(), useAutoConfig);
    TMap<TString, ui32> servicePools = NAutoConfigInitializer::GetServicePools(cfg.GetActorSystemConfig(), useAutoConfig);

    AppData.Reset(new TAppData(pools.SystemPoolId, pools.UserPoolId, pools.IOPoolId, pools.BatchPoolId,
                               servicePools,
                               TypeRegistry.Get(),
                               FunctionRegistry.Get(),
                               FormatFactory.Get(),
                               &KikimrShouldContinue));

    AppData->DataShardExportFactory = ModuleFactories ? ModuleFactories->DataShardExportFactory.get() : nullptr;
    AppData->SqsEventsWriterFactory = ModuleFactories ? ModuleFactories->SqsEventsWriterFactory.get() : nullptr;
    AppData->PersQueueMirrorReaderFactory = ModuleFactories ? ModuleFactories->PersQueueMirrorReaderFactory.get() : nullptr;
    AppData->PersQueueGetReadSessionsInfoWorkerFactory = ModuleFactories ? ModuleFactories->PQReadSessionsInfoWorkerFactory.get() : nullptr;
    AppData->IoContextFactory = ModuleFactories ? ModuleFactories->IoContextFactory.get() : nullptr;

    AppData->SqsAuthFactory = ModuleFactories
        ? ModuleFactories->SqsAuthFactory.get()
        : nullptr;

    AppData->DataStreamsAuthFactory = ModuleFactories
        ? ModuleFactories->DataStreamsAuthFactory.get()
        : nullptr;

    AppData->FolderServiceFactory = ModuleFactories
        ? ModuleFactories->FolderServiceFactory
        : nullptr;

    AppData->Counters = Counters;
    AppData->Mon = Monitoring.Get();
    AppData->PollerThreads = PollerThreads;
    AppData->LocalScopeId = runConfig.ScopeId;

    // setup streaming config
    if (runConfig.AppConfig.GetGRpcConfig().HasStreamingConfig()) {
        AppData->StreamingConfig.CopyFrom(runConfig.AppConfig.GetGRpcConfig().GetStreamingConfig());
    }

    if (runConfig.AppConfig.HasPQConfig()) {
        AppData->PQConfig.CopyFrom(runConfig.AppConfig.GetPQConfig());
    }

    if (runConfig.AppConfig.HasPQClusterDiscoveryConfig()) {
        AppData->PQClusterDiscoveryConfig.CopyFrom(runConfig.AppConfig.GetPQClusterDiscoveryConfig());
    }

    if (runConfig.AppConfig.HasNetClassifierConfig()) {
        AppData->NetClassifierConfig.CopyFrom(runConfig.AppConfig.GetNetClassifierConfig());
    }

    if (runConfig.AppConfig.HasSqsConfig()) {
        AppData->SqsConfig.CopyFrom(runConfig.AppConfig.GetSqsConfig());
    }

    if (runConfig.AppConfig.HasAuthConfig()) {
        AppData->AuthConfig.CopyFrom(runConfig.AppConfig.GetAuthConfig());
    }

    if (runConfig.AppConfig.HasKeyConfig()) {
        AppData->KeyConfig.CopyFrom(runConfig.AppConfig.GetKeyConfig());
    }

    if (runConfig.AppConfig.HasPDiskKeyConfig()) {
        AppData->PDiskKeyConfig.CopyFrom(runConfig.AppConfig.GetPDiskKeyConfig());
    }

    if (runConfig.AppConfig.HasHiveConfig()) {
        AppData->HiveConfig.CopyFrom(runConfig.AppConfig.GetHiveConfig());
    }

    if (runConfig.AppConfig.HasDataShardConfig()) {
        AppData->DataShardConfig = runConfig.AppConfig.GetDataShardConfig();
    }

    if (runConfig.AppConfig.HasColumnShardConfig()) {
        AppData->ColumnShardConfig = runConfig.AppConfig.GetColumnShardConfig();
    }

    if (runConfig.AppConfig.HasSchemeShardConfig()) {
        AppData->SchemeShardConfig = runConfig.AppConfig.GetSchemeShardConfig();
    }

    if (runConfig.AppConfig.HasMeteringConfig()) {
        AppData->MeteringConfig = runConfig.AppConfig.GetMeteringConfig();
    }

    if (runConfig.AppConfig.HasAuditConfig()) {
        AppData->AuditConfig = runConfig.AppConfig.GetAuditConfig();
    }

    AppData->TenantName = runConfig.TenantName;

    if (runConfig.AppConfig.GetDynamicNodeConfig().GetNodeInfo().HasName()) {
        AppData->NodeName = runConfig.AppConfig.GetDynamicNodeConfig().GetNodeInfo().GetName();
    }

    if (runConfig.AppConfig.HasBootstrapConfig()) {
        AppData->BootstrapConfig = runConfig.AppConfig.GetBootstrapConfig();
    }

    if (runConfig.AppConfig.HasSharedCacheConfig()) {
        AppData->SharedCacheConfig = runConfig.AppConfig.GetSharedCacheConfig();
    }

    if (runConfig.AppConfig.HasAwsCompatibilityConfig()) {
        AppData->AwsCompatibilityConfig = runConfig.AppConfig.GetAwsCompatibilityConfig();
    }

    if (runConfig.AppConfig.HasS3ProxyResolverConfig()) {
        AppData->S3ProxyResolverConfig = runConfig.AppConfig.GetS3ProxyResolverConfig();
    }

    if (runConfig.AppConfig.HasGraphConfig()) {
        AppData->GraphConfig.CopyFrom(runConfig.AppConfig.GetGraphConfig());
    }

    if (runConfig.AppConfig.HasMetadataCacheConfig()) {
        AppData->MetadataCacheConfig.CopyFrom(runConfig.AppConfig.GetMetadataCacheConfig());
    }

    if (runConfig.AppConfig.HasMemoryControllerConfig()) {
        AppData->MemoryControllerConfig.CopyFrom(runConfig.AppConfig.GetMemoryControllerConfig());
    }

    if (runConfig.AppConfig.HasReplicationConfig()) {
        AppData->ReplicationConfig = runConfig.AppConfig.GetReplicationConfig();
    }

    // setup resource profiles
    AppData->ResourceProfiles = new TResourceProfiles;
    if (runConfig.AppConfig.GetBootstrapConfig().ResourceProfilesSize())
        AppData->ResourceProfiles->LoadProfiles(runConfig.AppConfig.GetBootstrapConfig().GetResourceProfiles());

    if (runConfig.AppConfig.GetBootstrapConfig().HasEnableIntrospection())
        AppData->EnableIntrospection = runConfig.AppConfig.GetBootstrapConfig().GetEnableIntrospection();

    TAppDataInitializersList appDataInitializers;
    // setup domain info
    appDataInitializers.AddAppDataInitializer(new TDomainsInitializer(runConfig));
    // setup channel profiles
    appDataInitializers.AddAppDataInitializer(new TChannelProfilesInitializer(runConfig));
    // setup proxy scheme cache
    appDataInitializers.AddAppDataInitializer(new TProxySchemeCacheInitializer(runConfig));
    // setup dynamic nameservice
    appDataInitializers.AddAppDataInitializer(new TDynamicNameserviceInitializer(runConfig));
    // setup cms
    appDataInitializers.AddAppDataInitializer(new TCmsInitializer(runConfig));
    // setup labels
    appDataInitializers.AddAppDataInitializer(new TLabelsInitializer(runConfig));
    // setup cluster name
    appDataInitializers.AddAppDataInitializer(new TClusterNameInitializer(runConfig));
    // setup yaml config info
    appDataInitializers.AddAppDataInitializer(new TYamlConfigInitializer(runConfig));

    appDataInitializers.Initialize(AppData.Get());
}

void TKikimrRunner::InitializeLogSettings(const TKikimrRunConfig& runConfig)
{
    auto logBackend = CreateLogBackendWithUnifiedAgent(runConfig, Counters);
    LogBackend.reset(logBackend.Release());

    if (!runConfig.AppConfig.HasLogConfig())
        return;

    auto logConfig = runConfig.AppConfig.GetLogConfig();
    LogSettings.Reset(new NActors::NLog::TSettings(NActors::TActorId(runConfig.NodeId, "logger"),
        NActorsServices::LOGGER,
        (NActors::NLog::EPriority)logConfig.GetDefaultLevel(),
        (NActors::NLog::EPriority)logConfig.GetDefaultSamplingLevel(),
        logConfig.GetDefaultSamplingRate(),
        logConfig.GetTimeThresholdMs()));

    LogSettings->Append(
        NActorsServices::EServiceCommon_MIN,
        NActorsServices::EServiceCommon_MAX,
        NActorsServices::EServiceCommon_Name
    );
    LogSettings->Append(
        NKikimrServices::EServiceKikimr_MIN,
        NKikimrServices::EServiceKikimr_MAX,
        NKikimrServices::EServiceKikimr_Name
    );

    LogSettings->ClusterName = logConfig.HasClusterName() ? logConfig.GetClusterName() : "";

    if (logConfig.GetFormat() == "full") {
        LogSettings->Format = NLog::TSettings::PLAIN_FULL_FORMAT;
    } else if (logConfig.GetFormat() == "short") {
        LogSettings->Format = NLog::TSettings::PLAIN_SHORT_FORMAT;
    } else if (logConfig.GetFormat() == "json") {
        LogSettings->Format = NLog::TSettings::JSON_FORMAT;
        NKikimr::NStLog::OutputLogJson = true;
    } else {
        Y_ABORT("Unknown log format: \"%s\"", logConfig.GetFormat().data());
    }

    if (logConfig.HasAllowDropEntries()) {
        LogSettings->SetAllowDrop(logConfig.GetAllowDropEntries());
    }

    if (logConfig.HasUseLocalTimestamps()) {
        LogSettings->SetUseLocalTimestamps(logConfig.GetUseLocalTimestamps());
    }

    if (LogSettings->Format == NLog::TSettings::JSON_FORMAT) {
        TString fullHostName = HostName();
        size_t firstDot = fullHostName.find_first_of('.');
        LogSettings->ShortHostName = fullHostName.substr(0, firstDot);
    }
}

void TKikimrRunner::ApplyLogSettings(const TKikimrRunConfig& runConfig)
{
    if (!runConfig.AppConfig.HasLogConfig())
        return;

    auto logConfig = runConfig.AppConfig.GetLogConfig();
    for (const auto& entry : logConfig.GetEntry()) {
        const TString& componentName = entry.GetComponent();

        NLog::EComponent component;
        if (componentName.empty()) {
            component = NLog::InvalidComponent;
        } else {
            component = LogSettings->FindComponent(componentName);

            if (logConfig.GetIgnoreUnknownComponents()
                    && component == NLog::InvalidComponent)
            {
                continue;
            }

            Y_ABORT_UNLESS(component != NLog::InvalidComponent, "Invalid component name in log configuration file: \"%s\"",
                componentName.data());
        }

        TString explanation;
        if (entry.HasLevel()) {
            auto prio = (NLog::EPriority)entry.GetLevel();
            Y_ABORT_UNLESS(LogSettings->SetLevel(prio, component, explanation) == 0);

            if (component == NKikimrServices::GRPC_LIBRARY) {
                NConsole::SetGRpcLibraryLogVerbosity(prio);
            }
        }
        if (entry.HasSamplingLevel()) {
            Y_ABORT_UNLESS(LogSettings->SetSamplingLevel((NLog::EPriority)entry.GetSamplingLevel(), component, explanation) == 0);
        }
        if (entry.HasSamplingRate()) {
            Y_ABORT_UNLESS(LogSettings->SetSamplingRate(entry.GetSamplingRate(), component, explanation) == 0);
        }
    }
}

void TKikimrRunner::InitializeActorSystem(
    const TKikimrRunConfig& runConfig,
    TIntrusivePtr<TServiceInitializersList> serviceInitializers,
    const TBasicKikimrServicesMask& servicesMask)
{
    THolder<TActorSystemSetup> setup(new TActorSystemSetup());

    serviceInitializers->InitializeServices(setup.Get(), AppData.Get());

    if (Monitoring) {
        setup->LocalServices.emplace_back(MakeWebLoginServiceId(), TActorSetupCmd(CreateWebLoginService(),
            TMailboxType::HTSwap, AppData->UserPoolId));
        setup->LocalServices.emplace_back(NCrossRef::MakeCrossRefActorId(), TActorSetupCmd(NCrossRef::CreateCrossRefActor(),
            TMailboxType::HTSwap, AppData->SystemPoolId));
        setup->LocalServices.emplace_back(MakeMonVDiskStreamId(), TActorSetupCmd(CreateMonVDiskStreamActor(),
            TMailboxType::HTSwap, AppData->SystemPoolId));
        setup->LocalServices.emplace_back(MakeMonGetBlobId(), TActorSetupCmd(CreateMonGetBlobActor(),
            TMailboxType::HTSwap, AppData->SystemPoolId));
        setup->LocalServices.emplace_back(MakeMonBlobRangeId(), TActorSetupCmd(CreateMonBlobRangeActor(),
            TMailboxType::HTSwap, AppData->SystemPoolId));
    }

    ApplyLogSettings(runConfig);

    ActorSystem.Reset(new TActorSystem(setup, AppData.Get(), LogSettings));

    if (Monitoring) {
        if (servicesMask.EnableLogger) {
            Monitoring->RegisterActorPage(
                ActorsMonPage,
                "logger",
                "Logger",
                false,
                ActorSystem.Get(),
                LogSettings->LoggerActorId);
        }

        if (servicesMask.EnableProfiler) {
            Monitoring->RegisterActorPage(
                ActorsMonPage,
                "profiler",
                "Profiler",
                false,
                ActorSystem.Get(),
                MakeProfilerID(runConfig.NodeId));
        }

        if (servicesMask.EnableLoadService) {
            Monitoring->RegisterActorPage(
                ActorsMonPage,
                "load",
                "Load",
                false,
                ActorSystem.Get(),
                MakeLoadServiceID(runConfig.NodeId));
        }

        if (servicesMask.EnableFailureInjectionService) {
            Monitoring->RegisterActorPage(
                ActorsMonPage,
                "failure_injection",
                "Failure Injection",
                false,
                ActorSystem.Get(),
                MakeBlobStorageFailureInjectionID(runConfig.NodeId));
        }

        Monitoring->RegisterActorPage(
                nullptr,
                "get_blob",
                TString(),
                false,
                ActorSystem.Get(),
                MakeMonGetBlobId());

        Monitoring->RegisterActorPage(
                nullptr,
                "blob_range",
                TString(),
                false,
                ActorSystem.Get(),
                MakeMonBlobRangeId());

        Monitoring->RegisterActorPage(
                nullptr,
                "vdisk_stream",
                TString(),
                false,
                ActorSystem.Get(),
                MakeMonVDiskStreamId());

        Monitoring->RegisterActorPage(
                ActorsMonPage->RegisterIndexPage("interconnect", "Interconnect"),
                "overview",
                "Interconnect overview",
                false,
                ActorSystem.Get(),
                NInterconnect::MakeInterconnectMonActorId(runConfig.NodeId));

        if (servicesMask.EnableGRpcService) {
            Monitoring->RegisterActorPage(nullptr, "grpc", "GRPC", false, ActorSystem.Get(), NGRpcService::GrpcMonServiceId());
        }
    }

    if (servicesMask.EnableSqs && AppData->SqsConfig.GetEnableSqs()) {
        if (AppData->SqsConfig.GetHttpServerConfig().GetPort()) {

            SqsHttp.Reset(new NSQS::TAsyncHttpServer(AppData->SqsConfig));
            SqsHttp->Initialize(ActorSystem.Get(),
                                GetServiceCounters(AppData->Counters, "sqs"),
                                GetServiceCounters(AppData->Counters, "ymq_public"),
                                AppData->UserPoolId);
        }
    }

    if (runConfig.AppConfig.HasGRpcConfig()) {
        if (const ui32 grpcPort = runConfig.AppConfig.GetGRpcConfig().GetPort()) {
            auto driverConfig = NYdb::TDriverConfig().SetEndpoint(TStringBuilder() << "localhost:" << grpcPort);
            YdbDriver.Reset(new NYdb::TDriver(driverConfig));
            AppData->YdbDriver = YdbDriver.Get();
        }
    }

    if (YqSharedResources) {
        YqSharedResources->Init(ActorSystem.Get());
    }

    if (runConfig.AppConfig.HasPQConfig()) {
        const auto& pqConfig = runConfig.AppConfig.GetPQConfig();
        if (pqConfig.GetEnabled() && pqConfig.GetMirrorConfig().GetEnabled()) {
            if (AppData->PersQueueMirrorReaderFactory) {
                AppData->PersQueueMirrorReaderFactory->Initialize(
                    ActorSystem.Get(),
                    pqConfig.GetMirrorConfig().GetPQLibSettings()
                );
            }
        }
    }
}

TIntrusivePtr<TServiceInitializersList> TKikimrRunner::CreateServiceInitializersList(
    const TKikimrRunConfig& runConfig,
    const TBasicKikimrServicesMask& serviceMask) {

    using namespace NKikimrServicesInitializers;
    TIntrusivePtr<TServiceInitializersList> sil(new TServiceInitializersList);

    if (serviceMask.EnableMemoryLog) {
        sil->AddServiceInitializer(new TMemoryLogInitializer(runConfig));
    }

    if (serviceMask.EnableBasicServices) {
        sil->AddServiceInitializer(new TBasicServicesInitializer(runConfig, ModuleFactories));
    }
    if (serviceMask.EnableIcbService) {
        sil->AddServiceInitializer(new TImmediateControlBoardInitializer(runConfig));
    }
    if (serviceMask.EnableWhiteBoard) {
        sil->AddServiceInitializer(new TWhiteBoardServiceInitializer(runConfig));
    }
    if (serviceMask.EnableBSNodeWarden) {
        sil->AddServiceInitializer(new TBSNodeWardenInitializer(runConfig));
    }
    if (serviceMask.EnableSchemeBoardMonitoring) {
        sil->AddServiceInitializer(new TSchemeBoardMonitoringInitializer(runConfig));
    }
    if (serviceMask.EnableStateStorageService) {
        sil->AddServiceInitializer(new TStateStorageServiceInitializer(runConfig));
    }
    if (serviceMask.EnableLocalService) {
        sil->AddServiceInitializer(new TLocalServiceInitializer(runConfig));
    }
    if (serviceMask.EnableSharedCache) {
        sil->AddServiceInitializer(new TSharedCacheInitializer(runConfig));
    }
    if (serviceMask.EnableBlobCache) {
        sil->AddServiceInitializer(new TBlobCacheInitializer(runConfig));
    }
    if (serviceMask.EnableLogger) {
        sil->AddServiceInitializer(new TLoggerInitializer(runConfig, LogSettings, LogBackend));
    }
    if (serviceMask.EnableSchedulerActor) {
        sil->AddServiceInitializer(new TSchedulerActorInitializer(runConfig));
    }
    if (serviceMask.EnableProfiler) {
        sil->AddServiceInitializer(new TProfilerInitializer(runConfig));
    }
    if (serviceMask.EnableResourceBroker) {
        sil->AddServiceInitializer(new TResourceBrokerInitializer(runConfig));
    }
    if (serviceMask.EnableTabletResolver) {
        sil->AddServiceInitializer(new TTabletResolverInitializer(runConfig));
        sil->AddServiceInitializer(new TTabletPipePerNodeCachesInitializer(runConfig));
    }
    if (serviceMask.EnableTabletMonitoringProxy) {
        sil->AddServiceInitializer(new TTabletMonitoringProxyInitializer(runConfig));
    }
    if (serviceMask.EnableTabletCountersAggregator) {
        sil->AddServiceInitializer(new TTabletCountersAggregatorInitializer(runConfig));
    }
    if (serviceMask.EnableGRpcProxyStatus) {
        sil->AddServiceInitializer(new TGRpcProxyStatusInitializer(runConfig));
    }
    if (serviceMask.EnableRestartsCountPublisher) {
        sil->AddServiceInitializer(new TRestartsCountPublisher(runConfig));
    }
    if (serviceMask.EnableBootstrapper) {
        sil->AddServiceInitializer(new TBootstrapperInitializer(runConfig));
    }
    if (serviceMask.EnableMediatorTimeCastProxy) {
        sil->AddServiceInitializer(new TMediatorTimeCastProxyInitializer(runConfig));
    }
    if (serviceMask.EnableTxProxy) {
        sil->AddServiceInitializer(new TTxProxyInitializer(runConfig));
    }

    if (serviceMask.EnableSecurityServices) {
        sil->AddServiceInitializer(new TSecurityServicesInitializer(runConfig, ModuleFactories));
    }
    if (serviceMask.EnablePersQueueClusterTracker) {
        sil->AddServiceInitializer(new TPersQueueClusterTrackerInitializer(runConfig));
    }

    if (serviceMask.EnablePersQueueDirectReadCache) {
        sil->AddServiceInitializer(new TPersQueueDirectReadCacheInitializer(runConfig));
    }

    if (serviceMask.EnableIcNodeCacheService) {
        sil->AddServiceInitializer(new TIcNodeCacheServiceInitializer(runConfig));
    }

    if (serviceMask.EnableMiniKQLCompileService) {
        sil->AddServiceInitializer(new TMiniKQLCompileServiceInitializer(runConfig));
    }

    if (serviceMask.EnableHealthCheckService) {
        sil->AddServiceInitializer(new THealthCheckInitializer(runConfig));
    }

    if (serviceMask.EnableGRpcService) {
        sil->AddServiceInitializer(new TGRpcServicesInitializer(runConfig, ModuleFactories));
    }

#ifdef ACTORSLIB_COLLECT_EXEC_STATS
    if (serviceMask.EnableStatsCollector) {
        sil->AddServiceInitializer(new TStatsCollectorInitializer(runConfig));
    }
#endif
    if (serviceMask.EnableSelfPing) {
        sil->AddServiceInitializer(new TSelfPingInitializer(runConfig));
    }
    if (serviceMask.EnableTabletMonitor) {
        sil->AddServiceInitializer(new NKikimrServicesInitializers::TTabletMonitorInitializer(
            runConfig,
            new NNodeTabletMonitor::TTabletStateClassifier(),
            new NNodeTabletMonitor::TTabletListRenderer()));
    }

    if (serviceMask.EnableViewerService) {
        sil->AddServiceInitializer(new TViewerInitializer(runConfig, ModuleFactories));
    }
    if (serviceMask.EnableLoadService) {
        sil->AddServiceInitializer(new TLoadInitializer(runConfig));
    }
    if (serviceMask.EnableFailureInjectionService) {
        sil->AddServiceInitializer(new TFailureInjectionInitializer(runConfig));
    }
    if (serviceMask.EnablePersQueueL2Cache) {
        sil->AddServiceInitializer(new TPersQueueL2CacheInitializer(runConfig));
    }
    if (serviceMask.EnableNetClassifier) {
        sil->AddServiceInitializer(new TNetClassifierInitializer(runConfig));
    }

    sil->AddServiceInitializer(new TMemProfMonitorInitializer(runConfig, ProcessMemoryInfoProvider));

#if defined(ENABLE_MEMORY_TRACKING)
    if (serviceMask.EnableMemoryTracker) {
        sil->AddServiceInitializer(new TMemoryTrackerInitializer(runConfig));
    }
#endif

    sil->AddServiceInitializer(new TMemoryControllerInitializer(runConfig, ProcessMemoryInfoProvider));

    if (serviceMask.EnableKqp) {
        sil->AddServiceInitializer(new TKqpServiceInitializer(runConfig, ModuleFactories, *this));
    }

    if (serviceMask.EnableMetadataProvider) {
        sil->AddServiceInitializer(new TMetadataProviderInitializer(runConfig));
    }

    if (serviceMask.EnableExternalIndex) {
        sil->AddServiceInitializer(new TExternalIndexInitializer(runConfig));
    }

    if (serviceMask.EnableCompDiskLimiter) {
        sil->AddServiceInitializer(new TCompDiskLimiterInitializer(runConfig));
    }

    if (serviceMask.EnableGroupedMemoryLimiter) {
        sil->AddServiceInitializer(new TGroupedMemoryLimiterInitializer(runConfig));
    }

    if (serviceMask.EnableScanConveyor) {
        sil->AddServiceInitializer(new TScanConveyorInitializer(runConfig));
    }

    if (serviceMask.EnableCompConveyor) {
        sil->AddServiceInitializer(new TCompConveyorInitializer(runConfig));
    }

    if (serviceMask.EnableInsertConveyor) {
        sil->AddServiceInitializer(new TInsertConveyorInitializer(runConfig));
    }

    if (serviceMask.EnableCms) {
        sil->AddServiceInitializer(new TCmsServiceInitializer(runConfig));
    }

    if (serviceMask.EnableSqs) {
        sil->AddServiceInitializer(new TSqsServiceInitializer(runConfig, ModuleFactories));
    }

    if (serviceMask.EnableHttpProxy) {
        sil->AddServiceInitializer(new THttpProxyServiceInitializer(runConfig, ModuleFactories));
    }

    if (serviceMask.EnableConfigsDispatcher) {
        sil->AddServiceInitializer(new TConfigsDispatcherInitializer(runConfig));
    }

    if (serviceMask.EnableConfigsCache) {
        sil->AddServiceInitializer(new TConfigsCacheInitializer(runConfig));
    }

    if (serviceMask.EnableTabletInfo) {
        sil->AddServiceInitializer(new TTabletInfoInitializer(runConfig));
    }

    sil->AddServiceInitializer(new TLeaseHolderInitializer(runConfig));
    sil->AddServiceInitializer(new TConfigValidatorsInitializer(runConfig));

    if (serviceMask.EnableQuoterService) {
        sil->AddServiceInitializer(new TQuoterServiceInitializer(runConfig));
    }

    if (serviceMask.EnableSysViewService) {
        sil->AddServiceInitializer(new TSysViewServiceInitializer(runConfig));
    }

    if (serviceMask.EnableMeteringWriter) {
        sil->AddServiceInitializer(new TMeteringWriterInitializer(runConfig));
    }

    if (serviceMask.EnableAuditWriter) {
        sil->AddServiceInitializer(new TAuditWriterInitializer(runConfig));
    }

    if (serviceMask.EnableLongTxService) {
        sil->AddServiceInitializer(new TLongTxServiceInitializer(runConfig));
    }

    if (serviceMask.EnableKqp || serviceMask.EnableYandexQuery) {
        sil->AddServiceInitializer(new TYqlLogsInitializer(runConfig));
    }

    if (serviceMask.EnableYandexQuery && runConfig.AppConfig.GetFederatedQueryConfig().GetEnabled()) {
        YqSharedResources = NFq::CreateYqSharedResources(
            runConfig.AppConfig.GetFederatedQueryConfig(),
            NKikimr::CreateYdbCredentialsProviderFactory,
            Counters->GetSubgroup("counters", "yq"));
        sil->AddServiceInitializer(new TFederatedQueryInitializer(runConfig, ModuleFactories, YqSharedResources));
    }

    if (serviceMask.EnableSequenceProxyService) {
        sil->AddServiceInitializer(new TSequenceProxyServiceInitializer(runConfig));
    }

    if (serviceMask.EnableReplicationService) {
        sil->AddServiceInitializer(new TReplicationServiceInitializer(runConfig));
    }

    if (serviceMask.EnableLocalPgWire) {
        sil->AddServiceInitializer(new TLocalPgWireServiceInitializer(runConfig));
    }

    if (serviceMask.EnableKafkaProxy) {
        sil->AddServiceInitializer(new TKafkaProxyServiceInitializer(runConfig));
    }

    sil->AddServiceInitializer(new TStatServiceInitializer(runConfig));

    if (serviceMask.EnableDatabaseMetadataCache) {
        sil->AddServiceInitializer(new TDatabaseMetadataCacheInitializer(runConfig));
    }

    if (serviceMask.EnableGraphService) {
        sil->AddServiceInitializer(new TGraphServiceInitializer(runConfig));
    }

    sil->AddServiceInitializer(new TAwsApiInitializer(*this));

    return sil;
}

void TKikimrRunner::KikimrStart() {

    if (!!PollerThreads) {
        PollerThreads->Start();
    }

    ThreadSigmask(SIG_BLOCK);
    if (ActorSystem) {
        ActorSystem->Start();
    }

    if (!!Monitoring) {
        Monitoring->Start(ActorSystem.Get());
    }

    for (auto& server : GRpcServers) {
        if (server.second) {
            server.second->Start();

            TString endpoint;
            if (server.second->GetHost() != "[::]") {
                endpoint = server.second->GetHost();
            }
            endpoint += Sprintf(":%d", server.second->GetPort());
            ActorSystem->Send(NNodeWhiteboard::MakeNodeWhiteboardServiceId(ActorSystem->NodeId),
                              new NNodeWhiteboard::TEvWhiteboard::TEvSystemStateAddEndpoint(server.first, endpoint));
        }
    }

    if (SqsHttp) {
        SqsHttp->Start();
    }

    EnableActorCallstack();
    ThreadSigmask(SIG_UNBLOCK);
}

void TKikimrRunner::KikimrStop(bool graceful) {
    Y_UNUSED(graceful);

    if (EnabledGrpcService) {
        ActorSystem->Send(new IEventHandle(NGRpcService::CreateGrpcPublisherServiceActorId(), {}, new TEvents::TEvPoisonPill));
    }

    TIntrusivePtr<TDrainProgress> drainProgress(new TDrainProgress());
    if (AppData->FeatureFlags.GetEnableDrainOnShutdown() && GracefulShutdownSupported && ActorSystem) {
        drainProgress->OnSend();
        ActorSystem->Send(new IEventHandle(MakeTenantPoolRootID(), {}, new TEvLocal::TEvLocalDrainNode(drainProgress)));
    }

    if (KqpShutdownController) {
        KqpShutdownController->Stop();
    }

    DisableActorCallstack();

    if (AppData->FeatureFlags.GetEnableDrainOnShutdown() && GracefulShutdownSupported) {
        for (ui32 i = 0; i < 300; i++) {
            auto cnt = drainProgress->GetOnlineTabletsEstimate();
            if (cnt > 0) {
                Cerr << "Waiting for drain to complete: " << cnt << " tablets are online on node." << Endl;
            }

            if (drainProgress->CheckCompleted() || cnt == 0) {
                Cerr << "Drain completed." << Endl;
                break;
            }

            Sleep(TDuration::MilliSeconds(100));
        }

        auto stillOnline = drainProgress->GetOnlineTabletsEstimate();
        if (stillOnline > 0) {
            Cerr << "Drain completed, but " << *stillOnline << " tablet(s) are online." << Endl;
        }
    }

    if (ActorSystem) {
        ActorSystem->BroadcastToProxies([](const TActorId& proxyId) {
            return new IEventHandle(proxyId, {}, new TEvInterconnect::TEvTerminate);
        });
        ActorSystem->Send(new IEventHandle(MakeInterconnectListenerActorId(false), {}, new TEvents::TEvPoisonPill));
        ActorSystem->Send(new IEventHandle(MakeInterconnectListenerActorId(true), {}, new TEvents::TEvPoisonPill));
    }

    if (SqsHttp) {
        SqsHttp->Shutdown();
    }

    if (Monitoring) {
        Monitoring->Stop();
    }

    if (PollerThreads) {
        PollerThreads->Stop();
    }

    if (SqsHttp) {
        SqsHttp.Destroy();
    }

    // stop processing grpc requests/response - we must stop feeding ActorSystem
    for (auto& server : GRpcServers) {
        if (server.second) {
            server.second->Stop();
        }
    }

    if (ActorSystem) {
        ActorSystem->Stop();
    }

    for (auto& server : GRpcServers) {
        server.second.Destroy();
    }

    if (YqSharedResources) {
        YqSharedResources->Stop();
    }

    if (ActorSystem) {
        ActorSystem->Cleanup();
    }

    if (ModuleFactories) {
        if (ModuleFactories->DataShardExportFactory) {
            ModuleFactories->DataShardExportFactory->Shutdown();
        }
    }

    if (YdbDriver) {
        YdbDriver->Stop(true);
    }
}

void TKikimrRunner::BusyLoop() {
    auto shouldContinueState = KikimrShouldContinue.PollState();
    while (shouldContinueState == TProgramShouldContinue::Continue) {
        // TODO make this interval configurable
        Sleep(TDuration::MilliSeconds(10));
        shouldContinueState = KikimrShouldContinue.PollState();
    }
}

TProgramShouldContinue TKikimrRunner::KikimrShouldContinue;

void TKikimrRunner::OnTerminate(int) {
    KikimrShouldContinue.ShouldStop(0);
}


void TKikimrRunner::SetSignalHandlers() {
#ifdef _unix_
    signal(SIGPIPE, SIG_IGN);
#endif
    signal(SIGINT, &TKikimrRunner::OnTerminate);
    signal(SIGTERM, &TKikimrRunner::OnTerminate);

#if !defined(_win_)
    SetAsyncSignalHandler(SIGHUP, [](int) {
        TLogBackend::ReopenAllBackends();
    });
#endif
}

void TKikimrRunner::InitializeRegistries(const TKikimrRunConfig& runConfig) {
    TypeRegistry.Reset(new NScheme::TKikimrTypeRegistry());
    TypeRegistry->CalculateMetadataEtag();

    FunctionRegistry.Reset(NMiniKQL::CreateFunctionRegistry(NMiniKQL::CreateBuiltinRegistry())->Clone());
    FormatFactory.Reset(new TFormatFactory);

    const TString& udfsDir = runConfig.AppConfig.GetUDFsDir();

    TVector<TString> udfsPaths;
    if (!udfsDir.empty()) {
        if (NFs::Exists(udfsDir) && IsDir(udfsDir)) {
            NMiniKQL::FindUdfsInDir(udfsDir, &udfsPaths);
            if (udfsPaths.empty()) {
                Cout << "UDF directory " << udfsDir << " contains no dynamic UDFs. " << Endl;
            } else {
                Cout << "UDF directory " << udfsDir << " contains " << udfsPaths.size() << " dynamic UDFs. " << Endl;
            }
            NMiniKQL::TUdfModuleRemappings remappings;
            for (const auto& udfPath : udfsPaths) {
                FunctionRegistry->LoadUdfs(udfPath, remappings, 0);
            }
        } else {
            Cout << "UDF directory " << udfsDir << " doesn't exist, no dynamic UDFs will be loaded. " << Endl;
        }
    } else {
        Cout << "UDFsDir is not specified, no dynamic UDFs will be loaded. " << Endl;
    }

    NKikimr::NMiniKQL::FillStaticModules(*FunctionRegistry);

    NKikHouse::RegisterFormat(*FormatFactory);
}

TIntrusivePtr<TKikimrRunner> TKikimrRunner::CreateKikimrRunner(
        const TKikimrRunConfig& runConfig,
        std::shared_ptr<TModuleFactories> factories) {
    TIntrusivePtr<TKikimrRunner> runner(new TKikimrRunner(factories));
    runner->InitializeAllocator(runConfig);
    runner->InitializeRegistries(runConfig);
    runner->InitializeMonitoring(runConfig);
    runner->InitializeControlBoard(runConfig);
    runner->InitializeAppData(runConfig);
    runner->InitializeLogSettings(runConfig);
    TIntrusivePtr<TServiceInitializersList> sil(runner->CreateServiceInitializersList(runConfig, runConfig.ServicesMask));
    runner->InitializeActorSystem(runConfig, sil, runConfig.ServicesMask);
    runner->InitializeMonitoringLogin(runConfig);
    runner->InitializeKqpController(runConfig);
    runner->InitializeGracefulShutdown(runConfig);
    runner->InitializeGRpc(runConfig);
    return runner;
}

} // NKikimr
