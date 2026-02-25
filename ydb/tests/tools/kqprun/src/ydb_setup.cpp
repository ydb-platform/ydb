#include "ydb_setup.h"

#include <library/cpp/colorizer/colors.h>

#include <ydb/core/blob_depot/mon_main.h>
#include <ydb/core/client/server/msgbus_server_pq_metacache.h>
#include <ydb/core/grpc_services/grpc_request_proxy.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/testlib/basics/storage.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/services/persqueue_v1/grpc_pq_schema.h>
#include <ydb/services/persqueue_v1/services_initializer.h>

#include <ydb/tests/tools/kqprun/runlib/kikimr_setup.h>
#include <ydb/tests/tools/kqprun/src/proto/storage_meta.pb.h>

#include <yql/essentials/utils/log/log.h>
#include <yql/essentials/utils/log/tls_backend.h>

using namespace NKikimrRun;

namespace NKqpRun {

namespace {

class TKqprunServer : public NKikimr::Tests::TServer {
    using TBase = NKikimr::Tests::TServer;

public:
    using TPtr = TIntrusivePtr<TKqprunServer>;

    explicit TKqprunServer(const NKikimr::Tests::TServerSettings& settings)
        : TBase(settings, /* defaultInit */ false)
    {}

    void Initialize() {
        TBase::Initialize();
    }
};

class TSessionState {
public:
    explicit TSessionState(NActors::TTestActorRuntime* runtime, ui32 targetNodeIndex, const TString& database, const TString& traceId, TYdbSetupSettings::EVerbosity verbosityLevel)
        : Runtime_(runtime)
        , TargetNodeIndex_(targetNodeIndex)
    {
        auto event = std::make_unique<NKikimr::NKqp::TEvKqp::TEvCreateSessionRequest>();
        event->Record.SetTraceId(traceId);
        event->Record.SetApplicationName("kqprun");
        event->Record.MutableRequest()->SetDatabase(database);

        auto openPromise = NThreading::NewPromise<TString>();
        auto closePromise = NThreading::NewPromise<void>();
        SessionHolderActor_ = Runtime_->Register(CreateSessionHolderActor(TCreateSessionRequest{
            .Event = std::move(event),
            .TargetNode = Runtime_->GetNodeId(targetNodeIndex),
            .VerbosityLevel = verbosityLevel
        }, openPromise, closePromise));

        SessionId_ = openPromise.GetFuture().GetValueSync();
        CloseFuture_ = closePromise.GetFuture();
    }

    TString GetSessionId() const {
        CheckSession("execute request");
        return SessionId_;
    }

    void CloseSession() const {
        CheckSession("close session");
        Runtime_->Send(SessionHolderActor_, Runtime_->AllocateEdgeActor(TargetNodeIndex_), new NActors::TEvents::TEvPoison(), TargetNodeIndex_);
        CloseFuture_.GetValueSync();
    }

private:
    void CheckSession(const TString& action) const {
        if (CloseFuture_.HasException()) {
            CloseFuture_.TryRethrow();
        }
        if (CloseFuture_.HasValue()) {
            ythrow yexception() << "Failed to " << action << ", session unexpectedly closed\n";
        }
    }

private:
    NActors::TTestActorRuntime* Runtime_;
    ui32 TargetNodeIndex_ = 0;

    NThreading::TFuture<void> CloseFuture_;
    NActors::TActorId SessionHolderActor_;
    TString SessionId_;
};


void FillQueryMeta(TQueryMeta& meta, const NKikimrKqp::TQueryResponse& response) {
    meta.Ast = response.GetQueryAst();
    if (const auto& plan = response.GetQueryPlan()) {
        meta.Plan = plan;
    }
    meta.TotalDuration = TDuration::MicroSeconds(response.GetQueryStats().GetDurationUs());
}

}  // anonymous namespace


//// TYdbSetup::TImpl

class TYdbSetup::TImpl : public TKikimrSetupBase {
    using TBase = TKikimrSetupBase;
    using EVerbosity = TYdbSetupSettings::EVerbosity;
    using EHealthCheck = TYdbSetupSettings::EHealthCheck;

    class TPortGenerator {
    public:
        TPortGenerator(TPortManager& portManager, ui32 firstPort)
            : PortManager_(portManager)
            , Port_(firstPort)
        {}

        ui32 GetPort() {
            if (!Port_) {
                return PortManager_.GetPort();
            }
            return Port_++;
        }

    private:
        TPortManager& PortManager_;
        ui32 Port_;
    };

    inline static NColorizer::TColors CoutColors_ = NColorizer::AutoColors(Cout);
    inline static NColorizer::TColors CerrColors_ = NColorizer::AutoColors(Cerr);
    inline static std::terminate_handler TerminateHandler_;
    inline static std::unordered_map<int, void (*)(int)> SignalHandlers_;
    inline static std::unique_ptr<TFileHandle> StorageHolder_;
    inline static std::atomic<int> CurrentSignal_ = 0;

private:
    void SetStorageSettings(NKikimr::Tests::TServerSettings& serverSettings) {
        TFsPath diskPath;
        if (Settings_.PDisksPath && *Settings_.PDisksPath != "-") {
            diskPath = *Settings_.PDisksPath;
            if (!diskPath) {
                ythrow yexception() << "Storage directory path should not be empty";
            }
            if (diskPath.IsRelative()) {
                diskPath = TFsPath::Cwd() / diskPath;
            }
            diskPath.Fix();
            diskPath.MkDir();
            if (Settings_.VerbosityLevel >= EVerbosity::InitLogs) {
                Cout << CoutColors_.Cyan() << "Setup storage by path: " << diskPath.GetPath() << CoutColors_.Default() << Endl;
            }
        }

        bool formatDisk = true;
        if (diskPath) {
            StorageMetaPath_ = TFsPath(diskPath).Child("kqprun_storage_meta.conf");
            if (StorageMetaPath_.Exists() && !Settings_.FormatStorage) {
                if (!google::protobuf::TextFormat::ParseFromString(TFileInput(StorageMetaPath_.GetPath()).ReadAll(), &StorageMeta_)) {
                    ythrow yexception() << "Storage meta is corrupted, please use --format-storage";
                }
                formatDisk = false;
            }

            if (Settings_.DiskSize && StorageMeta_.GetStorageSize() != *Settings_.DiskSize) {
                if (!formatDisk) {
                    ythrow yexception() << "Cannot change disk size without formatting storage, current disk size " << NKikimr::NBlobDepot::FormatByteSize(StorageMeta_.GetStorageSize()) << ", please use --format-storage";
                }
                StorageMeta_.SetStorageSize(*Settings_.DiskSize);
            } else if (!StorageMeta_.GetStorageSize()) {
                StorageMeta_.SetStorageSize(DEFAULT_STORAGE_SIZE);
            }

            const TString& domainName = NKikimr::CanonizePath(Settings_.DomainName);
            if (!StorageMeta_.GetDomainName()) {
                StorageMeta_.SetDomainName(domainName);
            } else if (StorageMeta_.GetDomainName() != domainName) {
                ythrow yexception() << "Cannot change domain name without formatting storage, current name " << StorageMeta_.GetDomainName() << ", please use --format-storage";
            }

            if (!Settings_.NodeCount) {
                Settings_.NodeCount = StorageMeta_.GetNodesCount();
            }
            if (!Settings_.StorageGroupCount) {
                Settings_.StorageGroupCount = StorageMeta_.GetStorageGroupsCount();
            }

            UpdateStorageMeta();
        }

        TString storagePath = diskPath.GetPath();
        SlashFolderLocal(storagePath);

        const NKikimr::NFake::TStorage storage = {
            .UseDisk = !!Settings_.PDisksPath,
            .SectorSize = NKikimr::TTestStorageFactory::SECTOR_SIZE,
            .ChunkSize = Settings_.PDisksPath ? NKikimr::TTestStorageFactory::CHUNK_SIZE : NKikimr::TTestStorageFactory::MEM_CHUNK_SIZE,
            .DiskSize = Settings_.DiskSize ? *Settings_.DiskSize : 32_GB,
            .FormatDisk = formatDisk,
            .DiskPath = storagePath,
            .EventDispatchTimeout = TDuration::Max()
        };

        serverSettings.SetEnableMockOnSingleNode(!Settings_.DisableDiskMock && !Settings_.PDisksPath);
        serverSettings.SetCustomDiskParams(storage);
        serverSettings.SetStorageGeneration(0, /* fetchPoolsGeneration */ true);
    }

    static void HandleFinalizeSignal(int signal) {
        int expected = 0;
        CurrentSignal_.compare_exchange_strong(expected, signal);
    }

    static void FlushStorageFileHolderOnTerminate() {
        Finalize();

        if (TerminateHandler_) {
            TerminateHandler_();
        }
    }

    void SetupStorageFileHolder(const TString& storagePath) {
        StorageHolder_ = std::make_unique<TFileHandle>(TFsPath(storagePath).Child("pdisk_1.dat"), RdWr);
        if (!StorageHolder_->IsOpen()) {
            ythrow yexception() << "Failed to open storage file: " << storagePath;
        }

        std::atexit(&Finalize);
        TerminateHandler_ = std::set_terminate(&FlushStorageFileHolderOnTerminate);

        SignalHandlerPool_ = MakeHolder<TThreadPool>();
        SignalHandlerPool_->Start(1);
        Y_ENSURE(SignalHandlerPool_->AddFunc([]() {
            while (true) {
                const auto signal = CurrentSignal_.load();
                if (!signal) {
                    Sleep(TDuration::MilliSeconds(100));
                    continue;
                }

                Finalize();
                Cout << Endl << CoutColors_.Yellow() << "INTERRUPTED" << CoutColors_.Default() << Endl;

                if (const auto it = SignalHandlers_.find(signal); it != SignalHandlers_.end()) {
                    std::signal(signal, it->second);
                    std::raise(signal);
                }

                std::exit(1);
            }
        }));

        for (auto sig : {SIGTERM, SIGABRT, SIGINT}) {
            const auto prevHandler = std::signal(sig, &HandleFinalizeSignal);
            Y_ENSURE(prevHandler != SIG_ERR);
            SignalHandlers_.emplace(sig, prevHandler);
        }
    }

private:
    void DistributeDefaultResources() {
        static constexpr ui32 PDISKS_COUNT = 1;
        static constexpr ui32 PDISKS_SLOTS_COUNT = 16; // Maximal number of VDisks in PDisk (in kqprun storage config 1 VDisks <=> 1 Storage group)

        if (!Settings_.NodeCount) {
            Settings_.NodeCount = 1;
        }

        ui64 usedSlots = 1 + Settings_.StorageGroupCount; // One PDisk slot is reserved for static storage group
        ui64 tenantsToDistribute = !Settings_.StorageGroupCount;
        for (auto& [_, tenant] : Settings_.Tenants) {
            if (tenant.GetType() != TStorageMeta::TTenant::SERVERLESS) {
                if (!tenant.GetNodesCount()) {
                    tenant.SetNodesCount(1);
                }

                usedSlots += tenant.GetStorageGroupsCount();
                tenantsToDistribute += !tenant.GetStorageGroupsCount();
            }
        }

        const auto totalSlots = PDISKS_COUNT * PDISKS_SLOTS_COUNT;
        if (usedSlots + tenantsToDistribute > totalSlots) {
            auto storageInfo = TStringBuilder() << ".\nMaximum number of storage groups is "
                << totalSlots - 1 << ", number of PDisks: " << PDISKS_COUNT
                << ", number of slots per PDisk: " << PDISKS_SLOTS_COUNT << ", one group is reserved for static storage group";

            if (usedSlots > totalSlots) {
                ythrow yexception() << "Too many storage groups requested: " << usedSlots - 1 << ", try to format storage" << storageInfo;
            } else {
                ythrow yexception() << "Too many tenants requested, can not allocate at least one storage group for " << tenantsToDistribute
                    << " tenants" << (usedSlots - 1 ? TStringBuilder() << ", already used storage groups: " << usedSlots - 1 : TStringBuilder()) << ", try to format storage" << storageInfo;
            }
        }

        auto freeSlots = totalSlots - usedSlots;
        Y_ENSURE(freeSlots >= tenantsToDistribute);

        const auto extractSlots = [&freeSlots, &tenantsToDistribute]() {
            Y_ENSURE(tenantsToDistribute > 0);
            auto slots = freeSlots / tenantsToDistribute;
            freeSlots -= slots;
            tenantsToDistribute--;
            Y_ENSURE(freeSlots >= tenantsToDistribute);
            return slots;
        };

        if (!Settings_.StorageGroupCount) {
            if (Settings_.Tenants.empty()) {
                Settings_.StorageGroupCount = extractSlots();
            } else {
                Y_ENSURE(tenantsToDistribute > 0);
                Settings_.StorageGroupCount = 1;
                freeSlots--;
                tenantsToDistribute--;
            }
        }

        for (auto& [_, tenant] : Settings_.Tenants) {
            if (tenant.GetType() != TStorageMeta::TTenant::SERVERLESS && !tenant.GetStorageGroupsCount()) {
                tenant.SetStorageGroupsCount(extractSlots());
            }
        }

        StorageMeta_.SetNodesCount(Settings_.NodeCount);
        StorageMeta_.SetStorageGroupsCount(Settings_.StorageGroupCount);
        UpdateStorageMeta();
    }

    NKikimr::Tests::TServerSettings GetServerSettings(ui32 grpcPort) {
        auto serverSettings = TBase::GetServerSettings(Settings_, grpcPort, Settings_.VerbosityLevel >= EVerbosity::InitLogs);
        SetStorageSettings(serverSettings);

        for (const auto& [tenantPath, tenantInfo] : StorageMeta_.GetTenants()) {
            const auto [it, inserted] = Settings_.Tenants.emplace(tenantPath, tenantInfo);
            if (inserted) {
                continue;
            }

            if (tenantInfo.GetType() != TStorageMeta::TTenant::SERVERLESS) {
                auto& info = it->second;
                if (!info.GetNodesCount()) {
                    info.SetNodesCount(tenantInfo.GetNodesCount());
                }
                if (!info.GetStorageGroupsCount()) {
                    info.SetStorageGroupsCount(tenantInfo.GetStorageGroupsCount());
                } else if (info.GetStorageGroupsCount() < tenantInfo.GetStorageGroupsCount()) {
                    ythrow yexception() << "Reducing number of storage groups is not allowed, number of storage groups in tenant " << tenantPath << " is " << tenantInfo.GetStorageGroupsCount();
                }
            }
        }

        DistributeDefaultResources();
        serverSettings
            .SetNodeCount(Settings_.NodeCount)
            .SetDataCenterCount(Settings_.DcCount)
            .SetPqGateway(Settings_.PqGateway);

        serverSettings.StoragePoolTypes.clear();
        serverSettings.AddStoragePool("test", TStringBuilder() << NKikimr::CanonizePath(Settings_.DomainName) << ":test", Settings_.StorageGroupCount);

        ui32 dynNodesCount = 0;
        for (const auto& [tenantPath, tenantInfo] : Settings_.Tenants) {
            if (tenantInfo.GetType() != TStorageMeta::TTenant::SERVERLESS) {
                serverSettings.AddStoragePool(tenantPath, TStringBuilder() << GetTenantPath(tenantPath) << ":" << tenantPath, tenantInfo.GetStorageGroupsCount());
                dynNodesCount += tenantInfo.GetNodesCount();
            }
        }
        serverSettings.SetDynamicNodeCount(dynNodesCount);

        return serverSettings;
    }

    void CreateTenant(Ydb::Cms::CreateDatabaseRequest&& request, const TString& relativePath, const TString& type, TStorageMeta::TTenant tenantInfo) {
        const auto absolutePath = request.path();
        const auto [it, inserted] = StorageMeta_.MutableTenants()->emplace(relativePath, tenantInfo);
        if (inserted || it->second.GetCreationInProgress()) {
            if (Settings_.VerbosityLevel >= EVerbosity::Info) {
                Cout << CoutColors_.Yellow() << TInstant::Now().ToIsoStringLocal() << " Creating " << type << " tenant " << absolutePath << "..." << CoutColors_.Default() << Endl;
            }

            it->second.SetCreationInProgress(true);
            UpdateStorageMeta();

            Tenants_->CreateTenant(std::move(request), tenantInfo.GetNodesCount(), TENANT_CREATION_TIMEOUT, true);

            it->second.SetCreationInProgress(false);
            UpdateStorageMeta();
        } else {
            if (it->second.GetType() != tenantInfo.GetType()) {
                ythrow yexception() << "Can not change tenant " << absolutePath << " type without formatting storage, current type " << TStorageMeta::TTenant::EType_Name(it->second.GetType()) << ", please use --format-storage";
            }
            if (it->second.GetSharedTenant() != tenantInfo.GetSharedTenant()) {
                ythrow yexception() << "Can not change tenant " << absolutePath << " shared resources without formatting storage from '" << it->second.GetSharedTenant() << "', please use --format-storage";
            }
            if (it->second.GetNodesCount() != tenantInfo.GetNodesCount() || it->second.GetStorageGroupsCount() != tenantInfo.GetStorageGroupsCount()) {
                it->second.SetNodesCount(tenantInfo.GetNodesCount());
                it->second.SetStorageGroupsCount(tenantInfo.GetStorageGroupsCount());
                UpdateStorageMeta();
            }
            if (Settings_.VerbosityLevel >= EVerbosity::Info) {
                Cout << CoutColors_.Yellow() << TInstant::Now().ToIsoStringLocal() << " Starting " << type << " tenant " << absolutePath << "..." << CoutColors_.Default() << Endl;
            }
            if (!request.has_serverless_resources()) {
                Tenants_->Run(absolutePath, tenantInfo.GetNodesCount());
            }
        }
    }

    static void AddTenantStoragePool(Ydb::Cms::StorageUnits* storage, const TString& name, ui64 storageGroupsCount) {
        storage->set_unit_kind(name);
        storage->set_count(storageGroupsCount);
    }

    void CreateTenants() {
        std::set<TString> sharedTenants;
        std::map<TString, TStorageMeta::TTenant> serverlessTenants;
        for (const auto& [tenantPath, tenantInfo] : Settings_.Tenants) {
            Ydb::Cms::CreateDatabaseRequest request;
            request.set_path(GetTenantPath(tenantPath));

            switch (tenantInfo.GetType()) {
                case TStorageMeta::TTenant::DEDICATED:
                    AddTenantStoragePool(request.mutable_resources()->add_storage_units(), tenantPath, tenantInfo.GetStorageGroupsCount());
                    CreateTenant(std::move(request), tenantPath, "dedicated", tenantInfo);
                    break;

                case TStorageMeta::TTenant::SHARED:
                    sharedTenants.emplace(tenantPath);
                    AddTenantStoragePool(request.mutable_shared_resources()->add_storage_units(), tenantPath, tenantInfo.GetStorageGroupsCount());
                    CreateTenant(std::move(request), tenantPath, "shared", tenantInfo);
                    break;

                case TStorageMeta::TTenant::SERVERLESS:
                    serverlessTenants.emplace(tenantPath, tenantInfo);
                    break;

                default:
                    ythrow yexception() << "Unexpected tenant type: " << TStorageMeta::TTenant::EType_Name(tenantInfo.GetType());
                    break;
            }
        }

        for (auto [tenantPath, tenantInfo] : serverlessTenants) {
            if (!tenantInfo.GetSharedTenant()) {
                if (sharedTenants.empty()) {
                    ythrow yexception() << "Can not create serverless tenant, there is no shared tenants, please use `--shared <shared name>`";
                }
                if (sharedTenants.size() > 1) {
                    ythrow yexception() << "Can not create serverless tenant, there is more than one shared tenant, please use `--serverless " << tenantPath << "@<shared name>`";
                }
                tenantInfo.SetSharedTenant(*sharedTenants.begin());
            }

            Ydb::Cms::CreateDatabaseRequest request;
            request.set_path(GetTenantPath(tenantPath));
            request.mutable_serverless_resources()->set_shared_database_path(GetTenantPath(tenantInfo.GetSharedTenant()));
            ServerlessToShared_[request.path()] = request.serverless_resources().shared_database_path();
            CreateTenant(std::move(request), tenantPath, "serverless", tenantInfo);
        }
    }

    void InitializeServer(TPortGenerator& grpcPortGen) {
        const ui32 domainGrpcPort = grpcPortGen.GetPort();
        NKikimr::Tests::TServerSettings serverSettings = GetServerSettings(domainGrpcPort);

        Server_ = MakeIntrusive<TKqprunServer>(serverSettings);
        Server_->GetRuntime()->SetDispatchTimeout(TDuration::Max());
        Server_->Initialize();

        if (serverSettings.CustomDiskParams.UseDisk) {
            SetupStorageFileHolder(serverSettings.CustomDiskParams.DiskPath);
        }

        if (Settings_.GrpcEnabled) {
            Server_->EnableGRpc(GetGrpcSettings(domainGrpcPort, 0));
        }

        Client_ = MakeHolder<NKikimr::Tests::TClient>(serverSettings);
        Client_->InitRootScheme();

        Tenants_ = MakeHolder<NKikimr::Tests::TTenants>(Server_);
        CreateTenants();
    }

    void InitializeYqlLogger() {
        if (!Settings_.TraceOptEnabled) {
            return;
        }

        ModifyLogPriorities({{NKikimrServices::EServiceKikimr::KQP_YQL, NActors::NLog::PRI_TRACE}}, *Settings_.AppConfig.MutableLogConfig());
        NYql::NLog::InitLogger(NActors::CreateNullBackend());
    }

    NThreading::TFuture<void> InitializeTenantNodes(const TString& database, const std::optional<NKikimrWhiteboard::TSystemStateInfo>& systemStateInfo, TPortGenerator& grpcPortGen) const {
        EHealthCheck level = Settings_.HealthCheckLevel;
        i32 nodesCount = Settings_.NodeCount;
        TVector<ui32> tenantNodesIdx;
        if (database != Settings_.DomainName) {
            tenantNodesIdx = Tenants_->List(database);
            nodesCount = tenantNodesIdx.size();
        }

        const auto& absolutePath = NKikimr::CanonizePath(database);
        const TWaitResourcesSettings settings = {
            .ExpectedNodeCount = nodesCount,
            .HealthCheckLevel = level,
            .HealthCheckTimeout = Settings_.HealthCheckTimeout,
            .VerbosityLevel = Settings_.VerbosityLevel,
            .Database = absolutePath
        };
        const auto edgeActor = GetRuntime()->AllocateEdgeActor();

        std::vector<NThreading::TFuture<void>> futures;
        futures.reserve(nodesCount);
        for (i32 nodeIdx = 0; nodeIdx < nodesCount; ++nodeIdx) {
            const auto node = tenantNodesIdx ? tenantNodesIdx[nodeIdx] : nodeIdx;
            if (Settings_.GrpcEnabled) {
                if (node > 0) {
                    // Port for first static node also used in cluster initialization
                    Server_->EnableGRpc(GetGrpcSettings(grpcPortGen.GetPort(), node), node, absolutePath);
                }
            } else {
                NKikimr::NGRpcProxy::V1::IClustersCfgProvider* clustersCfgProvider = nullptr;
                NKikimr::NGRpcService::V1::ServicesInitializer(GetRuntime()->GetActorSystem(node), NKikimr::NMsgBusProxy::CreatePersQueueMetaCacheV2Id(), MakeIntrusive<NMonitoring::TDynamicCounters>(), &clustersCfgProvider).Execute();

                auto grpcRequestProxy = NKikimr::NGRpcService::CreateGRpcRequestProxy(Settings_.AppConfig);
                auto grpcRequestProxyId = GetRuntime()->Register(grpcRequestProxy, node, GetRuntime()->GetAppData(node).UserPoolId);
                GetRuntime()->GetActorSystem(node)->RegisterLocalService(NKikimr::NGRpcService::CreateGRpcRequestProxyId(), grpcRequestProxyId);

                if (Settings_.MonitoringEnabled) {
                    NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor(node);
                    GetRuntime()->Register(NKikimr::CreateBoardPublishActor(NKikimr::MakeEndpointsBoardPath(absolutePath), "", edgeActor, 0, true), node, GetRuntime()->GetAppData(node).UserPoolId);
                }
            }

            if (systemStateInfo) {
                GetRuntime()->Send(NKikimr::NNodeWhiteboard::MakeNodeWhiteboardServiceId(GetRuntime()->GetNodeId(node)), edgeActor, new NKikimr::NNodeWhiteboard::TEvWhiteboard::TEvSystemStateUpdate(*systemStateInfo));
            }

            const auto promise = NThreading::NewPromise();
            GetRuntime()->Register(CreateResourcesWaiterActor(promise, settings), tenantNodesIdx ? tenantNodesIdx[nodeIdx] : nodeIdx, GetRuntime()->GetAppData().SystemPoolId);
            futures.emplace_back(promise.GetFuture());
        }

        return NThreading::WaitAll(futures);
    }

    void InitializeTenants(TPortGenerator& grpcPortGen) const {
        const auto& systemStateInfo = GetSystemStateInfo(Server_->GetProcessMemoryInfoProvider());

        std::vector<NThreading::TFuture<void>> futures(1, InitializeTenantNodes(Settings_.DomainName, systemStateInfo, grpcPortGen));
        futures.reserve(StorageMeta_.GetTenants().size() + 1);
        for (const auto& [tenantName, tenantInfo] : StorageMeta_.GetTenants()) {
            if (tenantInfo.GetType() != TStorageMeta::TTenant::SERVERLESS) {
                futures.emplace_back(InitializeTenantNodes(GetTenantPath(tenantName), systemStateInfo, grpcPortGen));
            }
        }

        try {
            NThreading::WaitAll(futures).GetValue(2 * Settings_.HealthCheckTimeout);
        } catch (...) {
            ythrow yexception() << "Failed to initialize all resources: " << CurrentExceptionMessage();
        }
    }

public:
    explicit TImpl(const TYdbSetupSettings& settings)
        : Settings_(settings)
    {
        TPortGenerator grpcPortGen(PortManager, Settings_.FirstGrpcPort);
        InitializeYqlLogger();
        InitializeServer(grpcPortGen);
        InitializeTenants(grpcPortGen);

        if (Settings_.MonitoringEnabled && Settings_.VerbosityLevel >= EVerbosity::Info) {
            for (ui32 nodeIndex = 0; nodeIndex < Settings_.NodeCount; ++nodeIndex) {
                const auto port = Server_->GetRuntime()->GetMonPort(nodeIndex);
                Cout << CoutColors_.Cyan() << "Monitoring port"
                    << (Server_->StaticNodes() + Server_->DynamicNodes() > 1 ? TStringBuilder() << " for static node " << nodeIndex + 1 : TString())
                    << ": " << CoutColors_.Default()
                    << (nodeIndex == 0 ? FormatMonitoringLink(port, "monitoring/cluster/tenants") : ToString(port)) << Endl;
            }

            const auto printTenantNodes = [this](const std::pair<TString, TStorageMeta::TTenant>& tenantInfo) {
                if (tenantInfo.second.GetType() == TStorageMeta::TTenant::SERVERLESS) {
                    return;
                }
                const auto& nodes = Tenants_->List(GetTenantPath(tenantInfo.first));
                for (auto it = nodes.rbegin(); it != nodes.rend(); ++it) {
                    Cout << CoutColors_.Cyan() << "Monitoring port for dynamic node " << *it + 1 << " [" << tenantInfo.first << "]: " << CoutColors_.Default() << Server_->GetRuntime()->GetMonPort(*it) << Endl;
                }
            };
            std::for_each(Settings_.Tenants.rbegin(), Settings_.Tenants.rend(), std::bind(printTenantNodes, std::placeholders::_1));
        }

        if (Settings_.GrpcEnabled && Settings_.VerbosityLevel >= EVerbosity::Info) {
            Cout << CoutColors_.Cyan() << "Domain gRPC port: " << CoutColors_.Default() << FormatGrpcLink(Server_->GetGRpcServer().GetPort()) << Endl;
            for (const auto& [tenantPath, tenantInfo] : Settings_.Tenants) {
                if (tenantInfo.GetType() != TStorageMeta::TTenant::SERVERLESS) {
                    Cout << CoutColors_.Cyan() << "Tenant [" << tenantPath << "] gRPC port: " << CoutColors_.Default() << Server_->GetTenantGRpcServer(GetTenantPath(tenantPath)).GetPort() << Endl;
                }
            }
        }
    }

    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr SchemeQueryRequest(const TRequestOptions& query) {
        ui32 nodeIndex = GetNodeIndexForDatabase(query.Database);
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_DDL, nodeIndex, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvQueryRequest, NKikimr::NKqp::TEvKqp::TEvQueryResponse>(std::move(event), nodeIndex);
    }

    NKikimr::NKqp::TEvKqp::TEvScriptResponse::TPtr ScriptRequest(const TScriptRequest& script) {
        ui32 nodeIndex = GetNodeIndexForDatabase(script.Options.Database);
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvScriptRequest>();
        event->RetryMapping = script.RetryMapping;
        FillQueryRequest(script.Options, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT, nodeIndex, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvScriptRequest, NKikimr::NKqp::TEvKqp::TEvScriptResponse>(std::move(event), nodeIndex);
    }

    TQueryResponse QueryRequest(const TRequestOptions& query, TProgressCallback progressCallback) {
        auto request = GetQueryRequest(query);
        auto promise = NThreading::NewPromise<TQueryResponse>();
        GetRuntime()->Register(CreateRunScriptActorMock(std::move(request), promise, progressCallback), request.TargetNode - GetRuntime()->GetFirstNodeId(), GetRuntime()->GetAppData().UserPoolId);

        return promise.GetFuture().GetValueSync();
    }

    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr YqlScriptRequest(const TRequestOptions& query) {
        ui32 nodeIndex = GetNodeIndexForDatabase(query.Database);
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_SCRIPT, nodeIndex, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvQueryRequest, NKikimr::NKqp::TEvKqp::TEvQueryResponse>(std::move(event), nodeIndex);
    }

    NKikimr::NKqp::TEvGetScriptExecutionOperationResponse::TPtr GetScriptExecutionOperationRequest(const TString& database, const TString& operation, const TString& userSID) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvGetScriptExecutionOperation>(GetDatabasePath(database), operationId, userSID);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvGetScriptExecutionOperation, NKikimr::NKqp::TEvGetScriptExecutionOperationResponse>(std::move(event), database);
    }

    NKikimr::NKqp::TEvFetchScriptResultsResponse::TPtr FetchScriptExecutionResultsRequest(const TString& database, const TString& operation, const TString& userSID, i32 resultSetId) const {
        TString error;
        const auto executionId = NKikimr::NKqp::ScriptExecutionIdFromOperation(operation, error);
        Y_ENSURE(executionId, error);

        ui32 nodeIndex = GetNodeIndexForDatabase(database);
        NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor(nodeIndex);
        auto rowsLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultRowsLimit();
        auto sizeLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultSizeLimit();
        NActors::IActor* fetchActor = NKikimr::NKqp::CreateGetScriptExecutionResultActor(edgeActor, GetDatabasePath(database), *executionId, userSID, resultSetId, 0, rowsLimit, sizeLimit, TInstant::Max());

        GetRuntime()->Register(fetchActor, nodeIndex, GetRuntime()->GetAppData(nodeIndex).UserPoolId);

        return GetRuntime()->GrabEdgeEvent<NKikimr::NKqp::TEvFetchScriptResultsResponse>(edgeActor);
    }

    NKikimr::NKqp::TEvForgetScriptExecutionOperationResponse::TPtr ForgetScriptExecutionOperationRequest(const TString& database, const TString& operation, const TString& userSID) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvForgetScriptExecutionOperation>(GetDatabasePath(database), operationId, userSID);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvForgetScriptExecutionOperation, NKikimr::NKqp::TEvForgetScriptExecutionOperationResponse>(std::move(event), database);
    }

    NKikimr::NKqp::TEvCancelScriptExecutionOperationResponse::TPtr CancelScriptExecutionOperationRequest(const TString& database, const TString& operation, const TString& userSID) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvCancelScriptExecutionOperation>(GetDatabasePath(database), operationId, userSID);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvCancelScriptExecutionOperation, NKikimr::NKqp::TEvCancelScriptExecutionOperationResponse>(std::move(event), database);
    }

    void QueryRequestAsync(const TRequestOptions& query) {
        if (!AsyncQueryRunnerActorId_) {
            AsyncQueryRunnerActorId_ = GetRuntime()->Register(CreateAsyncQueryRunnerActor(Settings_.AsyncQueriesSettings), 0, GetRuntime()->GetAppData().UserPoolId);
        }

        auto request = GetQueryRequest(query);
        auto startPromise = NThreading::NewPromise();
        GetRuntime()->Send(*AsyncQueryRunnerActorId_, GetRuntime()->AllocateEdgeActor(), new NKikimrRun::TEvPrivate::TEvStartAsyncQuery(std::move(request), startPromise));

        return startPromise.GetFuture().GetValueSync();
    }

    void WaitAsyncQueries() const {
        if (!AsyncQueryRunnerActorId_) {
            return;
        }

        auto finalizePromise = NThreading::NewPromise();
        GetRuntime()->Send(*AsyncQueryRunnerActorId_, GetRuntime()->AllocateEdgeActor(), new NKikimrRun::TEvPrivate::TEvFinalizeAsyncQueryRunner(finalizePromise));

        return finalizePromise.GetFuture().GetValueSync();
    }

    void CloseSessions() const {
        if (!SessionState_) {
            return;
        }

        SessionState_->CloseSession();
    }

    void StartTraceOpt() const {
        if (!Settings_.TraceOptEnabled) {
            ythrow yexception() << "Trace opt was disabled";
        }

        NYql::NLog::YqlLogger().ResetBackend(MakeHolder<NYql::NLog::TTlsLogBackend>(CreateLogBackend(Settings_)));
    }

    static void StopTraceOpt() {
        NYql::NLog::YqlLogger().ResetBackend(NActors::CreateNullBackend());
    }

    TString GetDefaultDatabase() const {
        if (StorageMeta_.TenantsSize() > 1) {
            ythrow yexception() << "Can not choose default database, there is more than one tenants, please use `-D <database name>`";
        }
        if (StorageMeta_.TenantsSize() == 1) {
            return GetTenantPath(StorageMeta_.GetTenants().begin()->first);
        }
        return Settings_.DomainName;
    }

    static void Finalize() try {
        if (StorageHolder_) {
            if (!StorageHolder_->Flush()) {
                Cerr << CerrColors_.Red() << "Failed to flush storage data, errno: " << errno << CerrColors_.Default() << Endl;
            }
            StorageHolder_.reset();
        }
    } catch (...) {
        Cerr << CerrColors_.Red() << "Failed to finalize: " << CurrentExceptionMessage() << CerrColors_.Default() << Endl;
    }

private:
    NActors::TTestActorRuntime* GetRuntime() const override {
        return Server_->GetRuntime();
    }

    template <typename TRequest, typename TResponse>
    typename TResponse::TPtr RunKqpProxyRequest(THolder<TRequest> event, const TString& database) const {
        return RunKqpProxyRequest<TRequest, TResponse>(std::move(event), GetNodeIndexForDatabase(database));
    }

    template <typename TRequest, typename TResponse>
    typename TResponse::TPtr RunKqpProxyRequest(THolder<TRequest> event, ui32 nodeIndex) const {
        NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor(nodeIndex);
        NActors::TActorId kqpProxy = NKikimr::NKqp::MakeKqpProxyID(GetRuntime()->GetNodeId(nodeIndex));

        GetRuntime()->Send(kqpProxy, edgeActor, event.Release(), nodeIndex);

        return GetRuntime()->GrabEdgeEvent<TResponse>(edgeActor);
    }

private:
    void FillQueryRequest(const TRequestOptions& query, NKikimrKqp::EQueryType type, ui32 targetNodeIndex, NKikimrKqp::TEvQueryRequest& event) {
        event.SetTraceId(query.TraceId);

        if (query.UserSID) {
            event.SetUserToken(NACLib::TUserToken(
                Settings_.YqlToken,
                query.UserSID,
                query.GroupSIDs ? *query.GroupSIDs : TVector<NACLib::TSID>{GetRuntime()->GetAppData(targetNodeIndex).AllAuthenticatedUsers}
            ).SerializeAsString());
        }

        const auto& database = GetDatabasePath(query.Database);
        auto request = event.MutableRequest();
        request->SetQuery(query.Query);
        request->SetType(type);
        request->SetAction(query.Action);
        request->SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_PROFILE);
        request->SetDatabase(database);
        request->SetPoolId(query.PoolId);
        request->MutableYdbParameters()->insert(query.Params.begin(), query.Params.end());
        request->MutableQueryCachePolicy()->set_keep_in_cache(IsIn({NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY}, type));

        if (query.Timeout) {
            request->SetTimeoutMs(query.Timeout.MilliSeconds());
        }

        if (Settings_.SameSession) {
            if (!SessionState_) {
                SessionState_ = TSessionState(GetRuntime(), targetNodeIndex, database, query.TraceId, Settings_.VerbosityLevel);
            }
            request->SetSessionId(SessionState_->GetSessionId());
        }
    }

    TQueryRequest GetQueryRequest(const TRequestOptions& query) {
        ui32 targetNodeIndex = GetNodeIndexForDatabase(query.Database);
        auto event = std::make_unique<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY, targetNodeIndex, event->Record);

        if (auto progressStatsPeriodMs = Settings_.AppConfig.GetQueryServiceConfig().GetProgressStatsPeriodMs()) {
            event->SetProgressStatsPeriod(TDuration::MilliSeconds(progressStatsPeriodMs));
        }

        return {
            .Event = std::move(event),
            .TargetNode = GetRuntime()->GetNodeId(targetNodeIndex),
            .ResultRowsLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultRowsLimit(),
            .ResultSizeLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultSizeLimit(),
            .QueryId = query.QueryId
        };
    }

    TString GetTenantPath(const TString& tenantName) const {
        return TStringBuilder() << NKikimr::CanonizePath(Settings_.DomainName) << NKikimr::CanonizePath(tenantName);
    }

    TString GetDatabasePath(const TString& database) const {
        return NKikimr::CanonizePath(database ? database : GetDefaultDatabase());
    }

    ui32 GetNodeIndexForDatabase(const TString& path) const {
        auto canonizedPath = NKikimr::CanonizePath(path ? path : GetDefaultDatabase());
        if (canonizedPath == NKikimr::CanonizePath(Settings_.DomainName)) {
            return RandomNumber(Settings_.NodeCount);
        }

        if (const auto it = ServerlessToShared_.find(canonizedPath); it != ServerlessToShared_.end()) {
            canonizedPath = it->second;
        }

        if (const ui32 tenantSize = Tenants_->Size(canonizedPath)) {
            return Tenants_->List(canonizedPath)[RandomNumber(tenantSize)];
        }

        ythrow yexception() << "Unknown tenant '" << canonizedPath << "'";
    }

    void UpdateStorageMeta() const {
        if (StorageMetaPath_) {
            TString storageMetaStr;
            google::protobuf::TextFormat::PrintToString(StorageMeta_, &storageMetaStr);

            TFileOutput storageMetaOutput(StorageMetaPath_.GetPath());
            storageMetaOutput.Write(storageMetaStr);
            storageMetaOutput.Finish();
        }
    }

private:
    TYdbSetupSettings Settings_;

    TKqprunServer::TPtr Server_;
    THolder<NKikimr::Tests::TClient> Client_;
    THolder<NKikimr::Tests::TTenants> Tenants_;

    std::unordered_map<TString, TString> ServerlessToShared_;
    std::optional<NActors::TActorId> AsyncQueryRunnerActorId_;
    std::optional<TSessionState> SessionState_;
    TFsPath StorageMetaPath_;
    NKqpRun::TStorageMeta StorageMeta_;
    THolder<TThreadPool> SignalHandlerPool_;
};


//// TYdbSetup

TYdbSetup::TYdbSetup(const TYdbSetupSettings& settings)
    : Impl_(new TImpl(settings))
{}

TRequestResult TYdbSetup::SchemeQueryRequest(const TRequestOptions& query, TSchemeMeta& meta) const {
    auto schemeQueryOperationResponse = Impl_->SchemeQueryRequest(query)->Get()->Record;
    const auto& responseRecord = schemeQueryOperationResponse.GetResponse();

    meta.Ast = responseRecord.GetQueryAst();

    return TRequestResult(schemeQueryOperationResponse.GetYdbStatus(), responseRecord.GetQueryIssues());
}

TRequestResult TYdbSetup::ScriptRequest(const TScriptRequest& script, TString& operation) const {
    auto scriptExecutionOperation = Impl_->ScriptRequest(script);

    operation = scriptExecutionOperation->Get()->OperationId;

    return TRequestResult(scriptExecutionOperation->Get()->Status, scriptExecutionOperation->Get()->Issues);
}

TRequestResult TYdbSetup::QueryRequest(const TRequestOptions& query, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets, TProgressCallback progressCallback) const {
    resultSets.clear();

    TQueryResponse queryResponse = Impl_->QueryRequest(query, progressCallback);
    const auto& queryOperationResponse = queryResponse.Response->Get()->Record;
    const auto& responseRecord = queryOperationResponse.GetResponse();

    resultSets = std::move(queryResponse.ResultSets);
    FillQueryMeta(meta, responseRecord);

    return TRequestResult(queryOperationResponse.GetYdbStatus(), responseRecord.GetQueryIssues());
}

TRequestResult TYdbSetup::QueryRequest(const TRequestOptions& query) const {
    TQueryMeta meta;
    std::vector<Ydb::ResultSet> resultSets;
    return QueryRequest(query, meta, resultSets, nullptr);
}

TRequestResult TYdbSetup::YqlScriptRequest(const TRequestOptions& query, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets) const {
    resultSets.clear();

    auto yqlQueryOperationResponse = Impl_->YqlScriptRequest(query)->Get()->Record;
    const auto& responseRecord = yqlQueryOperationResponse.GetResponse();

    FillQueryMeta(meta, responseRecord);

    resultSets.reserve(responseRecord.ydbresults_size());
    for (const auto& result : responseRecord.ydbresults()) {
        resultSets.emplace_back(result);
    }

    return TRequestResult(yqlQueryOperationResponse.GetYdbStatus(), responseRecord.GetQueryIssues());
}

TRequestResult TYdbSetup::GetScriptExecutionOperationRequest(const TString& database, const TString& operation, const TString& userSID, TExecutionMeta& meta) const {
    auto scriptExecutionOperation = Impl_->GetScriptExecutionOperationRequest(database, operation, userSID);

    meta.Ready = scriptExecutionOperation->Get()->Ready;

    auto serializedMeta = scriptExecutionOperation->Get()->Metadata;
    if (serializedMeta) {
        Ydb::Query::ExecuteScriptMetadata deserializedMeta;
        serializedMeta->UnpackTo(&deserializedMeta);

        meta.ExecutionStatus = static_cast<NYdb::NQuery::EExecStatus>(deserializedMeta.exec_status());
        meta.ResultSetsCount = deserializedMeta.result_sets_meta_size();
        meta.Ast = deserializedMeta.exec_stats().query_ast();
        if (deserializedMeta.exec_stats().query_plan() != "{}") {
            meta.Plan = deserializedMeta.exec_stats().query_plan();
        }
        meta.TotalDuration = TDuration::MicroSeconds(deserializedMeta.exec_stats().total_duration_us());
    }

    return TRequestResult(scriptExecutionOperation->Get()->Status, scriptExecutionOperation->Get()->Issues);
}

TRequestResult TYdbSetup::FetchScriptExecutionResultsRequest(const TString& database, const TString& operation, const TString& userSID, i32 resultSetId, Ydb::ResultSet& resultSet) const {
    auto scriptExecutionResults = Impl_->FetchScriptExecutionResultsRequest(database, operation, userSID, resultSetId);

    resultSet = scriptExecutionResults->Get()->ResultSet.value_or(Ydb::ResultSet());

    return TRequestResult(scriptExecutionResults->Get()->Status, scriptExecutionResults->Get()->Issues);
}

TRequestResult TYdbSetup::ForgetScriptExecutionOperationRequest(const TString& database, const TString& operation, const TString& userSID) const {
    auto forgetScriptExecutionOperationResponse = Impl_->ForgetScriptExecutionOperationRequest(database, operation, userSID);

    return TRequestResult(forgetScriptExecutionOperationResponse->Get()->Status, forgetScriptExecutionOperationResponse->Get()->Issues);
}

TRequestResult TYdbSetup::CancelScriptExecutionOperationRequest(const TString& database, const TString& operation, const TString& userSID) const {
    auto cancelScriptExecutionOperationResponse = Impl_->CancelScriptExecutionOperationRequest(database, operation, userSID);

    return TRequestResult(cancelScriptExecutionOperationResponse->Get()->Status, cancelScriptExecutionOperationResponse->Get()->Issues);
}

void TYdbSetup::QueryRequestAsync(const TRequestOptions& query) const {
    Impl_->QueryRequestAsync(query);
}

void TYdbSetup::WaitAsyncQueries() const {
    Impl_->WaitAsyncQueries();
}

void TYdbSetup::CloseSessions() const {
    Impl_->CloseSessions();
}

void TYdbSetup::StartTraceOpt() const {
    Impl_->StartTraceOpt();
}

void TYdbSetup::StopTraceOpt() {
    TYdbSetup::TImpl::StopTraceOpt();
}

TString TYdbSetup::GetDefaultDatabase() const {
    return Impl_->GetDefaultDatabase();
}

}  // namespace NKqpRun
