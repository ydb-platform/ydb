#include "ydb_setup.h"

#include <library/cpp/colorizer/colors.h>

#include <ydb/core/blob_depot/mon_main.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/testlib/basics/storage.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/tests/tools/kqprun/runlib/kikimr_setup.h>
#include <ydb/tests/tools/kqprun/src/proto/storage_meta.pb.h>

#include <yql/essentials/utils/log/log.h>

using namespace NKikimrRun;

namespace NKqpRun {

namespace {

class TSessionState {
public:
    explicit TSessionState(NActors::TTestActorRuntime* runtime, ui32 targetNodeIndex, const TString& database, const TString& traceId, TYdbSetupSettings::EVerbose verboseLevel)
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
            .VerboseLevel = verboseLevel
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
    using EVerbose = TYdbSetupSettings::EVerbose;
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
            if (Settings_.VerboseLevel >= EVerbose::InitLogs) {
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
            .DiskPath = storagePath
        };

        serverSettings.SetEnableMockOnSingleNode(!Settings_.DisableDiskMock && !Settings_.PDisksPath);
        serverSettings.SetCustomDiskParams(storage);

        const auto storageGeneration = StorageMeta_.GetStorageGeneration();
        serverSettings.SetStorageGeneration(storageGeneration, storageGeneration > 0);
    }

    NKikimr::Tests::TServerSettings GetServerSettings(ui32 grpcPort) {
        auto serverSettings = TBase::GetServerSettings(Settings_, grpcPort, Settings_.VerboseLevel >= EVerbose::InitLogs);
        serverSettings
            .SetDataCenterCount(Settings_.DcCount)
            .SetPqGateway(Settings_.PqGateway);

        SetStorageSettings(serverSettings);

        for (const auto& [tenantPath, tenantInfo] : StorageMeta_.GetTenants()) {
            Settings_.Tenants.emplace(tenantPath, tenantInfo);
        }

        ui32 dynNodesCount = 0;
        for (const auto& [tenantPath, tenantInfo] : Settings_.Tenants) {
            if (tenantInfo.GetType() != TStorageMeta::TTenant::SERVERLESS) {
                serverSettings.AddStoragePool(tenantPath, TStringBuilder() << GetTenantPath(tenantPath) << ":" << tenantPath);
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
            if (Settings_.VerboseLevel >= EVerbose::Info) {
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
            if (it->second.GetNodesCount() != tenantInfo.GetNodesCount()) {
                it->second.SetNodesCount(tenantInfo.GetNodesCount());
                UpdateStorageMeta();
            }
            if (Settings_.VerboseLevel >= EVerbose::Info) {
                Cout << CoutColors_.Yellow() << TInstant::Now().ToIsoStringLocal() << " Starting " << type << " tenant " << absolutePath << "..." << CoutColors_.Default() << Endl;
            }
            if (!request.has_serverless_resources()) {
                Tenants_->Run(absolutePath, tenantInfo.GetNodesCount());
            }
        }
    }

    static void AddTenantStoragePool(Ydb::Cms::StorageUnits* storage, const TString& name) {
        storage->set_unit_kind(name);
        storage->set_count(1);
    }

    void CreateTenants() {
        std::set<TString> sharedTenants;
        std::map<TString, TStorageMeta::TTenant> serverlessTenants;
        for (const auto& [tenantPath, tenantInfo] : Settings_.Tenants) {
            Ydb::Cms::CreateDatabaseRequest request;
            request.set_path(GetTenantPath(tenantPath));

            switch (tenantInfo.GetType()) {
                case TStorageMeta::TTenant::DEDICATED:
                    AddTenantStoragePool(request.mutable_resources()->add_storage_units(), tenantPath);
                    CreateTenant(std::move(request), tenantPath, "dedicated", tenantInfo);
                    break;

                case TStorageMeta::TTenant::SHARED:
                    sharedTenants.emplace(tenantPath);
                    AddTenantStoragePool(request.mutable_shared_resources()->add_storage_units(), tenantPath);
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

        Server_ = MakeIntrusive<NKikimr::Tests::TServer>(serverSettings);

        StorageMeta_.SetStorageGeneration(StorageMeta_.GetStorageGeneration() + 1);
        UpdateStorageMeta();

        Server_->GetRuntime()->SetDispatchTimeout(TDuration::Max());

        if (Settings_.GrpcEnabled) {
            Server_->EnableGRpc(domainGrpcPort);
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
            .VerboseLevel = Settings_.VerboseLevel,
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
                    Server_->EnableGRpc(grpcPortGen.GetPort(), node, absolutePath);
                }
            } else if (Settings_.MonitoringEnabled) {
                NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor(node);
                GetRuntime()->Register(NKikimr::CreateBoardPublishActor(NKikimr::MakeEndpointsBoardPath(absolutePath), "", edgeActor, 0, true), node, GetRuntime()->GetAppData(node).UserPoolId);
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
        std::optional<NKikimrWhiteboard::TSystemStateInfo> systemStateInfo;
        if (const auto memoryInfoProvider = Server_->GetProcessMemoryInfoProvider()) {
            systemStateInfo = NKikimrWhiteboard::TSystemStateInfo();

            const auto& memInfo = memoryInfoProvider->Get();
            if (memInfo.CGroupLimit) {
                systemStateInfo->SetMemoryLimit(*memInfo.CGroupLimit);
            } else if (memInfo.MemTotal) {
                systemStateInfo->SetMemoryLimit(*memInfo.MemTotal);
            }
        }

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
        , CoutColors_(NColorizer::AutoColors(Cout))
    {
        TPortGenerator grpcPortGen(PortManager, Settings_.FirstGrpcPort);
        InitializeYqlLogger();
        InitializeServer(grpcPortGen);
        InitializeTenants(grpcPortGen);

        if (Settings_.MonitoringEnabled && Settings_.VerboseLevel >= EVerbose::Info) {
            for (ui32 nodeIndex = 0; nodeIndex < Settings_.NodeCount; ++nodeIndex) {
                Cout << CoutColors_.Cyan() << "Monitoring port" << (Server_->StaticNodes() + Server_->DynamicNodes() > 1 ? TStringBuilder() << " for static node " << nodeIndex + 1 : TString()) << ": " << CoutColors_.Default() << Server_->GetRuntime()->GetMonPort(nodeIndex) << Endl;
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

        if (Settings_.GrpcEnabled && Settings_.VerboseLevel >= EVerbose::Info) {
            Cout << CoutColors_.Cyan() << "Domain gRPC port: " << CoutColors_.Default() << Server_->GetGRpcServer().GetPort() << Endl;
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

    NKikimr::NKqp::TEvGetScriptExecutionOperationResponse::TPtr GetScriptExecutionOperationRequest(const TString& database, const TString& operation) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvGetScriptExecutionOperation>(GetDatabasePath(database), operationId);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvGetScriptExecutionOperation, NKikimr::NKqp::TEvGetScriptExecutionOperationResponse>(std::move(event), database);
    }

    NKikimr::NKqp::TEvFetchScriptResultsResponse::TPtr FetchScriptExecutionResultsRequest(const TString& database, const TString& operation, i32 resultSetId) const {
        TString error;
        const auto executionId = NKikimr::NKqp::ScriptExecutionIdFromOperation(operation, error);
        Y_ENSURE(executionId, error);

        ui32 nodeIndex = GetNodeIndexForDatabase(database);
        NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor(nodeIndex);
        auto rowsLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultRowsLimit();
        auto sizeLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultSizeLimit();
        NActors::IActor* fetchActor = NKikimr::NKqp::CreateGetScriptExecutionResultActor(edgeActor, GetDatabasePath(database), *executionId, resultSetId, 0, rowsLimit, sizeLimit, TInstant::Max());

        GetRuntime()->Register(fetchActor, nodeIndex, GetRuntime()->GetAppData(nodeIndex).UserPoolId);

        return GetRuntime()->GrabEdgeEvent<NKikimr::NKqp::TEvFetchScriptResultsResponse>(edgeActor);
    }

    NKikimr::NKqp::TEvForgetScriptExecutionOperationResponse::TPtr ForgetScriptExecutionOperationRequest(const TString& database, const TString& operation) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvForgetScriptExecutionOperation>(GetDatabasePath(database), operationId);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvForgetScriptExecutionOperation, NKikimr::NKqp::TEvForgetScriptExecutionOperationResponse>(std::move(event), database);
    }

    NKikimr::NKqp::TEvCancelScriptExecutionOperationResponse::TPtr CancelScriptExecutionOperationRequest(const TString& database, const TString& operation) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvCancelScriptExecutionOperation>(GetDatabasePath(database), operationId);

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

        NYql::NLog::YqlLogger().ResetBackend(CreateLogBackend(Settings_));
    }

    static void StopTraceOpt() {
        NYql::NLog::YqlLogger().ResetBackend(NActors::CreateNullBackend());
    }

private:
    NActors::TTestActorRuntime* GetRuntime() const {
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
        event.SetUserToken(NACLib::TUserToken(
            Settings_.YqlToken,
            query.UserSID,
            query.GroupSIDs ? *query.GroupSIDs : TVector<NACLib::TSID>{GetRuntime()->GetAppData(targetNodeIndex).AllAuthenticatedUsers}
        ).SerializeAsString());

        const auto& database = GetDatabasePath(query.Database);
        auto request = event.MutableRequest();
        request->SetQuery(query.Query);
        request->SetType(type);
        request->SetAction(query.Action);
        request->SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL);
        request->SetDatabase(database);
        request->SetPoolId(query.PoolId);
        request->MutableYdbParameters()->insert(query.Params.begin(), query.Params.end());

        if (query.Timeout) {
            request->SetTimeoutMs(query.Timeout.MilliSeconds());
        }

        if (Settings_.SameSession) {
            if (!SessionState_) {
                SessionState_ = TSessionState(GetRuntime(), targetNodeIndex, database, query.TraceId, Settings_.VerboseLevel);
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

    TString GetDefaultDatabase() const {
        if (StorageMeta_.TenantsSize() > 1) {
            ythrow yexception() << "Can not choose default database, there is more than one tenants, please use `-D <database name>`";
        }
        if (StorageMeta_.TenantsSize() == 1) {
            return GetTenantPath(StorageMeta_.GetTenants().begin()->first);
        }
        return Settings_.DomainName;
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
    NColorizer::TColors CoutColors_;

    NKikimr::Tests::TServer::TPtr Server_;
    THolder<NKikimr::Tests::TClient> Client_;
    THolder<NKikimr::Tests::TTenants> Tenants_;

    std::unordered_map<TString, TString> ServerlessToShared_;
    std::optional<NActors::TActorId> AsyncQueryRunnerActorId_;
    std::optional<TSessionState> SessionState_;
    TFsPath StorageMetaPath_;
    NKqpRun::TStorageMeta StorageMeta_;
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

TRequestResult TYdbSetup::GetScriptExecutionOperationRequest(const TString& database, const TString& operation, TExecutionMeta& meta) const {
    auto scriptExecutionOperation = Impl_->GetScriptExecutionOperationRequest(database, operation);

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

TRequestResult TYdbSetup::FetchScriptExecutionResultsRequest(const TString& database, const TString& operation, i32 resultSetId, Ydb::ResultSet& resultSet) const {
    auto scriptExecutionResults = Impl_->FetchScriptExecutionResultsRequest(database, operation, resultSetId);

    resultSet = scriptExecutionResults->Get()->ResultSet.value_or(Ydb::ResultSet());

    return TRequestResult(scriptExecutionResults->Get()->Status, scriptExecutionResults->Get()->Issues);
}

TRequestResult TYdbSetup::ForgetScriptExecutionOperationRequest(const TString& database, const TString& operation) const {
    auto forgetScriptExecutionOperationResponse = Impl_->ForgetScriptExecutionOperationRequest(database, operation);

    return TRequestResult(forgetScriptExecutionOperationResponse->Get()->Status, forgetScriptExecutionOperationResponse->Get()->Issues);
}

TRequestResult TYdbSetup::CancelScriptExecutionOperationRequest(const TString& database, const TString& operation) const {
    auto cancelScriptExecutionOperationResponse = Impl_->CancelScriptExecutionOperationRequest(database, operation);

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

}  // namespace NKqpRun
