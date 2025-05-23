#include "ydb_setup.h"

#include <library/cpp/colorizer/colors.h>

#include <ydb/core/blob_depot/mon_main.h>
#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>
#include <ydb/core/testlib/basics/storage.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>

#include <ydb/tests/tools/kqprun/src/proto/storage_meta.pb.h>

#include <yql/essentials/utils/log/log.h>

using namespace NKikimrRun;

namespace NKqpRun {

namespace {

class TStaticCredentialsProvider : public NYdb::ICredentialsProvider {
public:
    TStaticCredentialsProvider(const TString& yqlToken)
        : YqlToken_(yqlToken)
    {}

    std::string GetAuthInfo() const override {
        return YqlToken_;
    }

    bool IsValid() const override {
        return true;
    }

private:
    std::string YqlToken_;
};

class TStaticCredentialsProviderFactory : public NYdb::ICredentialsProviderFactory {
public:
    TStaticCredentialsProviderFactory(const TString& yqlToken)
        : YqlToken_(yqlToken)
    {}

    std::shared_ptr<NYdb::ICredentialsProvider> CreateProvider() const override {
        return std::make_shared<TStaticCredentialsProvider>(YqlToken_);
    }

private:
    TString YqlToken_;
};

class TStaticSecuredCredentialsFactory : public NYql::ISecuredServiceAccountCredentialsFactory {
public:
    TStaticSecuredCredentialsFactory(const TString& yqlToken)
        : YqlToken_(yqlToken)
    {}

    std::shared_ptr<NYdb::ICredentialsProviderFactory> Create(const TString&, const TString&) override {
        return std::make_shared<TStaticCredentialsProviderFactory>(YqlToken_);
    }

private:
    TString YqlToken_;
};


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

class TYdbSetup::TImpl {
    using EVerbose = TYdbSetupSettings::EVerbose;
    using EHealthCheck = TYdbSetupSettings::EHealthCheck;

private:
    TAutoPtr<TLogBackend> CreateLogBackend() const {
        if (Settings_.LogOutputFile) {
            return NActors::CreateFileBackend(Settings_.LogOutputFile);
        } else {
            return NActors::CreateStderrBackend();
        }
    }

    void SetLoggerSettings(NKikimr::Tests::TServerSettings& serverSettings) const {
        auto loggerInitializer = [this](NActors::TTestActorRuntime& runtime) {
            InitLogSettings(Settings_.AppConfig.GetLogConfig(), runtime);
            runtime.SetLogBackendFactory([this]() { return CreateLogBackend(); });
        };

        serverSettings.SetLoggerInitializer(loggerInitializer);
    }

    void SetFunctionRegistry(NKikimr::Tests::TServerSettings& serverSettings) const {
        if (!Settings_.FunctionRegistry) {
            return;
        }

        auto functionRegistryFactory = [this](const NKikimr::NScheme::TTypeRegistry&) {
            return Settings_.FunctionRegistry.Get();
        };

        serverSettings.SetFrFactory(functionRegistryFactory);
    }

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
        serverSettings.SetStorageGeneration(StorageMeta_.GetStorageGeneration());
    }

    NKikimr::Tests::TServerSettings GetServerSettings(ui32 grpcPort) {
        const ui32 msgBusPort = PortManager_.GetPort();

        NKikimr::Tests::TServerSettings serverSettings(msgBusPort, Settings_.AppConfig.GetAuthConfig(), Settings_.AppConfig.GetPQConfig());
        serverSettings.SetNodeCount(Settings_.NodeCount);

        serverSettings.SetDomainName(Settings_.DomainName);
        serverSettings.SetAppConfig(Settings_.AppConfig);
        serverSettings.SetFeatureFlags(Settings_.AppConfig.GetFeatureFlags());
        serverSettings.SetControls(Settings_.AppConfig.GetImmediateControlsConfig());
        serverSettings.SetCompactionConfig(Settings_.AppConfig.GetCompactionConfig());
        serverSettings.PQClusterDiscoveryConfig = Settings_.AppConfig.GetPQClusterDiscoveryConfig();
        serverSettings.NetClassifierConfig = Settings_.AppConfig.GetNetClassifierConfig();

        const auto& kqpSettings = Settings_.AppConfig.GetKQPConfig().GetSettings();
        serverSettings.SetKqpSettings({kqpSettings.begin(), kqpSettings.end()});

        serverSettings.SetCredentialsFactory(std::make_shared<TStaticSecuredCredentialsFactory>(Settings_.YqlToken));
        serverSettings.SetComputationFactory(Settings_.ComputationFactory);
        serverSettings.SetYtGateway(Settings_.YtGateway);
        serverSettings.S3ActorsFactory = NYql::NDq::CreateS3ActorsFactory();
        serverSettings.SetInitializeFederatedQuerySetupFactory(true);
        serverSettings.SetVerbose(Settings_.VerboseLevel >= EVerbose::InitLogs);

        SetLoggerSettings(serverSettings);
        SetFunctionRegistry(serverSettings);
        SetStorageSettings(serverSettings);

        if (Settings_.MonitoringEnabled) {
            serverSettings.InitKikimrRunConfig();
            serverSettings.SetMonitoringPortOffset(Settings_.MonitoringPortOffset, true);
            serverSettings.SetNeedStatsCollectors(true);
        }

        if (Settings_.GrpcEnabled) {
            serverSettings.SetGrpcPort(grpcPort);
        }

        for (const auto& [tenantPath, tenantInfo] : StorageMeta_.GetTenants()) {
            Settings_.Tenants.emplace(tenantPath, tenantInfo);
        }

        ui32 dynNodesCount = 0;
        for (const auto& [tenantPath, tenantInfo] : Settings_.Tenants) {
            if (tenantInfo.GetType() != TStorageMeta::TTenant::SERVERLESS) {
                serverSettings.AddStoragePoolType(tenantPath);
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

        if (Settings_.MonitoringEnabled) {
            ui32 nodeIndex = GetNodeIndexForDatabase(absolutePath);
            NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor(nodeIndex);
            GetRuntime()->Register(NKikimr::CreateBoardPublishActor(NKikimr::MakeEndpointsBoardPath(absolutePath), "", edgeActor, 0, true), nodeIndex, GetRuntime()->GetAppData(nodeIndex).UserPoolId);
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

    void InitializeServer(ui32 grpcPort) {
        NKikimr::Tests::TServerSettings serverSettings = GetServerSettings(grpcPort);

        Server_ = MakeIntrusive<NKikimr::Tests::TServer>(serverSettings);

        StorageMeta_.SetStorageGeneration(StorageMeta_.GetStorageGeneration() + 1);
        UpdateStorageMeta();

        Server_->GetRuntime()->SetDispatchTimeout(TDuration::Max());

        if (Settings_.GrpcEnabled) {
            Server_->EnableGRpc(grpcPort);
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

    NThreading::TFuture<void> RunHealthCheck(const TString& database) const {
        EHealthCheck level = Settings_.HealthCheckLevel;
        i32 nodesCount = Settings_.NodeCount;
        if (database != Settings_.DomainName) {
            nodesCount = Tenants_->Size(database);
        } else if (StorageMeta_.TenantsSize() > 0) {
            level = std::min(level, EHealthCheck::NodesCount);
        }

        const TWaitResourcesSettings settings = {
            .ExpectedNodeCount = nodesCount,
            .HealthCheckLevel = level,
            .HealthCheckTimeout = Settings_.HealthCheckTimeout,
            .VerboseLevel = Settings_.VerboseLevel,
            .Database = NKikimr::CanonizePath(database)
        };
        const auto promise = NThreading::NewPromise();
        GetRuntime()->Register(CreateResourcesWaiterActor(promise, settings), GetNodeIndexForDatabase(database), GetRuntime()->GetAppData().SystemPoolId);

        return promise.GetFuture();
    }

    void WaitResourcesPublishing() const {
        std::vector<NThreading::TFuture<void>> futures(1, RunHealthCheck(Settings_.DomainName));
        for (const auto& [tenantName, _] : StorageMeta_.GetTenants()) {
            futures.emplace_back(RunHealthCheck(GetTenantPath(tenantName)));
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
        const ui32 grpcPort = Settings_.GrpcPort ? Settings_.GrpcPort : PortManager_.GetPort();

        InitializeYqlLogger();
        InitializeServer(grpcPort);
        WaitResourcesPublishing();

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
            Cout << CoutColors_.Cyan() << "Domain gRPC port: " << CoutColors_.Default() << grpcPort << Endl;
        }
    }

    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr SchemeQueryRequest(const TRequestOptions& query) {
        ui32 nodeIndex = GetNodeIndexForDatabase(query.Database);
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_DDL, nodeIndex, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvQueryRequest, NKikimr::NKqp::TEvKqp::TEvQueryResponse>(std::move(event), nodeIndex);
    }

    NKikimr::NKqp::TEvKqp::TEvScriptResponse::TPtr ScriptRequest(const TRequestOptions& script) {
        ui32 nodeIndex = GetNodeIndexForDatabase(script.Database);
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvScriptRequest>();
        FillQueryRequest(script, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT, nodeIndex, event->Record);

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
        GetRuntime()->Send(*AsyncQueryRunnerActorId_, GetRuntime()->AllocateEdgeActor(), new TEvPrivate::TEvStartAsyncQuery(std::move(request), startPromise));

        return startPromise.GetFuture().GetValueSync();
    }

    void WaitAsyncQueries() const {
        if (!AsyncQueryRunnerActorId_) {
            return;
        }

        auto finalizePromise = NThreading::NewPromise();
        GetRuntime()->Send(*AsyncQueryRunnerActorId_, GetRuntime()->AllocateEdgeActor(), new TEvPrivate::TEvFinalizeAsyncQueryRunner(finalizePromise));

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

        NYql::NLog::YqlLogger().ResetBackend(CreateLogBackend());
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
        event.SetUserToken(NACLib::TUserToken(Settings_.YqlToken, query.UserSID, {}).SerializeAsString());

        const auto& database = GetDatabasePath(query.Database);
        auto request = event.MutableRequest();
        request->SetQuery(query.Query);
        request->SetType(type);
        request->SetAction(query.Action);
        request->SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL);
        request->SetDatabase(database);
        request->SetPoolId(query.PoolId);

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
        const TString& result = NKikimr::CanonizePath(database ? database : GetDefaultDatabase());
        if (StorageMeta_.TenantsSize() > 0 && result == NKikimr::CanonizePath(Settings_.DomainName)) {
            ythrow yexception() << "Cannot use root domain '" << result << "' as request database then created additional tenants";
        }
        return result;
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
    TPortManager PortManager_;

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

TRequestResult TYdbSetup::ScriptRequest(const TRequestOptions& script, TString& operation) const {
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
