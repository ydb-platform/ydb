#include "fq_setup.h"
#include "actors.h"

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/testing/unittest/tests_data.h>

#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/init/init.h>
#include <ydb/core/fq/libs/mock/yql_mock.h>
#include <ydb/core/testlib/test_client.h>
#include <ydb/library/folder_service/mock/mock_folder_service_adapter.h>
#include <ydb/library/grpc/server/actors/logger.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/tests/tools/kqprun/runlib/kikimr_setup.h>

#include <yql/essentials/utils/log/log.h>

using namespace NKikimrRun;

namespace NFqRun {

namespace {

TRequestResult GetStatus(const NYql::TIssues& issues) {
    return TRequestResult(issues ? Ydb::StatusIds::BAD_REQUEST : Ydb::StatusIds::SUCCESS, issues);
}

}  // anonymous namespace

class TFqSetup::TImpl : public TKikimrSetupBase {
    using TBase = TKikimrSetupBase;
    using EVerbose = TFqSetupSettings::EVerbose;

private:
    NKikimr::Tests::TServerSettings GetServerSettings(ui32 grpcPort) {
        auto serverSettings = TBase::GetServerSettings(Settings, grpcPort, Settings.VerboseLevel >= EVerbose::InitLogs);

        serverSettings.SetEnableYqGrpc(true);

        return serverSettings;
    }

    void InitializeServer(ui32 grpcPort) {
        const auto& serverSettings = GetServerSettings(grpcPort);

        Server = MakeIntrusive<NKikimr::Tests::TServer>(serverSettings);
        Server->GetRuntime()->SetDispatchTimeout(TDuration::Max());

        Server->EnableGRpc(NYdbGrpc::TServerOptions()
            .SetHost("localhost")
            .SetPort(grpcPort)
            .SetLogger(NYdbGrpc::CreateActorSystemLogger(*GetRuntime()->GetActorSystem(0), NKikimrServices::GRPC_SERVER))
            .SetGRpcShutdownDeadline(TDuration::Zero())
        );

        Client = std::make_unique<NKikimr::Tests::TClient>(serverSettings);
        Client->InitRootScheme();
    }

    NFq::NConfig::TConfig GetFqProxyConfig(ui32 grpcPort) const {
        auto fqConfig = Settings.AppConfig.GetFederatedQueryConfig();

        fqConfig.MutableControlPlaneStorage()->AddSuperUsers(BUILTIN_ACL_ROOT);
        fqConfig.MutablePrivateProxy()->AddGrantedUsers(BUILTIN_ACL_ROOT);

        const TString endpoint = TStringBuilder() << "localhost:" << grpcPort;
        const TString database = NKikimr::CanonizePath(Settings.DomainName);
        const auto fillStorageConfig = [endpoint, database](NFq::NConfig::TYdbStorageConfig* config, std::optional<TExternalDatabase> externalDatabase = std::nullopt) {
            config->SetEndpoint(externalDatabase ? externalDatabase->Endpoint : endpoint);
            config->SetDatabase(externalDatabase ? externalDatabase->Database : database);
            if (externalDatabase) {
                config->SetToken(externalDatabase->Token);
            }
        };

        auto* privateApiConfig = fqConfig.MutablePrivateApi();
        privateApiConfig->SetTaskServiceEndpoint(endpoint);
        privateApiConfig->SetTaskServiceDatabase(database);

        auto* nodesMenagerConfig = fqConfig.MutableNodesManager();
        nodesMenagerConfig->SetPort(grpcPort);
        nodesMenagerConfig->SetHost("localhost");

        if (Settings.EmulateS3) {
            fqConfig.MutableCommon()->SetObjectStorageEndpoint("file://");
        }

        auto& cpStorage = *fqConfig.MutableControlPlaneStorage();
        cpStorage.SetUseInMemory(!Settings.EnableCpStorage);
        fillStorageConfig(cpStorage.MutableStorage(), Settings.CpStorageDatabase);
        fillStorageConfig(fqConfig.MutableDbPool()->MutableStorage(), Settings.CpStorageDatabase);

        auto& checkpoints = *fqConfig.MutableCheckpointCoordinator();
        checkpoints.SetEnabled(Settings.EnableCheckpoints);
        if (Settings.EnableCheckpoints) {
            fillStorageConfig(checkpoints.MutableStorage(), Settings.CheckpointsDatabase);
        }

        fqConfig.MutableQuotasManager()->SetEnabled(Settings.EnableQuotas);

        auto& rateLimiter = *fqConfig.MutableRateLimiter();
        rateLimiter.SetEnabled(Settings.EnableQuotas);
        rateLimiter.SetControlPlaneEnabled(Settings.EnableQuotas);
        rateLimiter.SetDataPlaneEnabled(Settings.EnableQuotas);
        if (Settings.EnableQuotas) {
            fillStorageConfig(rateLimiter.MutableDatabase(), Settings.RateLimiterDatabase);
        }

        auto& rowDispatcher = *fqConfig.MutableRowDispatcher()->MutableCoordinator();
        rowDispatcher.SetLocalMode(!Settings.EnableRemoteRd);
        if (Settings.EnableRemoteRd) {
            fillStorageConfig(rowDispatcher.MutableDatabase(), Settings.RowDispatcherDatabase);
        }

        auto& ydbCompute = *fqConfig.MutableCompute()->MutableYdb();
        ydbCompute.SetEnable(Settings.EnableYdbCompute);
        ydbCompute.MutableSynchronizationService()->SetEnable(Settings.EnableYdbCompute);

        auto& computeControlPlane = *ydbCompute.MutableControlPlane();
        computeControlPlane.SetEnable(Settings.EnableYdbCompute);

        if (Settings.SingleComputeDatabase && !Settings.SharedComputeDatabases.empty()) {
            ythrow yexception() << "Expected one of single compute database or shared compute databases";
        }
        for (size_t i = 0; const auto& database : Settings.SharedComputeDatabases) {
            auto& databaseConfig = *computeControlPlane.MutableCms()->MutableDatabaseMapping()->AddCommon();
            databaseConfig.SetId(TStringBuilder() << "sls" << (Settings.SharedComputeDatabases.size() > 1 ? TStringBuilder() << "_" << ++i : TStringBuilder()));
            databaseConfig.MutableExecutionConnection()->SetEndpoint(database.Endpoint);
            fillStorageConfig(databaseConfig.MutableControlPlaneConnection(), database);

            const auto path = NKikimr::CanonizePath(database.Database);
            const auto splitPos = path.rfind('/');
            if (splitPos == 0 || splitPos == TString::npos) {
                ythrow yexception() << "Unexpected shared compute database: " << database.Database << ", it should not be empty or root";
            }
            databaseConfig.SetTenant(path.substr(0, splitPos));
        }
        if (Settings.SingleComputeDatabase || (Settings.EnableYdbCompute && computeControlPlane.GetTypeCase() == NFq::NConfig::TYdbComputeControlPlane::TYPE_NOT_SET)) {
            auto& singleCompute = *computeControlPlane.MutableSingle();
            singleCompute.SetId("single");
            fillStorageConfig(singleCompute.MutableConnection(), Settings.SingleComputeDatabase);
        }

        return fqConfig;
    }

    void InitializeFqProxy(ui32 grpcPort) {
        const auto& fqConfig = GetFqProxyConfig(grpcPort);
        if (Settings.VerboseLevel >= EVerbose::InitLogs) {
            Cout << "FQ config:\n" << fqConfig.DebugString() << Endl;
        }

        const auto counters = GetRuntime()->GetAppData().Counters->GetSubgroup("counters", "yq");
        YqSharedResources = NFq::CreateYqSharedResources(fqConfig, NKikimr::CreateYdbCredentialsProviderFactory, counters);

        const auto actorRegistrator = [runtime = GetRuntime()](NActors::TActorId serviceActorId, NActors::IActor* actor) {
            auto actorId = runtime->Register(actor, 0, runtime->GetAppData().UserPoolId);
            runtime->RegisterService(serviceActorId, actorId);
        };

        const auto folderServiceFactory = [](auto& config) {
            return NKikimr::NFolderService::CreateMockFolderServiceAdapterActor(config, "");
        };

        NFq::Init(
            fqConfig, GetRuntime()->GetNodeId(), actorRegistrator, &GetRuntime()->GetAppData(),
            Settings.DomainName, nullptr, YqSharedResources, folderServiceFactory, 0, {}, Settings.PqGatewayFactory
        );
        YqSharedResources->Init(GetRuntime()->GetActorSystem(0));
    }

    void InitializeYqlLogger() {
        if (!Settings.EnableTraceOpt) {
            return;
        }

        ModifyLogPriorities({{NKikimrServices::EServiceKikimr::YQL_PROXY, NActors::NLog::PRI_TRACE}}, *Settings.AppConfig.MutableLogConfig());
        NYql::NLog::InitLogger(NActors::CreateNullBackend());
    }

public:
    explicit TImpl(const TFqSetupSettings& settings)
        : Settings(settings)
    {
        const ui32 grpcPort = Settings.FirstGrpcPort ? Settings.FirstGrpcPort : PortManager.GetPort();
        if (Settings.GrpcEnabled && Settings.VerboseLevel >= EVerbose::Info) {
            Cout << CoutColors.Cyan() << "Domain gRPC port: " << CoutColors.Default() << grpcPort << Endl;
        }

        Settings.GrpcEnabled = true;

        InitializeYqlLogger();
        InitializeServer(grpcPort);
        InitializeFqProxy(grpcPort);

        if (Settings.MonitoringEnabled && Settings.VerboseLevel >= EVerbose::Info) {
            Cout << CoutColors.Cyan() << "Monitoring port: " << CoutColors.Default() << GetRuntime()->GetMonPort() << Endl;
        }
    }

    ~TImpl() {
        if (YqSharedResources) {
            YqSharedResources->Stop();
        }
    }

    NFq::TEvControlPlaneProxy::TEvCreateQueryResponse::TPtr QueryRequest(const TRequestOptions& query) const {
        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvCreateQueryRequest, NFq::TEvControlPlaneProxy::TEvCreateQueryResponse>(
            GetQueryRequest(query), query.FqOptions
        );
    }

    NFq::TEvControlPlaneProxy::TEvDescribeQueryResponse::TPtr DescribeQuery(const TString& queryId, const TFqOptions& options) const {
        FederatedQuery::DescribeQueryRequest request;
        request.set_query_id(queryId);

        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvDescribeQueryRequest, NFq::TEvControlPlaneProxy::TEvDescribeQueryResponse>(request, options);
    }

    NFq::TEvControlPlaneProxy::TEvGetResultDataResponse::TPtr FetchQueryResults(const TString& queryId, i32 resultSetId, const TFqOptions& options) const {
        FederatedQuery::GetResultDataRequest request;
        request.set_query_id(queryId);
        request.set_result_set_index(resultSetId);
        request.set_limit(MAX_RESULT_SET_ROWS);

        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvGetResultDataRequest, NFq::TEvControlPlaneProxy::TEvGetResultDataResponse>(request, options);
    }

    NFq::TEvControlPlaneProxy::TEvCreateConnectionResponse::TPtr CreateConnection(const FederatedQuery::ConnectionContent& connection, const TFqOptions& options) const {
        FederatedQuery::CreateConnectionRequest request;
        *request.mutable_content() = connection;

        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvCreateConnectionRequest, NFq::TEvControlPlaneProxy::TEvCreateConnectionResponse>(request, options);
    }

    NFq::TEvControlPlaneProxy::TEvCreateBindingResponse::TPtr CreateBinding(const FederatedQuery::BindingContent& binding, const TFqOptions& options) const {
        FederatedQuery::CreateBindingRequest request;
        *request.mutable_content() = binding;

        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvCreateBindingRequest, NFq::TEvControlPlaneProxy::TEvCreateBindingResponse>(request, options);
    }

    void QueryRequestAsync(const TRequestOptions& query, TDuration pingPeriod) {
        if (!AsyncQueryRunnerActorId) {
            AsyncQueryRunnerActorId = GetRuntime()->Register(CreateAsyncQueryRunnerActor(Settings.AsyncQueriesSettings), 0, GetRuntime()->GetAppData().UserPoolId);
        }

        TQueryRequest request = {
            .Event = GetControlPlaneRequest<NFq::TEvControlPlaneProxy::TEvCreateQueryRequest>(GetQueryRequest(query), query.FqOptions),
            .PingPeriod = pingPeriod
        };
        auto startPromise = NThreading::NewPromise();
        GetRuntime()->Send(*AsyncQueryRunnerActorId, GetRuntime()->AllocateEdgeActor(), new NKikimrRun::TEvPrivate::TEvStartAsyncQuery(std::move(request), startPromise));

        return startPromise.GetFuture().GetValueSync();
    }

    void WaitAsyncQueries() const {
        if (!AsyncQueryRunnerActorId) {
            return;
        }

        auto finalizePromise = NThreading::NewPromise();
        GetRuntime()->Send(*AsyncQueryRunnerActorId, GetRuntime()->AllocateEdgeActor(), new NKikimrRun::TEvPrivate::TEvFinalizeAsyncQueryRunner(finalizePromise));

        return finalizePromise.GetFuture().GetValueSync();
    }

    void StartTraceOpt() const {
        if (!Settings.EnableTraceOpt) {
            ythrow yexception() << "Trace opt was disabled";
        }

        NYql::NLog::YqlLogger().ResetBackend(CreateLogBackend(Settings));
    }

    static void StopTraceOpt() {
        NYql::NLog::YqlLogger().ResetBackend(NActors::CreateNullBackend());
    }

private:
    NActors::TTestActorRuntime* GetRuntime() const {
        return Server->GetRuntime();
    }

    static FederatedQuery::CreateQueryRequest GetQueryRequest(const TRequestOptions& query) {
        FederatedQuery::CreateQueryRequest request;
        request.set_execute_mode(query.Action);

        auto& content = *request.mutable_content();
        content.set_type(query.Type);
        content.set_text(query.Query);
        SetupAcl(content.mutable_acl());

        return request;
    }

    template <typename TRequest, typename TProto>
    std::unique_ptr<TRequest> GetControlPlaneRequest(const TProto& request, const TFqOptions& options) const {
        return std::make_unique<TRequest>(TStringBuilder() << "yandexcloud://" << options.Scope, request, BUILTIN_ACL_ROOT, Settings.YqlToken ? Settings.YqlToken : "fqrun", TVector<TString>{});
    }

    template <typename TRequest, typename TResponse, typename TProto>
    typename TResponse::TPtr RunControlPlaneProxyRequest(const TProto& request, const TFqOptions& options) const {
        return RunControlPlaneProxyRequest<TRequest, TResponse>(GetControlPlaneRequest<TRequest>(request, options));
    }

    template <typename TRequest, typename TResponse>
    typename TResponse::TPtr RunControlPlaneProxyRequest(std::unique_ptr<TRequest> event) const {
        NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor();
        NActors::TActorId controlPlaneProxy = NFq::ControlPlaneProxyActorId();

        GetRuntime()->Send(controlPlaneProxy, edgeActor, event.release());

        return GetRuntime()->GrabEdgeEvent<TResponse>(edgeActor);
    }

private:
    TFqSetupSettings Settings;
    const NColorizer::TColors CoutColors;

    NKikimr::Tests::TServer::TPtr Server;
    std::unique_ptr<NKikimr::Tests::TClient> Client;
    NFq::IYqSharedResources::TPtr YqSharedResources;
    TPortManager PortManager;

    std::optional<NActors::TActorId> AsyncQueryRunnerActorId;
};

TFqSetup::TFqSetup(const TFqSetupSettings& settings)
    : Impl(new TImpl(settings))
{}

TRequestResult TFqSetup::QueryRequest(const TRequestOptions& query, TString& queryId) const {
    const auto response = Impl->QueryRequest(query);

    queryId = response->Get()->Result.query_id();

    return GetStatus(response->Get()->Issues);
}

TRequestResult TFqSetup::DescribeQuery(const TString& queryId, const TFqOptions& options, TExecutionMeta& meta) const {
    const auto response = Impl->DescribeQuery(queryId, options);

    const auto& result = response->Get()->Result.query();
    meta.Status = result.meta().status();
    NYql::IssuesFromMessage(result.issue(), meta.Issues);
    NYql::IssuesFromMessage(result.transient_issue(), meta.TransientIssues);

    meta.ResultSetSizes.clear();
    for (const auto& resultMeta : result.result_set_meta()) {
        meta.ResultSetSizes.emplace_back(resultMeta.rows_count());
    }

    meta.Ast = result.ast().data();
    meta.Plan = result.plan().json();
    meta.Statistics = result.statistics().json();

    return GetStatus(response->Get()->Issues);
}

TRequestResult TFqSetup::FetchQueryResults(const TString& queryId, i32 resultSetId, const TFqOptions& options, Ydb::ResultSet& resultSet) const {
    const auto response = Impl->FetchQueryResults(queryId, resultSetId, options);

    resultSet = response->Get()->Result.result_set();

    return GetStatus(response->Get()->Issues);
}

TRequestResult TFqSetup::CreateConnection(const FederatedQuery::ConnectionContent& connection, const TFqOptions& options, TString& connectionId) const {
    const auto response = Impl->CreateConnection(connection, options);

    connectionId = response->Get()->Result.connection_id();

    return GetStatus(response->Get()->Issues);
}

TRequestResult TFqSetup::CreateBinding(const FederatedQuery::BindingContent& binding, const TFqOptions& options) const {
    const auto response = Impl->CreateBinding(binding, options);
    return GetStatus(response->Get()->Issues);
}

void TFqSetup::QueryRequestAsync(const TRequestOptions& query, TDuration pingPeriod) const {
    Impl->QueryRequestAsync(query, pingPeriod);
}

void TFqSetup::WaitAsyncQueries() const {
    Impl->WaitAsyncQueries();
}

void TFqSetup::StartTraceOpt() const {
    Impl->StartTraceOpt();
}

void TFqSetup::StopTraceOpt() {
    TFqSetup::TImpl::StopTraceOpt();
}

}  // namespace NFqRun
