#include "actors.h"
#include "fq_setup.h"

#include <ydb/core/fq/libs/control_plane_proxy/events/events.h>
#include <ydb/core/fq/libs/init/init.h>
#include <ydb/core/fq/libs/mock/yql_mock.h>
#include <ydb/core/fq/libs/wasm_services/mock/tvm.h>
#include <ydb/core/node_whiteboard/node_whiteboard.h>
#include <ydb/core/testlib/test_client.h>

#include <ydb/library/actors/wilson/wilson_uploader.h>
#include <ydb/library/folder_service/mock/mock_folder_service_adapter.h>
#include <ydb/library/grpc/server/actors/logger.h>
#include <ydb/library/protobuf_printer/security_printer.h>
#include <ydb/library/security/ydb_credentials_provider_factory.h>
#include <ydb/tests/tools/kqprun/runlib/kikimr_setup.h>

#include <library/cpp/colorizer/colors.h>
#include <library/cpp/protobuf/interop/cast.h>
#include <library/cpp/protobuf/json/proto2json.h>
#include <library/cpp/testing/unittest/tests_data.h>
#include <util/stream/file.h>
#include <util/string/hex.h>
#include <yql/essentials/utils/log/log.h>

using namespace NKikimrRun;

namespace NFqRun {

namespace {

class TLocalTraceCollector: public NActors::TActorBootstrapped<TLocalTraceCollector> {
public:
    using TSpans = TVector<opentelemetry::proto::trace::v1::Span>;

    struct TEvSnapshot: NActors::TEventLocal<TEvSnapshot, NActors::TEvents::ES_PRIVATE * 65536 + 77> {
        NThreading::TPromise<TSpans> Promise;

        explicit TEvSnapshot(NThreading::TPromise<TSpans> promise)
            : Promise(std::move(promise))
        {
        }
    };

    void Bootstrap() {
        Become(&TLocalTraceCollector::StateWork);
    }

    void Handle(NWilson::TEvWilson::TPtr& ev) {
        const auto bytes = ev->Get()->Span.ByteSizeLong();
        if (Spans.size() >= 10000 || bytes > (16 << 20) - Bytes) {
            ++Dropped;
            return;
        }
        Bytes += bytes;
        Spans.push_back(std::move(ev->Get()->Span));
    }

    void Handle(TEvSnapshot::TPtr& ev) {
        if (Dropped) {
            Cerr << "Tracing: dropped " << Dropped << " spans at local capture limit" << Endl;
        }
        ev->Get()->Promise.SetValue(std::move(Spans));
        Bytes = 0;
    }

    STRICT_STFUNC(StateWork,
        hFunc(NWilson::TEvWilson, Handle);
        hFunc(TEvSnapshot, Handle);
    );

private:
    TSpans Spans;
    size_t Bytes = 0, Dropped = 0;
};

TRequestResult GetStatus(const NYql::TIssues& issues) {
    return TRequestResult(issues ? Ydb::StatusIds::BAD_REQUEST : Ydb::StatusIds::SUCCESS, issues);
}

}   // anonymous namespace

class TFqSetup::TImpl: public TKikimrSetupBase {
    using TBase = TKikimrSetupBase;
    using EVerbosity = TFqSetupSettings::EVerbosity;

private:
    NKikimr::Tests::TServerSettings GetServerSettings(ui32 grpcPort) {
        auto serverSettings = TBase::GetServerSettings(Settings, grpcPort, Settings.VerbosityLevel >= EVerbosity::InitLogs);

        serverSettings.SetEnableYqGrpc(true);

        return serverSettings;
    }

    void InitializeServer(ui32 grpcPort) {
        const auto& serverSettings = GetServerSettings(grpcPort);

        Server = MakeIntrusive<NKikimr::Tests::TServer>(serverSettings);
        Server->GetRuntime()->SetDispatchTimeout(TDuration::Max());

        Server->EnableGRpc(GetGrpcSettings(grpcPort, 0, TDuration::Zero()));

        Client = std::make_unique<NKikimr::Tests::TClient>(serverSettings);
        Client->InitRootScheme();
    }

    NFq::NConfig::TConfig GetFqProxyConfig(ui32 grpcPort) const {
        auto fqConfig = Settings.AppConfig.GetFederatedQueryConfig();
        if (Settings.TraceFile) {
            fqConfig.MutableWasmServices()->SetTraceSamplePerMillion(1000000);
        }

        fqConfig.MutableControlPlaneStorage()->AddSuperUsers(BUILTIN_ACL_ROOT);
        fqConfig.MutablePrivateProxy()->AddGrantedUsers(BUILTIN_ACL_ROOT);

        if (fqConfig.GetWasmServices().GetEnabled()) {
            fqConfig.MutableWasmServices()->SetModuleSource(NFq::NConfig::TWasmServicesConfig::LOCAL_FILES);
            auto* storage = fqConfig.MutableControlPlaneStorage();
            if (Find(storage->GetAvailableConnection(), "EXTERNAL_SERVICE") == storage->GetAvailableConnection().end()) {
                storage->AddAvailableConnection("EXTERNAL_SERVICE");
            }
        }

        const TString endpoint = TStringBuilder() << "localhost:" << grpcPort;
        const TString database = NKikimr::CanonizePath(Settings.DomainName);
        const auto fillStorageConfig = [endpoint, database](NFq::NConfig::TYdbStorageConfig* config,
                                           std::optional<TExternalDatabase> externalDatabase = std::nullopt) {
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
        // The in-memory implementation acknowledges ControlQuery without
        // applying it. Service calls need real cancellation via the pinger.
        cpStorage.SetUseInMemory(!Settings.EnableCpStorage && !fqConfig.GetWasmServices().GetEnabled());
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
            databaseConfig.SetId(
                TStringBuilder() << "sls" << (Settings.SharedComputeDatabases.size() > 1 ? TStringBuilder() << "_" << ++i : TStringBuilder()));
            databaseConfig.MutableExecutionConnection()->SetEndpoint(database.Endpoint);
            fillStorageConfig(databaseConfig.MutableControlPlaneConnection(), database);

            const auto path = NKikimr::CanonizePath(database.Database);
            const auto splitPos = path.rfind('/');
            if (splitPos == 0 || splitPos == TString::npos) {
                ythrow yexception() << "Unexpected shared compute database: " << database.Database << ", it should not be empty or root";
            }
            databaseConfig.SetTenant(path.substr(0, splitPos));
        }
        if (Settings.SingleComputeDatabase ||
            (Settings.EnableYdbCompute && computeControlPlane.GetTypeCase() == NFq::NConfig::TYdbComputeControlPlane::TYPE_NOT_SET)) {
            auto& singleCompute = *computeControlPlane.MutableSingle();
            singleCompute.SetId("single");
            fillStorageConfig(singleCompute.MutableConnection(), Settings.SingleComputeDatabase);
        }

        return fqConfig;
    }

    void InitializeFqProxy(ui32 grpcPort) {
        const auto& fqConfig = GetFqProxyConfig(grpcPort);
        if (Settings.VerbosityLevel >= EVerbosity::InitLogs) {
            Cout << "FQ config:\n" << NKikimr::SecureDebugStringMultiline(fqConfig) << Endl;
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

        NFq::Init(fqConfig, GetRuntime()->GetNodeId(), actorRegistrator, &GetRuntime()->GetAppData(), Settings.DomainName, nullptr,
            YqSharedResources, folderServiceFactory, 0, {}, Settings.PqGatewayFactory,
            Settings.MockTvm ? NFq::NWasmServices::CreateMockTvmTicketProvider(TDuration::MilliSeconds(Settings.MockTvmDelayMs)) : nullptr);
        YqSharedResources->Init(GetRuntime()->GetActorSystem(0));
    }

    void InitializeYqlLogger() {
        if (!Settings.EnableTraceOpt) {
            return;
        }

        ModifyLogPriorities(
            { { NKikimrServices::EServiceKikimr::YQL_PROXY, NActors::NLog::PRI_TRACE } }, *Settings.AppConfig.MutableLogConfig());
        NYql::NLog::InitLogger(NActors::CreateNullBackend());
    }

    void InitializeTenantNodes() {
        if (const auto& systemStateInfo = GetSystemStateInfo(Server->GetProcessMemoryInfoProvider())) {
            GetRuntime()->Send(NKikimr::NNodeWhiteboard::MakeNodeWhiteboardServiceId(GetRuntime()->GetFirstNodeId()),
                GetRuntime()->AllocateEdgeActor(), new NKikimr::NNodeWhiteboard::TEvWhiteboard::TEvSystemStateUpdate(*systemStateInfo));
        }
    }

public:
    explicit TImpl(const TFqSetupSettings& settings)
        : Settings(settings)
    {
        const ui32 grpcPort = Settings.FirstGrpcPort ? Settings.FirstGrpcPort : PortManager.GetPort();
        if (Settings.GrpcEnabled && Settings.VerbosityLevel >= EVerbosity::Info) {
            Cout << CoutColors.Cyan() << "Domain gRPC port: " << CoutColors.Default() << FormatGrpcLink(grpcPort) << Endl;
        }

        Settings.GrpcEnabled = true;

        InitializeYqlLogger();
        InitializeServer(grpcPort);
        if (Settings.TraceFile) {
            TraceCollector = GetRuntime()->Register(new TLocalTraceCollector);
            GetRuntime()->RegisterService(NWilson::MakeWilsonUploaderId(), TraceCollector);
        }
        InitializeFqProxy(grpcPort);
        InitializeTenantNodes();

        if (Settings.MonitoringEnabled && Settings.VerbosityLevel >= EVerbosity::Info) {
            Cout << CoutColors.Cyan() << "Monitoring port: " << CoutColors.Default()
                 << FormatMonitoringLink(GetRuntime()->GetMonPort(), "internal") << Endl;
        }
    }

    ~TImpl() {
        if (TraceCollector) {
            try {
                auto promise = NThreading::NewPromise<TLocalTraceCollector::TSpans>();
                GetRuntime()->GetActorSystem(0)->Send(TraceCollector, new TLocalTraceCollector::TEvSnapshot(promise));
                Y_ENSURE(promise.GetFuture().Wait(TDuration::Seconds(5)), "trace snapshot timed out");
                TFileOutput out(Settings.TraceFile);
                for (auto span : promise.GetFuture().GetValueSync()) {
                    span.set_trace_id(HexEncode(span.trace_id()));
                    span.set_span_id(HexEncode(span.span_id()));
                    span.set_parent_span_id(HexEncode(span.parent_span_id()));
                    if (span.name() == "SQL execution") {
                        for (const auto& attr : span.attributes()) {
                            if (attr.key() == "query_id") {
                                Cout << "Query " << attr.value().string_value() << " trace_id=" << span.trace_id() << Endl;
                            }
                        }
                    }
                    NProtobufJson::Proto2Json(span, out, NProtobufJson::TProto2JsonConfig());
                    out << '\n';
                }
            } catch (const std::exception& e) {
                Cerr << "Tracing export failed: " << e.what() << Endl;
            }
        }
        if (YqSharedResources) {
            YqSharedResources->Stop();
        }
    }

    NFq::TEvControlPlaneProxy::TEvCreateQueryResponse::TPtr QueryRequest(const TRequestOptions& query) const {
        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvCreateQueryRequest, NFq::TEvControlPlaneProxy::TEvCreateQueryResponse>(
            GetQueryRequest(query), query.FqOptions);
    }

    NFq::TEvControlPlaneProxy::TEvDescribeQueryResponse::TPtr DescribeQuery(const TString& queryId, const TFqOptions& options) const {
        FederatedQuery::DescribeQueryRequest request;
        request.set_query_id(queryId);

        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvDescribeQueryRequest,
            NFq::TEvControlPlaneProxy::TEvDescribeQueryResponse>(request, options);
    }

    NFq::TEvControlPlaneProxy::TEvControlQueryResponse::TPtr AbortQuery(const TString& queryId, const TFqOptions& options) const {
        FederatedQuery::ControlQueryRequest request;
        request.set_query_id(queryId);
        request.set_action(FederatedQuery::ABORT);
        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvControlQueryRequest,
            NFq::TEvControlPlaneProxy::TEvControlQueryResponse>(request, options);
    }

    NFq::TEvControlPlaneProxy::TEvGetResultDataResponse::TPtr FetchQueryResults(
        const TString& queryId, i32 resultSetId, const TFqOptions& options) const {
        FederatedQuery::GetResultDataRequest request;
        request.set_query_id(queryId);
        request.set_result_set_index(resultSetId);
        request.set_limit(MAX_RESULT_SET_ROWS);

        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvGetResultDataRequest,
            NFq::TEvControlPlaneProxy::TEvGetResultDataResponse>(request, options);
    }

    NFq::TEvControlPlaneProxy::TEvCreateConnectionResponse::TPtr CreateConnection(
        const FederatedQuery::ConnectionContent& connection, const TFqOptions& options) const {
        FederatedQuery::CreateConnectionRequest request;
        *request.mutable_content() = connection;

        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvCreateConnectionRequest,
            NFq::TEvControlPlaneProxy::TEvCreateConnectionResponse>(request, options);
    }

    NFq::TEvControlPlaneProxy::TEvListConnectionsResponse::TPtr ListConnections(
        const TString& name, const TString& pageToken, const TFqOptions& options) const {
        FederatedQuery::ListConnectionsRequest request;
        request.set_limit(100);
        request.set_page_token(pageToken);
        request.mutable_filter()->set_name(name);
        request.mutable_filter()->set_created_by_me(true);
        request.mutable_filter()->set_visibility(FederatedQuery::Acl::PRIVATE);
        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvListConnectionsRequest,
            NFq::TEvControlPlaneProxy::TEvListConnectionsResponse>(request, options);
    }

    NFq::TEvControlPlaneProxy::TEvCreateBindingResponse::TPtr CreateBinding(
        const FederatedQuery::BindingContent& binding, const TFqOptions& options) const {
        FederatedQuery::CreateBindingRequest request;
        *request.mutable_content() = binding;

        return RunControlPlaneProxyRequest<NFq::TEvControlPlaneProxy::TEvCreateBindingRequest,
            NFq::TEvControlPlaneProxy::TEvCreateBindingResponse>(request, options);
    }

    void QueryRequestAsync(const TRequestOptions& query, TDuration pingPeriod) {
        if (!AsyncQueryRunnerActorId) {
            AsyncQueryRunnerActorId =
                GetRuntime()->Register(CreateAsyncQueryRunnerActor(Settings.AsyncQueriesSettings), 0, GetRuntime()->GetAppData().UserPoolId);
        }

        TQueryRequest request = { .Event = GetControlPlaneRequest<NFq::TEvControlPlaneProxy::TEvCreateQueryRequest>(
                                      GetQueryRequest(query), query.FqOptions),
            .PingPeriod = pingPeriod };
        auto startPromise = NThreading::NewPromise();
        GetRuntime()->Send(*AsyncQueryRunnerActorId, GetRuntime()->AllocateEdgeActor(),
            new NKikimrRun::TEvPrivate::TEvStartAsyncQuery(std::move(request), startPromise));

        return startPromise.GetFuture().GetValueSync();
    }

    void WaitAsyncQueries() const {
        if (!AsyncQueryRunnerActorId) {
            return;
        }

        auto finalizePromise = NThreading::NewPromise();
        GetRuntime()->Send(*AsyncQueryRunnerActorId, GetRuntime()->AllocateEdgeActor(),
            new NKikimrRun::TEvPrivate::TEvFinalizeAsyncQueryRunner(finalizePromise));

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
    NActors::TTestActorRuntime* GetRuntime() const override {
        return Server->GetRuntime();
    }

    static FederatedQuery::CreateQueryRequest GetQueryRequest(const TRequestOptions& query) {
        FederatedQuery::CreateQueryRequest request;
        request.set_execute_mode(query.Action);

        auto& content = *request.mutable_content();
        content.set_type(query.Type);
        content.set_text(query.Query);
        SetupAcl(content.mutable_acl());

        if (const auto timeout = query.Timeout) {
            *content.mutable_limits()->mutable_execution_deadline() = NProtoInterop::CastToProto(TInstant::Now() + timeout);
        }

        return request;
    }

    template <typename TRequest, typename TProto>
    std::unique_ptr<TRequest> GetControlPlaneRequest(const TProto& request, const TFqOptions& options) const {
        return std::make_unique<TRequest>(TStringBuilder() << "yandexcloud://" << options.Scope, request, BUILTIN_ACL_ROOT,
            Settings.YqlToken ? Settings.YqlToken : "fqrun", TVector<TString>{});
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

        auto response = GetRuntime()->GrabEdgeEvent<TResponse>(edgeActor, IsInterrupted() ? CancellationTimeout() : TDuration::Seconds(30));
        Y_ENSURE(response, "Timed out waiting for FQ control plane response");
        return response;
    }

private:
    TFqSetupSettings Settings;
    const NColorizer::TColors CoutColors;

    NKikimr::Tests::TServer::TPtr Server;
    std::unique_ptr<NKikimr::Tests::TClient> Client;
    NFq::IYqSharedResources::TPtr YqSharedResources;
    TPortManager PortManager;

    NActors::TActorId TraceCollector;
    std::optional<NActors::TActorId> AsyncQueryRunnerActorId;
};

TFqSetup::TFqSetup(const TFqSetupSettings& settings)
    : Impl(new TImpl(settings))
{
}

TRequestResult TFqSetup::AbortQuery(const TString& queryId, const TFqOptions& options) const {
    return GetStatus(Impl->AbortQuery(queryId, options)->Get()->Issues);
}

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

TRequestResult TFqSetup::CreateConnection(
    const FederatedQuery::ConnectionContent& connection, const TFqOptions& options, TString& connectionId) const {
    const auto response = Impl->CreateConnection(connection, options);

    connectionId = response->Get()->Result.connection_id();

    return GetStatus(response->Get()->Issues);
}

TRequestResult TFqSetup::FindConnection(const TString& name, const TFqOptions& options, TString& connectionId) const {
    connectionId.clear();
    TString pageToken;
    do {
        const auto response = Impl->ListConnections(name, pageToken, options);
        const auto status = GetStatus(response->Get()->Issues);
        if (!status.IsSuccess()) {
            return status;
        }
        for (const auto& connection : response->Get()->Result.connection()) {
            if (connection.content().name() == name) {
                Y_ENSURE(connection.content().setting().has_external_service(), "Existing connection is not an ExternalService: " << name);
                connectionId = connection.meta().id();
                return status;
            }
        }
        pageToken = response->Get()->Result.next_page_token();
    } while (!pageToken.empty());
    return GetStatus({});
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

}   // namespace NFqRun
