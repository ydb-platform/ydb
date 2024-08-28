#include "ydb_setup.h"

#include <library/cpp/colorizer/colors.h>

#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>

#include <ydb/core/testlib/test_client.h>

#include <ydb/library/yql/providers/s3/actors/yql_s3_actors_factory_impl.h>
#include <ydb/library/yql/utils/log/log.h>

namespace NKqpRun {

namespace {

class TStaticCredentialsProvider : public NYdb::ICredentialsProvider {
public:
    TStaticCredentialsProvider(const TString& yqlToken)
        : YqlToken_(yqlToken)
    {}

    TString GetAuthInfo() const override {
        return YqlToken_;
    }

    bool IsValid() const override {
        return true;
    }

private:
    TString YqlToken_;
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
            if (Settings_.AppConfig.GetLogConfig().HasDefaultLevel()) {
                auto priority = NActors::NLog::EPriority(Settings_.AppConfig.GetLogConfig().GetDefaultLevel());
                auto descriptor = NKikimrServices::EServiceKikimr_descriptor();
                for (int i = 0; i < descriptor->value_count(); ++i) {
                    runtime.SetLogPriority(static_cast<NKikimrServices::EServiceKikimr>(descriptor->value(i)->number()), priority);
                }
            }

            for (auto setting : Settings_.AppConfig.GetLogConfig().get_arr_entry()) {
                NKikimrServices::EServiceKikimr service;
                if (!NKikimrServices::EServiceKikimr_Parse(setting.GetComponent(), &service)) {
                    ythrow yexception() << "Invalid kikimr service name " << setting.GetComponent();
                }

                runtime.SetLogPriority(service, NActors::NLog::EPriority(setting.GetLevel()));
            }

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
        serverSettings.SetVerbose(false);

        SetLoggerSettings(serverSettings);
        SetFunctionRegistry(serverSettings);

        if (Settings_.MonitoringEnabled) {
            serverSettings.InitKikimrRunConfig();
            serverSettings.SetMonitoringPortOffset(Settings_.MonitoringPortOffset);
            serverSettings.SetNeedStatsCollectors(true);
        }

        if (Settings_.GrpcEnabled) {
            serverSettings.SetGrpcPort(grpcPort);
        }

        return serverSettings;
    }

    void InitializeServer(ui32 grpcPort) {
        NKikimr::Tests::TServerSettings serverSettings = GetServerSettings(grpcPort);

        Server_ = MakeHolder<NKikimr::Tests::TServer>(serverSettings);
        Server_->GetRuntime()->SetDispatchTimeout(TDuration::Max());

        if (Settings_.GrpcEnabled) {
            Server_->EnableGRpc(grpcPort);
        }

        Client_ = MakeHolder<NKikimr::Tests::TClient>(serverSettings);
        Client_->InitRootScheme();
    }

    void InitializeYqlLogger() {
        if (!Settings_.TraceOptEnabled) {
            return;
        }

        bool found = false;
        for (auto& entry : *Settings_.AppConfig.MutableLogConfig()->MutableEntry()) {
            if (entry.GetComponent() == "KQP_YQL") {
                entry.SetLevel(NActors::NLog::PRI_TRACE);
                found = true;
                break;
            }
        }

        if (!found) {
            auto entry = Settings_.AppConfig.MutableLogConfig()->AddEntry();
            entry->SetComponent("KQP_YQL");
            entry->SetLevel(NActors::NLog::PRI_TRACE);
        }

        NYql::NLog::InitLogger(NActors::CreateNullBackend());
    }

    void WaitResourcesPublishing() const {
        auto promise = NThreading::NewPromise();
        GetRuntime()->Register(CreateResourcesWaiterActor(promise, Settings_.NodeCount), 0, GetRuntime()->GetAppData().SystemPoolId);

        try {
            promise.GetFuture().GetValue(Settings_.InitializationTimeout);
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

        if (Settings_.MonitoringEnabled) {
            for (ui32 nodeIndex = 0; nodeIndex < Settings_.NodeCount; ++nodeIndex) {
                Cout << CoutColors_.Cyan() << "Monitoring port" << (Settings_.NodeCount > 1 ? TStringBuilder() << " for node " << nodeIndex + 1 : TString()) << ": " << CoutColors_.Default() << Server_->GetRuntime()->GetMonPort(nodeIndex) << Endl;
            }
        }

        if (Settings_.GrpcEnabled) {
            Cout << CoutColors_.Cyan() << "gRPC port: " << CoutColors_.Default() << grpcPort << Endl;
        }
    }

    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr SchemeQueryRequest(const TRequestOptions& query) const {
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_DDL, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvQueryRequest, NKikimr::NKqp::TEvKqp::TEvQueryResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvKqp::TEvScriptResponse::TPtr ScriptRequest(const TRequestOptions& script) const {
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvScriptRequest>();
        FillScriptRequest(script, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvScriptRequest, NKikimr::NKqp::TEvKqp::TEvScriptResponse>(std::move(event));
    }

    TQueryResponse QueryRequest(const TRequestOptions& query, TProgressCallback progressCallback) const {
        auto request = GetQueryRequest(query);
        auto promise = NThreading::NewPromise<TQueryResponse>();
        GetRuntime()->Register(CreateRunScriptActorMock(std::move(request), promise, progressCallback), 0, GetRuntime()->GetAppData().UserPoolId);

        return promise.GetFuture().GetValueSync();
    }

    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr YqlScriptRequest(const TRequestOptions& query) const {
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_SCRIPT, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvQueryRequest, NKikimr::NKqp::TEvKqp::TEvQueryResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvGetScriptExecutionOperationResponse::TPtr GetScriptExecutionOperationRequest(const TString& operation) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvGetScriptExecutionOperation>(Settings_.DomainName, operationId);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvGetScriptExecutionOperation, NKikimr::NKqp::TEvGetScriptExecutionOperationResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvFetchScriptResultsResponse::TPtr FetchScriptExecutionResultsRequest(const TString& operation, i32 resultSetId) const {
        TString executionId = *NKikimr::NKqp::ScriptExecutionIdFromOperation(operation);

        ui32 nodeIndex = RandomNumber(Settings_.NodeCount);
        NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor(nodeIndex);
        auto rowsLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultRowsLimit();
        auto sizeLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultSizeLimit();
        NActors::IActor* fetchActor = NKikimr::NKqp::CreateGetScriptExecutionResultActor(edgeActor, Settings_.DomainName, executionId, resultSetId, 0, rowsLimit, sizeLimit, TInstant::Max());

        GetRuntime()->Register(fetchActor, nodeIndex, GetRuntime()->GetAppData(nodeIndex).UserPoolId);

        return GetRuntime()->GrabEdgeEvent<NKikimr::NKqp::TEvFetchScriptResultsResponse>(edgeActor);
    }

    NKikimr::NKqp::TEvForgetScriptExecutionOperationResponse::TPtr ForgetScriptExecutionOperationRequest(const TString& operation) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvForgetScriptExecutionOperation>(Settings_.DomainName, operationId);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvForgetScriptExecutionOperation, NKikimr::NKqp::TEvForgetScriptExecutionOperationResponse>(std::move(event));
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
    typename TResponse::TPtr RunKqpProxyRequest(THolder<TRequest> event) const {
        ui32 nodeIndex = RandomNumber(Settings_.NodeCount);
        NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor(nodeIndex);
        NActors::TActorId kqpProxy = NKikimr::NKqp::MakeKqpProxyID(GetRuntime()->GetNodeId(nodeIndex));

        GetRuntime()->Send(kqpProxy, edgeActor, event.Release(), nodeIndex);

        return GetRuntime()->GrabEdgeEvent<TResponse>(edgeActor);
    }

private:
    void FillQueryRequest(const TRequestOptions& query, NKikimrKqp::EQueryType type, NKikimrKqp::TEvQueryRequest& event) const {
        event.SetTraceId(query.TraceId);
        event.SetUserToken(NACLib::TUserToken(Settings_.YqlToken, query.UserSID, {}).SerializeAsString());

        auto request = event.MutableRequest();
        request->SetQuery(query.Query);
        request->SetType(type);
        request->SetAction(query.Action);
        request->SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL);
        request->SetDatabase(Settings_.DomainName);
        request->SetPoolId(query.PoolId);
    }

    void FillScriptRequest(const TRequestOptions& script, NKikimrKqp::TEvQueryRequest& event) const {
        FillQueryRequest(script, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT, event);

        auto request = event.MutableRequest();
        if (script.Action == NKikimrKqp::QUERY_ACTION_EXECUTE) {
            request->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
            request->MutableTxControl()->set_commit_tx(true);
        }
    }

    TQueryRequest GetQueryRequest(const TRequestOptions& query) const {
        auto event = std::make_unique<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY, event->Record);

        if (auto progressStatsPeriodMs = Settings_.AppConfig.GetQueryServiceConfig().GetProgressStatsPeriodMs()) {
            event->SetProgressStatsPeriod(TDuration::MilliSeconds(progressStatsPeriodMs));
        }

        return {
            .Event = std::move(event),
            .TargetNode = GetRuntime()->GetNodeId(RandomNumber(Settings_.NodeCount)),
            .ResultRowsLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultRowsLimit(),
            .ResultSizeLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultSizeLimit()
        };
    }

private:
    TYdbSetupSettings Settings_;
    NColorizer::TColors CoutColors_;

    THolder<NKikimr::Tests::TServer> Server_;
    THolder<NKikimr::Tests::TClient> Client_;
    TPortManager PortManager_;

    std::optional<NActors::TActorId> AsyncQueryRunnerActorId_;
};


//// TRequestResult

TRequestResult::TRequestResult()
    : Status(Ydb::StatusIds::STATUS_CODE_UNSPECIFIED)
{}

TRequestResult::TRequestResult(Ydb::StatusIds::StatusCode status, const NYql::TIssues& issues)
    : Status(status)
    , Issues(issues)
{}

TRequestResult::TRequestResult(Ydb::StatusIds::StatusCode status, const google::protobuf::RepeatedPtrField<Ydb::Issue::IssueMessage>& issues)
    : Status(status)
{
    NYql::IssuesFromMessage(issues, Issues);
}

bool TRequestResult::IsSuccess() const {
    return Status == Ydb::StatusIds::SUCCESS;
}

TString TRequestResult::ToString() const {
    return TStringBuilder() << "Request finished with status: " << Status << "\nIssues:\n" << Issues.ToString() << "\n";
}


//// TYdbSetup

TYdbSetup::TYdbSetup(const TYdbSetupSettings& settings)
    : Impl_(new TImpl(settings))
{}

TRequestResult TYdbSetup::SchemeQueryRequest(const TRequestOptions& query, TSchemeMeta& meta) const {
    auto schemeQueryOperationResponse = Impl_->SchemeQueryRequest(query)->Get()->Record.GetRef();
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
    const auto& queryOperationResponse = queryResponse.Response->Get()->Record.GetRef();
    const auto& responseRecord = queryOperationResponse.GetResponse();

    resultSets = std::move(queryResponse.ResultSets);
    FillQueryMeta(meta, responseRecord);

    return TRequestResult(queryOperationResponse.GetYdbStatus(), responseRecord.GetQueryIssues());
}

TRequestResult TYdbSetup::YqlScriptRequest(const TRequestOptions& query, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets) const {
    resultSets.clear();

    auto yqlQueryOperationResponse = Impl_->YqlScriptRequest(query)->Get()->Record.GetRef();
    const auto& responseRecord = yqlQueryOperationResponse.GetResponse();

    FillQueryMeta(meta, responseRecord);

    resultSets.reserve(responseRecord.results_size());
    for (const auto& result : responseRecord.results()) {
        resultSets.emplace_back();
        NKikimr::NKqp::ConvertKqpQueryResultToDbResult(result, &resultSets.back());
    }

    return TRequestResult(yqlQueryOperationResponse.GetYdbStatus(), responseRecord.GetQueryIssues());
}

TRequestResult TYdbSetup::GetScriptExecutionOperationRequest(const TString& operation, TExecutionMeta& meta) const {
    auto scriptExecutionOperation = Impl_->GetScriptExecutionOperationRequest(operation);

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

TRequestResult TYdbSetup::FetchScriptExecutionResultsRequest(const TString& operation, i32 resultSetId, Ydb::ResultSet& resultSet) const {
    auto scriptExecutionResults = Impl_->FetchScriptExecutionResultsRequest(operation, resultSetId);

    resultSet = scriptExecutionResults->Get()->ResultSet.value_or(Ydb::ResultSet());

    return TRequestResult(scriptExecutionResults->Get()->Status, scriptExecutionResults->Get()->Issues);
}

TRequestResult TYdbSetup::ForgetScriptExecutionOperationRequest(const TString& operation) const {
    auto forgetScriptExecutionOperationResponse = Impl_->ForgetScriptExecutionOperationRequest(operation);

    return TRequestResult(forgetScriptExecutionOperationResponse->Get()->Status, forgetScriptExecutionOperationResponse->Get()->Issues);
}

void TYdbSetup::QueryRequestAsync(const TRequestOptions& query) const {
    Impl_->QueryRequestAsync(query);
}

void TYdbSetup::WaitAsyncQueries() const {
    Impl_->WaitAsyncQueries();
}

void TYdbSetup::StartTraceOpt() const {
    Impl_->StartTraceOpt();
}

void TYdbSetup::StopTraceOpt() {
    TYdbSetup::TImpl::StopTraceOpt();
}

}  // namespace NKqpRun
