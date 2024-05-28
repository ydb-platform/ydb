#include "ydb_setup.h"

#include <library/cpp/colorizer/colors.h>

#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>

#include <ydb/core/testlib/test_client.h>

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

}  // anonymous namespace


//// TYdbSetup::TImpl

class TYdbSetup::TImpl {
private:
    TAutoPtr<TLogBackend> CreateLogBackend() const {
        if (Settings_.LogOutputFile) {
            return NActors::CreateFileBackend(*Settings_.LogOutputFile);
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
        };

        serverSettings.SetLoggerInitializer(loggerInitializer);
        serverSettings.SetLogBackend(CreateLogBackend());
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

    NKikimr::Tests::TServerSettings GetServerSettings() {
        ui32 msgBusPort = PortManager_.GetPort();

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
        serverSettings.SetInitializeFederatedQuerySetupFactory(true);

        SetLoggerSettings(serverSettings);
        SetFunctionRegistry(serverSettings);

        if (Settings_.MonitoringEnabled) {
            serverSettings.InitKikimrRunConfig();
        }

        return serverSettings;
    }

    void InitializeServer() {
        NKikimr::Tests::TServerSettings serverSettings = GetServerSettings();

        Server_ = MakeHolder<NKikimr::Tests::TServer>(serverSettings);
        Server_->GetRuntime()->SetDispatchTimeout(TDuration::Max());

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
        GetRuntime()->Register(CreateResourcesWaiterActor(promise, Settings_.NodeCount));

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
        InitializeYqlLogger();
        InitializeServer();

        if (Settings_.NodeCount > 1) {
            WaitResourcesPublishing();
        }

        if (Settings_.MonitoringEnabled) {
            Cout << CoutColors_.Cyan() << "Monitoring port: " << CoutColors_.Default() << Server_->GetRuntime()->GetMonPort() << Endl;
        }
    }

    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr SchemeQueryRequest(const TString& query, const TString& traceId) const {
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_DDL, NKikimrKqp::QUERY_ACTION_EXECUTE, traceId, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvQueryRequest, NKikimr::NKqp::TEvKqp::TEvQueryResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvKqp::TEvScriptResponse::TPtr ScriptRequest(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId) const {
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvScriptRequest>();
        FillScriptRequest(script, action, traceId, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvScriptRequest, NKikimr::NKqp::TEvKqp::TEvScriptResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr QueryRequest(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId, std::vector<Ydb::ResultSet>& resultSets, TProgressCallback progressCallback) const {
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_QUERY, action, traceId, event->Record);

        if (auto progressStatsPeriodMs = Settings_.AppConfig.GetQueryServiceConfig().GetProgressStatsPeriodMs()) {
            event->SetProgressStatsPeriod(TDuration::MilliSeconds(progressStatsPeriodMs));
        }

        auto promise = NThreading::NewPromise<NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr>();
        auto rowsLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultRowsLimit();
        auto sizeLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultSizeLimit();
        GetRuntime()->Register(CreateRunScriptActorMock(std::move(event), promise, rowsLimit, sizeLimit, resultSets, progressCallback));

        return promise.GetFuture().GetValueSync();
    }

    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr YqlScriptRequest(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId) const {
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillQueryRequest(query, NKikimrKqp::QUERY_TYPE_SQL_SCRIPT, action, traceId, event->Record);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvQueryRequest, NKikimr::NKqp::TEvKqp::TEvQueryResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvGetScriptExecutionOperationResponse::TPtr GetScriptExecutionOperationRequest(const TString& operation) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvGetScriptExecutionOperation>(Settings_.DomainName, operationId);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvGetScriptExecutionOperation, NKikimr::NKqp::TEvGetScriptExecutionOperationResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvFetchScriptResultsResponse::TPtr FetchScriptExecutionResultsRequest(const TString& operation, i32 resultSetId) const {
        TString executionId = *NKikimr::NKqp::ScriptExecutionIdFromOperation(operation);

        NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor();
        auto rowsLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultRowsLimit();
        auto sizeLimit = Settings_.AppConfig.GetQueryServiceConfig().GetScriptResultSizeLimit();
        NActors::IActor* fetchActor = NKikimr::NKqp::CreateGetScriptExecutionResultActor(edgeActor, Settings_.DomainName, executionId, resultSetId, 0, rowsLimit, sizeLimit, TInstant::Max());

        GetRuntime()->Register(fetchActor);

        return GetRuntime()->GrabEdgeEvent<NKikimr::NKqp::TEvFetchScriptResultsResponse>(edgeActor);
    }

    NKikimr::NKqp::TEvForgetScriptExecutionOperationResponse::TPtr ForgetScriptExecutionOperationRequest(const TString& operation) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvForgetScriptExecutionOperation>(Settings_.DomainName, operationId, TInstant::Max());

        return RunKqpProxyRequest<NKikimr::NKqp::TEvForgetScriptExecutionOperation, NKikimr::NKqp::TEvForgetScriptExecutionOperationResponse>(std::move(event));
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
        NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor();
        NActors::TActorId kqpProxy = NKikimr::NKqp::MakeKqpProxyID(GetRuntime()->GetNodeId());

        GetRuntime()->Send(kqpProxy, edgeActor, event.Release());

        return GetRuntime()->GrabEdgeEvent<TResponse>(edgeActor);
    }

private:
    void FillQueryRequest(const TString& query, NKikimrKqp::EQueryType type, NKikimrKqp::EQueryAction action, const TString& traceId, NKikimrKqp::TEvQueryRequest& event) const {
        event.SetTraceId(traceId);
        event.SetUserToken(NACLib::TUserToken(Settings_.YqlToken, BUILTIN_ACL_ROOT, {}).SerializeAsString());

        auto request = event.MutableRequest();
        request->SetQuery(query);
        request->SetType(type);
        request->SetAction(action);
        request->SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL);
        request->SetDatabase(Settings_.DomainName);
    }

    void FillScriptRequest(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId, NKikimrKqp::TEvQueryRequest& event) const {
        FillQueryRequest(script, NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT, action, traceId, event);

        auto request = event.MutableRequest();
        if (action == NKikimrKqp::QUERY_ACTION_EXECUTE) {
            request->MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
            request->MutableTxControl()->set_commit_tx(true);
        }
    }

private:
    TYdbSetupSettings Settings_;
    NColorizer::TColors CoutColors_;

    THolder<NKikimr::Tests::TServer> Server_;
    THolder<NKikimr::Tests::TClient> Client_;
    TPortManager PortManager_;
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

TRequestResult TYdbSetup::SchemeQueryRequest(const TString& query, const TString& traceId, TSchemeMeta& meta) const {
    auto schemeQueryOperationResponse = Impl_->SchemeQueryRequest(query, traceId)->Get()->Record.GetRef();
    const auto& responseRecord = schemeQueryOperationResponse.GetResponse();

    meta.Ast = responseRecord.GetQueryAst();

    return TRequestResult(schemeQueryOperationResponse.GetYdbStatus(), responseRecord.GetQueryIssues());
}

TRequestResult TYdbSetup::ScriptRequest(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId, TString& operation) const {
    auto scriptExecutionOperation = Impl_->ScriptRequest(script, action, traceId);

    operation = scriptExecutionOperation->Get()->OperationId;

    return TRequestResult(scriptExecutionOperation->Get()->Status, scriptExecutionOperation->Get()->Issues);
}

TRequestResult TYdbSetup::QueryRequest(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets, TProgressCallback progressCallback) const {
    resultSets.clear();

    auto queryOperationResponse = Impl_->QueryRequest(query, action, traceId, resultSets, progressCallback)->Get()->Record.GetRef();
    const auto& responseRecord = queryOperationResponse.GetResponse();

    meta.Ast = responseRecord.GetQueryAst();
    if (const auto& plan = responseRecord.GetQueryPlan()) {
        meta.Plan = plan;
    }

    return TRequestResult(queryOperationResponse.GetYdbStatus(), responseRecord.GetQueryIssues());
}

TRequestResult TYdbSetup::YqlScriptRequest(const TString& query, NKikimrKqp::EQueryAction action, const TString& traceId, TQueryMeta& meta, std::vector<Ydb::ResultSet>& resultSets) const {
    resultSets.clear();

    auto yqlQueryOperationResponse = Impl_->YqlScriptRequest(query, action, traceId)->Get()->Record.GetRef();
    const auto& responseRecord = yqlQueryOperationResponse.GetResponse();

    meta.Ast = responseRecord.GetQueryAst();
    meta.Plan = responseRecord.GetQueryPlan();

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

void TYdbSetup::StartTraceOpt() const {
    Impl_->StartTraceOpt();
}

void TYdbSetup::StopTraceOpt() {
    TYdbSetup::TImpl::StopTraceOpt();
}

}  // namespace NKqpRun
