#include "ydb_setup.h"

#include <ydb/core/kqp/common/kqp_script_executions.h>
#include <ydb/core/kqp/proxy_service/kqp_script_executions.h>

#include <ydb/core/testlib/test_client.h>


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

        if (Settings_.LogOutputFile) {
            serverSettings.SetLogBackend(NActors::CreateFileBackend(*Settings_.LogOutputFile));
        } else {
            serverSettings.SetLogBackend(NActors::CreateStderrBackend());
        }
    }

    void SetFunctionRegistry(NKikimr::Tests::TServerSettings& serverSettings) const {
        if (!Settings_.FunctionRegistry) {
            return;
        }

        auto functionRegistryFactory = [this](const NKikimr::NScheme::TTypeRegistry&) {
            return Settings_.FunctionRegistry;
        };

        serverSettings.SetFrFactory(functionRegistryFactory);
    }

    NKikimr::Tests::TServerSettings GetServerSettings() {
        ui32 msgBusPort = PortManager_.GetPort();

        NKikimr::Tests::TServerSettings serverSettings(msgBusPort);
        serverSettings.SetNodeCount(1);

        serverSettings.SetDomainName(Settings_.DomainName);
        serverSettings.SetAppConfig(Settings_.AppConfig);
        serverSettings.SetFeatureFlags(Settings_.AppConfig.GetFeatureFlags());

        serverSettings.SetCredentialsFactory(std::make_shared<TStaticSecuredCredentialsFactory>(Settings_.YqlToken));
        serverSettings.SetInitializeFederatedQuerySetupFactory(true);

        SetLoggerSettings(serverSettings);
        SetFunctionRegistry(serverSettings);

        return serverSettings;
    }

    void InitializeServer() {
        NKikimr::Tests::TServerSettings serverSettings = GetServerSettings();

        Server_ = MakeHolder<NKikimr::Tests::TServer>(serverSettings);
        Server_->GetRuntime()->SetDispatchTimeout(TDuration::Max());

        Client_ = MakeHolder<NKikimr::Tests::TClient>(serverSettings);
        Client_->InitRootScheme();
    }

public:
    explicit TImpl(const TYdbSetupSettings& settings)
        : Settings_(settings)
    {
        InitializeServer();
    }

    NKikimr::NKqp::TEvKqp::TEvQueryResponse::TPtr SchemeQueryRequest(const TString& query) const {
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvQueryRequest>();
        FillSchemeRequest(query, *event->Record.MutableRequest());

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvQueryRequest, NKikimr::NKqp::TEvKqp::TEvQueryResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvKqp::TEvScriptResponse::TPtr ScriptQueryRequest(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId) const {
        auto event = MakeHolder<NKikimr::NKqp::TEvKqp::TEvScriptRequest>();
        event->Record.SetTraceId(traceId);

        FillScriptRequest(script, action, *event->Record.MutableRequest());

        return RunKqpProxyRequest<NKikimr::NKqp::TEvKqp::TEvScriptRequest, NKikimr::NKqp::TEvKqp::TEvScriptResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvGetScriptExecutionOperationResponse::TPtr GetScriptExecutionOperationRequest(const TString& operation) const {
        NKikimr::NOperationId::TOperationId operationId(operation);
        auto event = MakeHolder<NKikimr::NKqp::TEvGetScriptExecutionOperation>(Settings_.DomainName, operationId);

        return RunKqpProxyRequest<NKikimr::NKqp::TEvGetScriptExecutionOperation, NKikimr::NKqp::TEvGetScriptExecutionOperationResponse>(std::move(event));
    }

    NKikimr::NKqp::TEvKqp::TEvFetchScriptResultsResponse::TPtr FetchScriptExecutionResultsRequest(const TString& operation, i32 resultSetId, i64 limit) const {
        TString executionId = *NKikimr::NKqp::ScriptExecutionIdFromOperation(operation);

        NActors::TActorId edgeActor = GetRuntime()->AllocateEdgeActor();
        NActors::IActor* fetchActor = NKikimr::NKqp::CreateGetScriptExecutionResultActor(edgeActor, Settings_.DomainName, executionId, resultSetId, 0, limit);

        GetRuntime()->Register(fetchActor);

        return GetRuntime()->GrabEdgeEvent<NKikimr::NKqp::TEvKqp::TEvFetchScriptResultsResponse>(edgeActor);
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
    void FillSchemeRequest(const TString& query, NKikimrKqp::TQueryRequest& request) const {
        request.SetType(NKikimrKqp::QUERY_TYPE_SQL_DDL);
        request.SetAction(NKikimrKqp::QUERY_ACTION_EXECUTE);
        request.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL);

        request.SetDatabase(Settings_.DomainName);
        request.SetQuery(query);
    }

    void FillScriptRequest(const TString& script, NKikimrKqp::EQueryAction action, NKikimrKqp::TQueryRequest& request) const {
        if (action == NKikimrKqp::QUERY_ACTION_EXECUTE) {
            request.MutableTxControl()->mutable_begin_tx()->mutable_serializable_read_write();
            request.MutableTxControl()->set_commit_tx(true);
        }

        request.SetType(NKikimrKqp::QUERY_TYPE_SQL_GENERIC_SCRIPT);
        request.SetAction(action);
        request.SetCollectStats(Ydb::Table::QueryStatsCollection::STATS_COLLECTION_FULL);

        request.SetDatabase(Settings_.DomainName);
        request.SetQuery(script);
    }

private:
    TYdbSetupSettings Settings_;

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

TRequestResult TYdbSetup::SchemeQueryRequest(const TString& query, TSchemeMeta& meta) const {
    auto schemeQueryOperationResponse = Impl_->SchemeQueryRequest(query)->Get()->Record.GetRef();

    meta.Ast = schemeQueryOperationResponse.GetResponse().GetQueryAst();

    NYql::TIssues issues;
    NYql::IssuesFromMessage(schemeQueryOperationResponse.GetResponse().GetQueryIssues(), issues);

    return TRequestResult(schemeQueryOperationResponse.GetYdbStatus(), issues);
}

TRequestResult TYdbSetup::ScriptQueryRequest(const TString& script, NKikimrKqp::EQueryAction action, const TString& traceId, TString& operation) const {
    auto scriptExecutionOperation = Impl_->ScriptQueryRequest(script, action, traceId);

    operation = scriptExecutionOperation->Get()->OperationId;

    return TRequestResult(scriptExecutionOperation->Get()->Status, scriptExecutionOperation->Get()->Issues);
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
        meta.Plan = deserializedMeta.exec_stats().query_plan();
    }

    return TRequestResult(scriptExecutionOperation->Get()->Status, scriptExecutionOperation->Get()->Issues);
}

TRequestResult TYdbSetup::FetchScriptExecutionResultsRequest(const TString& operation, i32 resultSetId, i64 limit, Ydb::ResultSet& resultSet) const {
    auto scriptExecutionResults = Impl_->FetchScriptExecutionResultsRequest(operation, resultSetId, limit)->Get()->Record;

    resultSet = scriptExecutionResults.GetResultSet();

    NYql::TIssues issues;
    NYql::IssuesFromMessage(scriptExecutionResults.GetIssues(), issues);

    return TRequestResult(scriptExecutionResults.GetStatus(), issues);
}

}  // namespace NKqpRun
