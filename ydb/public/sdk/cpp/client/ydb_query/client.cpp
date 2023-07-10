#include "client.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_query/impl/exec_query.h>

namespace NYdb::NQuery {

class TQueryClient::TImpl: public TClientImplCommon<TQueryClient::TImpl> {
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
        , Settings_(settings)
    {
    }

    ~TImpl() {
        // TODO: Drain sessions.
    }

    TAsyncExecuteQueryIterator StreamExecuteQuery(const TString& query, const TTxControl& txControl,
        const TMaybe<TParams>& params, const TExecuteQuerySettings& settings)
    {
        return TExecQueryImpl::StreamExecuteQuery(Connections_, DbDriverState_, query, txControl, params, settings);
    }

    TAsyncExecuteQueryResult ExecuteQuery(const TString& query, const TTxControl& txControl,
        const TMaybe<TParams>& params, const TExecuteQuerySettings& settings)
    {
        return TExecQueryImpl::ExecuteQuery(Connections_, DbDriverState_, query, txControl, params, settings);
    }

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const TString& script, const TExecuteScriptSettings& settings) {
        using namespace Ydb::Query;
        auto request = MakeOperationRequest<ExecuteScriptRequest>(settings);
        request.set_exec_mode(settings.ExecMode_);
        request.set_stats_mode(settings.StatsMode_);
        request.mutable_script_content()->set_syntax(settings.Syntax_);
        request.mutable_script_content()->set_text(script);

        auto promise = NThreading::NewPromise<TScriptExecutionOperation>();

        auto responseCb = [promise]
            (Ydb::Operations::Operation* response, TPlainStatus status) mutable {
                try {
                    if (response) {
                        NYql::TIssues opIssues;
                        NYql::IssuesFromMessage(response->issues(), opIssues);
                        TStatus executeScriptStatus(TPlainStatus{static_cast<EStatus>(response->status()), std::move(opIssues),
                            status.Endpoint, std::move(status.Metadata)});
                        promise.SetValue(TScriptExecutionOperation(TStatus(std::move(executeScriptStatus)), std::move(*response)));
                    } else {
                        promise.SetValue(TScriptExecutionOperation(TStatus(std::move(status))));
                    }
                } catch (...) {
                    promise.SetException(std::current_exception());
                }
            };

        Connections_->Run<V1::QueryService, ExecuteScriptRequest, Ydb::Operations::Operation>(
            std::move(request),
            responseCb,
            &V1::QueryService::Stub::AsyncExecuteScript,
            DbDriverState_,
            TRpcRequestSettings::Make(settings),
            TEndpointKey());

        return promise.GetFuture();
    }

    TAsyncFetchScriptResultsResult FetchScriptResults(const TString& executionId, int64_t resultSetId, const TFetchScriptResultsSettings& settings) {
        auto request = MakeRequest<Ydb::Query::FetchScriptResultsRequest>();
        request.set_execution_id(executionId);
        request.set_result_set_id(resultSetId);
        return FetchScriptResultsImpl(std::move(request), settings);
    }

    TAsyncFetchScriptResultsResult FetchScriptResults(const TScriptExecutionOperation& scriptExecutionOperation, int64_t resultSetId, const TFetchScriptResultsSettings& settings) {
        auto request = MakeRequest<Ydb::Query::FetchScriptResultsRequest>();
        request.set_operation_id(NKikimr::NOperationId::ProtoToString(scriptExecutionOperation.Id()));
        request.set_result_set_id(resultSetId);
        return FetchScriptResultsImpl(std::move(request), settings);
    }

    TAsyncFetchScriptResultsResult FetchScriptResultsImpl(Ydb::Query::FetchScriptResultsRequest&& request, const TFetchScriptResultsSettings& settings) {
        using namespace Ydb::Query;
        if (settings.FetchToken_) {
            request.set_fetch_token(settings.FetchToken_);
        }
        request.set_rows_limit(settings.RowsLimit_);

        auto promise = NThreading::NewPromise<TFetchScriptResultsResult>();

        auto extractor = [promise]
            (FetchScriptResultsResponse* response, TPlainStatus status) mutable {
                if (response) {
                    NYql::TIssues opIssues;
                    NYql::IssuesFromMessage(response->issues(), opIssues);
                    TStatus st(static_cast<EStatus>(response->status()), std::move(opIssues));

                    if (st.IsSuccess()) {
                        promise.SetValue(
                            TFetchScriptResultsResult(
                                std::move(st),
                                TResultSet(std::move(*response->mutable_result_set())),
                                response->result_set_index(),
                                response->next_fetch_token()
                            )
                        );
                    } else {
                        promise.SetValue(TFetchScriptResultsResult(std::move(st)));
                    }
                } else {
                    TStatus st(std::move(status));
                    promise.SetValue(TFetchScriptResultsResult(std::move(st)));
                }
            };

        TRpcRequestSettings rpcSettings;
        rpcSettings.ClientTimeout = TDuration::Seconds(60);

        Connections_->Run<V1::QueryService, FetchScriptResultsRequest, FetchScriptResultsResponse>(
            std::move(request),
            extractor,
            &V1::QueryService::Stub::AsyncFetchScriptResults,
            DbDriverState_,
            rpcSettings,
            TEndpointKey());

        return promise.GetFuture();
    }

private:
    TClientSettings Settings_;
};

TQueryClient::TQueryClient(const TDriver& driver, const TClientSettings& settings)
    : Impl_(new TQueryClient::TImpl(CreateInternalInterface(driver), settings))
{
}

TAsyncExecuteQueryResult TQueryClient::ExecuteQuery(const TString& query, const TTxControl& txControl,
    const TExecuteQuerySettings& settings)
{
    return Impl_->ExecuteQuery(query, txControl, {}, settings);
}

TAsyncExecuteQueryResult TQueryClient::ExecuteQuery(const TString& query, const TTxControl& txControl,
    const TParams& params, const TExecuteQuerySettings& settings)
{
    return Impl_->ExecuteQuery(query, txControl, params, settings);
}


TAsyncExecuteQueryIterator TQueryClient::StreamExecuteQuery(const TString& query, const TTxControl& txControl,
    const TExecuteQuerySettings& settings)
{
    return Impl_->StreamExecuteQuery(query, txControl, {}, settings);
}

TAsyncExecuteQueryIterator TQueryClient::StreamExecuteQuery(const TString& query, const TTxControl& txControl,
    const TParams& params, const TExecuteQuerySettings& settings)
{
    return Impl_->StreamExecuteQuery(query, txControl, params, settings);
}

NThreading::TFuture<TScriptExecutionOperation> TQueryClient::ExecuteScript(const TString& script,
    const TExecuteScriptSettings& settings)
{
    return Impl_->ExecuteScript(script, settings);
}

TAsyncFetchScriptResultsResult TQueryClient::FetchScriptResults(const TString& executionId, int64_t resultSetId,
    const TFetchScriptResultsSettings& settings)
{
    return Impl_->FetchScriptResults(executionId, resultSetId, settings);
}

TAsyncFetchScriptResultsResult TQueryClient::FetchScriptResults(const TScriptExecutionOperation& scriptExecutionOperation, int64_t resultSetId,
    const TFetchScriptResultsSettings& settings)
{
    return Impl_->FetchScriptResults(scriptExecutionOperation, resultSetId, settings);
}

} // namespace NYdb::NQuery
