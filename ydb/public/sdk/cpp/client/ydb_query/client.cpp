#include "client.h"
#include "impl/client_session.h"

#define INCLUDE_YDB_INTERNAL_H
#include <ydb/public/sdk/cpp/client/impl/ydb_endpoints/endpoints.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/make_request/make.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry_async.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry_sync.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/session_client/session_client.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/session_pool/session_pool.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb/public/lib/operation_id/operation_id.h>
#include <ydb/public/sdk/cpp/client/ydb_common_client/impl/client.h>
#include <ydb/public/sdk/cpp/client/ydb_query/impl/exec_query.h>
#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>

namespace NYdb::NQuery {

using TRetryContext = NRetry::TRetryContextAsync<TQueryClient, TExecuteQueryResult>;
using TRetryContextWithSession = NRetry::TRetryWithSessionAsync<TQueryClient, TQueryClient::TQueryFunc, TExecuteQueryResult>;

TCreateSessionSettings::TCreateSessionSettings() {
    ClientTimeout_ = TDuration::Seconds(5);
};

class TQueryClient::TImpl: public TClientImplCommon<TQueryClient::TImpl>, public ISessionClient {
    friend class ::NYdb::NQuery::TSession;
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
        , Settings_(settings)
        , SessionPool_(Settings_.SessionPoolSettings_.MaxActiveSessions_)
    {
    }

    ~TImpl() {
        // TODO: Drain sessions.
    }

    TAsyncExecuteQueryIterator StreamExecuteQuery(const TString& query, const TTxControl& txControl,
        const TMaybe<TParams>& params, const TExecuteQuerySettings& settings, const TString& sessionId = {})
    {
        return TExecQueryImpl::StreamExecuteQuery(
            Connections_, DbDriverState_, query, txControl, params, settings, sessionId);
    }

    TAsyncExecuteQueryResult ExecuteQuery(const TString& query, const TTxControl& txControl,
        const TMaybe<TParams>& params, const TExecuteQuerySettings& settings,
        const TString& sessionId = {})
    {
        return TExecQueryImpl::ExecuteQuery(
            Connections_, DbDriverState_, query, txControl, params, settings, sessionId);
    }

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const TString& script, const TExecuteScriptSettings& settings) {
        using namespace Ydb::Query;
        auto request = MakeOperationRequest<ExecuteScriptRequest>(settings);
        request.set_exec_mode(settings.ExecMode_);
        request.set_stats_mode(settings.StatsMode_);
        request.mutable_script_content()->set_syntax(settings.Syntax_);
        request.mutable_script_content()->set_text(script);
        SetDuration(settings.ResultsTtl_, *request.mutable_results_ttl());
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
            TRpcRequestSettings::Make(settings));

        return promise.GetFuture();
    }

    TAsyncFetchScriptResultsResult FetchScriptResults(const NKikimr::NOperationId::TOperationId& operationId, int64_t resultSetIndex, const TFetchScriptResultsSettings& settings) {
        auto request = MakeRequest<Ydb::Query::FetchScriptResultsRequest>();
        request.set_operation_id(NKikimr::NOperationId::ProtoToString(operationId));
        request.set_result_set_index(resultSetIndex);
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
            rpcSettings);

        return promise.GetFuture();
    }

    void DeleteSession(TKqpSessionCommon* sessionImpl) override {
        //TODO: Remove this copy-paste

        // Closing not owned by session pool session should not fire getting new session
        if (sessionImpl->IsOwnedBySessionPool()) {
            if (SessionPool_.CheckAndFeedWaiterNewSession(sessionImpl->NeedUpdateActiveCounter())) {
                // We requested new session for waiter which already incremented
                // active session counter and old session will be deleted
                // - skip update active counter in this case
                sessionImpl->SetNeedUpdateActiveCounter(false);
            }
        }

        if (sessionImpl->NeedUpdateActiveCounter()) {
            SessionPool_.DecrementActiveCounter();
        }

        delete sessionImpl;
    }

    bool ReturnSession(TKqpSessionCommon* sessionImpl) override {
        Y_VERIFY(sessionImpl->GetState() == TSession::TImpl::S_ACTIVE ||
            sessionImpl->GetState() == TSession::TImpl::S_IDLE);

        //TODO: Remove this copy-paste from table client
        bool needUpdateCounter = sessionImpl->NeedUpdateActiveCounter();
        // Also removes NeedUpdateActiveCounter flag
        sessionImpl->MarkIdle();
        sessionImpl->SetTimeInterval(TDuration::Zero());
        if (!SessionPool_.ReturnSession(sessionImpl, needUpdateCounter)) {
            sessionImpl->SetNeedUpdateActiveCounter(needUpdateCounter);
            return false;
        }
        return true;
    }

    void DoAttachSession(Ydb::Query::CreateSessionResponse* resp,
        NThreading::TPromise<TCreateSessionResult> promise, const TString& endpoint,
        std::shared_ptr<TQueryClient::TImpl> client)
    {
        using TStreamProcessorPtr = TSession::TImpl::TStreamProcessorPtr;
        Ydb::Query::AttachSessionRequest request;
        const auto sessionId = resp->session_id();
        request.set_session_id(sessionId);

        auto args = std::make_shared<TSession::TImpl::TAttachSessionArgs>(promise, sessionId, endpoint, client);

        // Do not pass client timeout here. Session must be alive
        TRpcRequestSettings rpcSettings;
        rpcSettings.PreferredEndpoint = TEndpointKey(endpoint, GetNodeIdFromSession(sessionId));

        Connections_->StartReadStream<
            Ydb::Query::V1::QueryService,
            Ydb::Query::AttachSessionRequest,
            Ydb::Query::SessionState>
        (
            std::move(request),
            [args] (TPlainStatus status, TStreamProcessorPtr processor) mutable {
            if (processor) {
                TSession::TImpl::MakeImplAsync(processor, args);
            } else {
                TStatus st(std::move(status));
                args->Promise.SetValue(TCreateSessionResult(std::move(st), TSession()));
            }
        },
        &Ydb::Query::V1::QueryService::Stub::AsyncAttachSession,
        DbDriverState_,
        rpcSettings);
    }

    TAsyncCreateSessionResult CreateAttachedSession(TDuration timeout) {
        using namespace Ydb::Query;

        Ydb::Query::CreateSessionRequest request;

        auto promise = NThreading::NewPromise<TCreateSessionResult>();

        auto self = shared_from_this();

        auto extractor = [promise, self] (Ydb::Query::CreateSessionResponse* resp, TPlainStatus status) mutable {
            if (resp) {
                if (resp->status() != Ydb::StatusIds::SUCCESS) {
                    NYql::TIssues opIssues;
                    NYql::IssuesFromMessage(resp->issues(), opIssues);
                    TStatus st(static_cast<EStatus>(resp->status()), std::move(opIssues));
                    promise.SetValue(TCreateSessionResult(std::move(st), TSession()));
                } else {
                    self->DoAttachSession(resp, promise, status.Endpoint, self);
                }
            } else {
                TStatus st(std::move(status));
                promise.SetValue(TCreateSessionResult(std::move(st), TSession()));
            }
        };

        TRpcRequestSettings rpcSettings;
        rpcSettings.ClientTimeout = timeout;

        Connections_->Run<V1::QueryService, CreateSessionRequest, CreateSessionResponse>(
            std::move(request),
            extractor,
            &V1::QueryService::Stub::AsyncCreateSession,
            DbDriverState_,
            rpcSettings);

        return promise.GetFuture();
    }

    TAsyncCreateSessionResult GetSession(const TCreateSessionSettings& settings) {
        using namespace NSessionPool;

        class TQueryClientGetSessionCtx : public IGetSessionCtx {
        public:
            TQueryClientGetSessionCtx(std::shared_ptr<TQueryClient::TImpl> client, TDuration timeout)
                : Promise(NThreading::NewPromise<TCreateSessionResult>())
                , Client(client)
                , ClientTimeout(timeout)
            {}

            TAsyncCreateSessionResult GetFuture() {
                return Promise.GetFuture();
            }

            void ReplyError(TStatus status) override {
                TSession session;
                ScheduleReply(TCreateSessionResult(std::move(status), std::move(session)));
            }

            void ReplySessionToUser(TKqpSessionCommon* session) override {
                TCreateSessionResult val(
                    TStatus(TPlainStatus()),
                    TSession(
                        Client,
                        static_cast<TSession::TImpl*>(session)
                    )
                );

                ScheduleReply(std::move(val));
            }

            void ReplyNewSession() override {
                Client->CreateAttachedSession(ClientTimeout).Subscribe(
                    [promise{std::move(Promise)}](TAsyncCreateSessionResult future) mutable
                {
                    promise.SetValue(future.ExtractValue());
                });
            }

        private:
            void ScheduleReply(TCreateSessionResult val) {
                Promise.SetValue(std::move(val));
            }
            NThreading::TPromise<TCreateSessionResult> Promise;
            std::shared_ptr<TQueryClient::TImpl> Client;
            TDuration ClientTimeout;
        };

        auto ctx = std::make_unique<TQueryClientGetSessionCtx>(shared_from_this(), settings.ClientTimeout_);
        auto future = ctx->GetFuture();
        SessionPool_.GetSession(std::move(ctx));
        return future;
    }

    i64 GetActiveSessionCount() const {
        return SessionPool_.GetActiveSessions();
    }

    i64 GetActiveSessionsLimit() const {
        return SessionPool_.GetActiveSessionsLimit();
    }

    i64 GetCurrentPoolSize() const {
        return SessionPool_.GetCurrentPoolSize();
    }

    void StartPeriodicSessionPoolTask() {
        // Session pool guarantees than client is alive during call callbacks
        auto deletePredicate = [this](TKqpSessionCommon* s, size_t sessionsCount) {

            const auto& sessionPoolSettings = Settings_.SessionPoolSettings_;
            const auto spentTime = s->GetTimeToTouchFast() - s->GetTimeInPastFast();

            if (spentTime >= sessionPoolSettings.CloseIdleThreshold_) {
                if (sessionsCount > sessionPoolSettings.MinPoolSize_) {
                    return true;
                }
            }

            return false;
        };

        // No need to keep-alive
        auto keepAliveCmd = [](TKqpSessionCommon*) {
        };

        std::weak_ptr<TQueryClient::TImpl> weak = shared_from_this();
        Connections_->AddPeriodicTask(
            SessionPool_.CreatePeriodicTask(
                weak,
                std::move(keepAliveCmd),
                std::move(deletePredicate)
            ), NSessionPool::PERIODIC_ACTION_INTERVAL);
    }

    void CollectRetryStatAsync(EStatus status) {
        Y_UNUSED(status);
    }

    void CollectRetryStatSync(EStatus status) {
        Y_UNUSED(status);
    }

private:
    TClientSettings Settings_;
    NSessionPool::TSessionPool SessionPool_;
};

TQueryClient::TQueryClient(const TDriver& driver, const TClientSettings& settings)
    : Impl_(new TQueryClient::TImpl(CreateInternalInterface(driver), settings))
{
    Impl_->StartPeriodicSessionPoolTask();
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

TAsyncFetchScriptResultsResult TQueryClient::FetchScriptResults(const NKikimr::NOperationId::TOperationId& operationId, int64_t resultSetIndex,
    const TFetchScriptResultsSettings& settings)
{
    return Impl_->FetchScriptResults(operationId, resultSetIndex, settings);
}

TAsyncCreateSessionResult TQueryClient::GetSession(const TCreateSessionSettings& settings)
{
    return Impl_->GetSession(settings);
}

i64 TQueryClient::GetActiveSessionCount() const {
    return Impl_->GetActiveSessionCount();
}

i64 TQueryClient::GetActiveSessionsLimit() const {
    return Impl_->GetActiveSessionsLimit();
}

i64 TQueryClient::GetCurrentPoolSize() const {
    return Impl_->GetCurrentPoolSize();
}

TAsyncExecuteQueryResult TQueryClient::RetryQuery(TQueryFunc&& queryFunc, TRetryOperationSettings settings)
{
    TRetryContext::TPtr ctx(new TRetryContextWithSession(*this, std::move(queryFunc), settings));
    ctx->Execute();
    return ctx->GetFuture();
}

////////////////////////////////////////////////////////////////////////////////

TCreateSessionResult::TCreateSessionResult(TStatus&& status, TSession&& session)
    : TStatus(std::move(status))
    , Session_(std::move(session))
{}

TSession TCreateSessionResult::GetSession() const {
    CheckStatusOk("TCreateSessionResult::GetSession");
    return Session_;
}

////////////////////////////////////////////////////////////////////////////////

TSession::TSession()
{}

TSession::TSession(std::shared_ptr<TQueryClient::TImpl> client, TSession::TImpl* session)
    : Client_(client)
    , SessionImpl_(session, TKqpSessionCommon::GetSmartDeleter(client))
{}

const TString& TSession::GetId() const {
    return SessionImpl_->GetId();
}

TAsyncExecuteQueryResult TSession::ExecuteQuery(const TString& query, const TTxControl& txControl,
    const TExecuteQuerySettings& settings)
{
    return NSessionPool::InjectSessionStatusInterception(
        SessionImpl_,
        Client_->ExecuteQuery(query, txControl, {}, settings, SessionImpl_->GetId()),
        true,
        Client_->Settings_.SessionPoolSettings_.CloseIdleThreshold_);
}

TAsyncExecuteQueryResult TSession::ExecuteQuery(const TString& query, const TTxControl& txControl,
    const TParams& params, const TExecuteQuerySettings& settings)
{
    return NSessionPool::InjectSessionStatusInterception(
        SessionImpl_,
        Client_->ExecuteQuery(query, txControl, params, settings, SessionImpl_->GetId()),
        true,
        Client_->Settings_.SessionPoolSettings_.CloseIdleThreshold_);
}

TAsyncExecuteQueryIterator TSession::StreamExecuteQuery(const TString& query, const TTxControl& txControl,
    const TExecuteQuerySettings& settings)
{
    return NSessionPool::InjectSessionStatusInterception(
        SessionImpl_,
        Client_->StreamExecuteQuery(query, txControl, {}, settings, SessionImpl_->GetId()),
        true,
        Client_->Settings_.SessionPoolSettings_.CloseIdleThreshold_);
}

TAsyncExecuteQueryIterator TSession::StreamExecuteQuery(const TString& query, const TTxControl& txControl,
    const TParams& params, const TExecuteQuerySettings& settings)
{
    return NSessionPool::InjectSessionStatusInterception(
        SessionImpl_,
        Client_->StreamExecuteQuery(query, txControl, params, settings, SessionImpl_->GetId()),
        true,
        Client_->Settings_.SessionPoolSettings_.CloseIdleThreshold_);
}

} // namespace NYdb::NQuery
