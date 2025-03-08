#include "impl/client_session.h"

#include <ydb-cpp-sdk/client/query/client.h>

#define INCLUDE_YDB_INTERNAL_H
#include <src/client/impl/ydb_endpoints/endpoints.h>
#include <src/client/impl/ydb_internal/make_request/make.h>
#include <src/client/impl/ydb_internal/retry/retry.h>
#include <src/client/impl/ydb_internal/retry/retry_async.h>
#include <src/client/impl/ydb_internal/retry/retry_sync.h>
#include <src/client/impl/ydb_internal/session_client/session_client.h>
#include <src/client/impl/ydb_internal/session_pool/session_pool.h>
#undef INCLUDE_YDB_INTERNAL_H

#include <ydb-cpp-sdk/library/operation_id/operation_id.h>
#include <src/client/common_client/impl/client.h>
#include <src/client/query/impl/exec_query.h>
#include <ydb-cpp-sdk/client/retry/retry.h>

#include <ydb/public/api/grpc/ydb_query_v1.grpc.pb.h>

namespace NYdb::inline Dev::NQuery {

using TRetryContextResultAsync = NRetry::Async::TRetryContext<TQueryClient, TAsyncExecuteQueryResult>;
using TRetryContextAsync = NRetry::Async::TRetryContext<TQueryClient, TAsyncStatus>;

NYdb::NRetry::TRetryOperationSettings GetRetrySettings(TDuration timeout, bool isIndempotent) {
    return NYdb::NRetry::TRetryOperationSettings()
        .Idempotent(isIndempotent)
        .GetSessionClientTimeout(timeout)
        .MaxTimeout(timeout);
}

TCreateSessionSettings::TCreateSessionSettings() {
    ClientTimeout_ = TDuration::Seconds(5);
};

static void SetTxSettings(const TTxSettings& txSettings, Ydb::Query::TransactionSettings* proto)
{
    switch (txSettings.GetMode()) {
        case TTxSettings::TS_SERIALIZABLE_RW:
            proto->mutable_serializable_read_write();
            break;
        case TTxSettings::TS_ONLINE_RO:
            proto->mutable_online_read_only()->set_allow_inconsistent_reads(
                txSettings.OnlineSettings_.AllowInconsistentReads_);
            break;
        case TTxSettings::TS_STALE_RO:
            proto->mutable_stale_read_only();
            break;
        case TTxSettings::TS_SNAPSHOT_RO:
            proto->mutable_snapshot_read_only();
            break;
        case TTxSettings::TS_SNAPSHOT_RW:
            proto->mutable_snapshot_read_write();
            break;
        default:
            throw TContractViolation("Unexpected transaction mode.");
    }
}

class TQueryClient::TImpl: public TClientImplCommon<TQueryClient::TImpl>, public ISessionClient {
    friend class ::NYdb::NQuery::TSession;
public:
    TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings)
        : TClientImplCommon(std::move(connections), settings)
        , Settings_(settings)
        , SessionPool_(Settings_.SessionPoolSettings_.MaxActiveSessions_)
    {
        SetStatCollector(DbDriverState_->StatCollector.GetClientStatCollector("Query"));
        SessionPool_.SetStatCollector(DbDriverState_->StatCollector.GetSessionPoolStatCollector("Query"));
    }

    ~TImpl() {
        // TODO: Drain sessions.
    }

    void SetStatCollector(const NSdkStats::TStatCollector::TClientStatCollector& collector) {
        QuerySizeHistogram_.Set(collector.QuerySize);
        ParamsSizeHistogram_.Set(collector.ParamsSize);
        RetryOperationStatCollector_ = collector.RetryOperationStatCollector;
    }

    TAsyncExecuteQueryIterator StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
        const std::optional<TParams>& params, const TExecuteQuerySettings& settings, const std::optional<TSession>& session = {})
    {
        CollectQuerySize(query);
        CollectParamsSize(params ? &params->GetProtoMap() : nullptr);
        return TExecQueryImpl::StreamExecuteQuery(
            Connections_, DbDriverState_, query, txControl, params, settings, session);
    }

    TAsyncExecuteQueryResult ExecuteQuery(const std::string& query, const TTxControl& txControl,
        const std::optional<TParams>& params, const TExecuteQuerySettings& settings,
        const std::optional<TSession>& session = {})
    {
        CollectQuerySize(query);
        CollectParamsSize(params ? &params->GetProtoMap() : nullptr);
        return TExecQueryImpl::ExecuteQuery(
            Connections_, DbDriverState_, query, txControl, params, settings, session);
    }

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const std::string& script, const std::optional<TParams>& params, const TExecuteScriptSettings& settings) {
        using namespace Ydb::Query;
        auto request = MakeOperationRequest<ExecuteScriptRequest>(settings);
        request.set_exec_mode(::Ydb::Query::ExecMode(settings.ExecMode_));
        request.set_stats_mode(::Ydb::Query::StatsMode(settings.StatsMode_));
        request.set_pool_id(TStringType{settings.ResourcePool_});
        request.mutable_script_content()->set_syntax(::Ydb::Query::Syntax(settings.Syntax_));
        request.mutable_script_content()->set_text(TStringType{script});
        SetDuration(settings.ResultsTtl_, *request.mutable_results_ttl());
    
        if (params) {
            *request.mutable_parameters() = params->GetProtoMap();
        }

        auto promise = NThreading::NewPromise<TScriptExecutionOperation>();

        auto responseCb = [promise]
            (Ydb::Operations::Operation* response, TPlainStatus status) mutable {
                try {
                    if (response) {
                        NYdb::NIssue::TIssues opIssues;
                        NYdb::NIssue::IssuesFromMessage(response->issues(), opIssues);
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
        request.set_operation_id(TStringType{operationId.ToString()});
        request.set_result_set_index(resultSetIndex);
        return FetchScriptResultsImpl(std::move(request), settings);
    }

    TAsyncStatus RollbackTransaction(const std::string& txId, const NYdb::NQuery::TRollbackTxSettings& settings, const TSession& session) {
        using namespace Ydb::Query;
        auto request = MakeRequest<Ydb::Query::RollbackTransactionRequest>();
        request.set_session_id(TStringType{session.GetId()});
        request.set_tx_id(TStringType{txId});

        auto promise = NThreading::NewPromise<TStatus>();

        auto responseCb = [promise, session]
            (Ydb::Query::RollbackTransactionResponse* response, TPlainStatus status) mutable {
                try {
                    if (response) {
                        NYdb::NIssue::TIssues opIssues;
                        NYdb::NIssue::IssuesFromMessage(response->issues(), opIssues);
                        TStatus rollbackTxStatus(TPlainStatus{static_cast<EStatus>(response->status()), std::move(opIssues),
                            status.Endpoint, std::move(status.Metadata)});

                        promise.SetValue(std::move(rollbackTxStatus));
                    } else {
                        promise.SetValue(TStatus(std::move(status)));
                    }
                } catch (...) {
                    promise.SetException(std::current_exception());
                }
            };

        Connections_->Run<V1::QueryService, RollbackTransactionRequest, RollbackTransactionResponse>(
            std::move(request),
            responseCb,
            &V1::QueryService::Stub::AsyncRollbackTransaction,
            DbDriverState_,
            TRpcRequestSettings::Make(settings, session.SessionImpl_->GetEndpointKey()));

        return promise.GetFuture();
    }

    TAsyncCommitTransactionResult CommitTransaction(const std::string& txId, const NYdb::NQuery::TCommitTxSettings& settings, const TSession& session) {
        using namespace Ydb::Query;
        auto request = MakeRequest<Ydb::Query::CommitTransactionRequest>();
        request.set_session_id(TStringType{session.GetId()});
        request.set_tx_id(TStringType{txId});

        auto promise = NThreading::NewPromise<TCommitTransactionResult>();

        auto responseCb = [promise, session]
            (Ydb::Query::CommitTransactionResponse* response, TPlainStatus status) mutable {
                try {
                    if (response) {
                        NYdb::NIssue::TIssues opIssues;
                        NYdb::NIssue::IssuesFromMessage(response->issues(), opIssues);
                        TStatus commitTxStatus(TPlainStatus{static_cast<EStatus>(response->status()), std::move(opIssues),
                            status.Endpoint, std::move(status.Metadata)});

                        TCommitTransactionResult commitTxResult(std::move(commitTxStatus));
                        promise.SetValue(std::move(commitTxResult));
                    } else {
                        promise.SetValue(TCommitTransactionResult(TStatus(std::move(status))));
                    }
                } catch (...) {
                    promise.SetException(std::current_exception());
                }
            };

        Connections_->Run<V1::QueryService, CommitTransactionRequest, CommitTransactionResponse>(
            std::move(request),
            responseCb,
            &V1::QueryService::Stub::AsyncCommitTransaction,
            DbDriverState_,
            TRpcRequestSettings::Make(settings, session.SessionImpl_->GetEndpointKey()));

        return promise.GetFuture();
    }

    TAsyncBeginTransactionResult BeginTransaction(const TTxSettings& txSettings,
        const TBeginTxSettings& settings, const TSession& session)
    {
        using namespace Ydb::Query;
        auto request = MakeRequest<Ydb::Query::BeginTransactionRequest>();
        request.set_session_id(TStringType{session.GetId()});
        SetTxSettings(txSettings, request.mutable_tx_settings());

        auto promise = NThreading::NewPromise<TBeginTransactionResult>();

        auto responseCb = [promise, session]
            (Ydb::Query::BeginTransactionResponse* response, TPlainStatus status) mutable {
                try {
                    if (response) {
                        NYdb::NIssue::TIssues opIssues;
                        NYdb::NIssue::IssuesFromMessage(response->issues(), opIssues);
                        TStatus beginTxStatus(TPlainStatus{static_cast<EStatus>(response->status()), std::move(opIssues),
                            status.Endpoint, std::move(status.Metadata)});

                        TBeginTransactionResult beginTxResult(std::move(beginTxStatus),
                            TTransaction(session, response->tx_meta().id()));
                        promise.SetValue(std::move(beginTxResult));
                    } else {
                        promise.SetValue(TBeginTransactionResult(
                            TStatus(std::move(status)), TTransaction(session, "")));
                    }
                } catch (...) {
                    promise.SetException(std::current_exception());
                }
            };

        Connections_->Run<V1::QueryService, BeginTransactionRequest, BeginTransactionResponse>(
            std::move(request),
            responseCb,
            &V1::QueryService::Stub::AsyncBeginTransaction,
            DbDriverState_,
            TRpcRequestSettings::Make(settings, session.SessionImpl_->GetEndpointKey()));

        return promise.GetFuture();
    }

    TAsyncFetchScriptResultsResult FetchScriptResultsImpl(Ydb::Query::FetchScriptResultsRequest&& request, const TFetchScriptResultsSettings& settings) {
        using namespace Ydb::Query;
        if (!settings.FetchToken_.empty()) {
            request.set_fetch_token(TStringType{settings.FetchToken_});
        }
        request.set_rows_limit(settings.RowsLimit_);

        auto promise = NThreading::NewPromise<TFetchScriptResultsResult>();

        auto extractor = [promise]
            (FetchScriptResultsResponse* response, TPlainStatus status) mutable {
                if (response) {
                    NYdb::NIssue::TIssues opIssues;
                    NYdb::NIssue::IssuesFromMessage(response->issues(), opIssues);
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
        Y_ABORT_UNLESS(sessionImpl->GetState() == TSession::TImpl::S_ACTIVE ||
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
        NThreading::TPromise<TCreateSessionResult> promise, const std::string& endpoint,
        std::shared_ptr<TQueryClient::TImpl> client)
    {
        using TStreamProcessorPtr = TSession::TImpl::TStreamProcessorPtr;
        Ydb::Query::AttachSessionRequest request;
        const auto sessionId = resp->session_id();
        request.set_session_id(sessionId);

        auto args = std::make_shared<TSession::TImpl::TAttachSessionArgs>(promise, sessionId, endpoint, client, client);

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
                args->Promise.SetValue(TCreateSessionResult(std::move(st), TSession(args->Client)));
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
                    NYdb::NIssue::TIssues opIssues;
                    NYdb::NIssue::IssuesFromMessage(resp->issues(), opIssues);
                    TStatus st(static_cast<EStatus>(resp->status()), std::move(opIssues));
                    promise.SetValue(TCreateSessionResult(std::move(st), TSession(self)));
                } else {
                    self->DoAttachSession(resp, promise, status.Endpoint, self);
                }
            } else {
                TStatus st(std::move(status));
                promise.SetValue(TCreateSessionResult(std::move(st), TSession(self)));
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

    int64_t GetActiveSessionCount() const {
        return SessionPool_.GetActiveSessions();
    }

    int64_t GetActiveSessionsLimit() const {
        return SessionPool_.GetActiveSessionsLimit();
    }

    int64_t GetCurrentPoolSize() const {
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

        std::weak_ptr<TQueryClient::TImpl> weak = shared_from_this();
        Connections_->AddPeriodicTask(
            SessionPool_.CreatePeriodicTask(
                weak,
                NSessionPool::TSessionPool::TKeepAliveCmd(), // no keep-alive cmd for query service
                std::move(deletePredicate)
            ), NSessionPool::PERIODIC_ACTION_INTERVAL);
    }

    void CollectRetryStatAsync(EStatus status) {
        RetryOperationStatCollector_.IncAsyncRetryOperation(status);
    }

    void CollectRetryStatSync(EStatus status) {
        RetryOperationStatCollector_.IncSyncRetryOperation(status);
    }

    void CollectQuerySize(const std::string& query) {
        if (QuerySizeHistogram_.IsCollecting()) {
            QuerySizeHistogram_.Record(query.size());
        }
    }

    void CollectParamsSize(const ::google::protobuf::Map<TStringType, Ydb::TypedValue>* params) {
        if (params && ParamsSizeHistogram_.IsCollecting()) {
            size_t size = 0;
            for (auto& keyvalue: *params) {
                size += keyvalue.second.ByteSizeLong();
            }
            ParamsSizeHistogram_.Record(size);
        }
    }

private:
    NSdkStats::TStatCollector::TClientRetryOperationStatCollector RetryOperationStatCollector_;
    NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> QuerySizeHistogram_;
    NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> ParamsSizeHistogram_;

    TClientSettings Settings_;
    NSessionPool::TSessionPool SessionPool_;
};

TQueryClient::TQueryClient(const TDriver& driver, const TClientSettings& settings)
    : Impl_(new TQueryClient::TImpl(CreateInternalInterface(driver), settings))
{
    Impl_->StartPeriodicSessionPoolTask();
}

TAsyncExecuteQueryResult TQueryClient::ExecuteQuery(const std::string& query, const TTxControl& txControl,
    const TExecuteQuerySettings& settings)
{
    return Impl_->ExecuteQuery(query, txControl, {}, settings);
}

TAsyncExecuteQueryResult TQueryClient::ExecuteQuery(const std::string& query, const TTxControl& txControl,
    const TParams& params, const TExecuteQuerySettings& settings)
{
    return Impl_->ExecuteQuery(query, txControl, params, settings);
}

TAsyncExecuteQueryIterator TQueryClient::StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
    const TExecuteQuerySettings& settings)
{
    return Impl_->StreamExecuteQuery(query, txControl, {}, settings);
}

TAsyncExecuteQueryIterator TQueryClient::StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
    const TParams& params, const TExecuteQuerySettings& settings)
{
    return Impl_->StreamExecuteQuery(query, txControl, params, settings);
}

NThreading::TFuture<TScriptExecutionOperation> TQueryClient::ExecuteScript(const std::string& script,
    const TExecuteScriptSettings& settings)
{
    return Impl_->ExecuteScript(script, {}, settings);
}

NThreading::TFuture<TScriptExecutionOperation> TQueryClient::ExecuteScript(const std::string& script,
    const TParams& params, const TExecuteScriptSettings& settings)
{
    return Impl_->ExecuteScript(script, params, settings);
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

int64_t TQueryClient::GetActiveSessionCount() const {
    return Impl_->GetActiveSessionCount();
}

int64_t TQueryClient::GetActiveSessionsLimit() const {
    return Impl_->GetActiveSessionsLimit();
}

int64_t TQueryClient::GetCurrentPoolSize() const {
    return Impl_->GetCurrentPoolSize();
}

TAsyncExecuteQueryResult TQueryClient::RetryQuery(TQueryResultFunc&& queryFunc, TRetryOperationSettings settings)
{
    TRetryContextResultAsync::TPtr ctx(new NRetry::Async::TRetryWithSession(*this, std::move(queryFunc), settings));
    return ctx->Execute();
}

TAsyncStatus TQueryClient::RetryQuery(TQueryFunc&& queryFunc, TRetryOperationSettings settings) {
    TRetryContextAsync::TPtr ctx(new NRetry::Async::TRetryWithSession(*this, std::move(queryFunc), settings));
    return ctx->Execute();
}

TAsyncStatus TQueryClient::RetryQuery(TQueryWithoutSessionFunc&& queryFunc, TRetryOperationSettings settings) {
    TRetryContextAsync::TPtr ctx(new NRetry::Async::TRetryWithoutSession(*this, std::move(queryFunc), settings));
    return ctx->Execute();
}

TStatus TQueryClient::RetryQuerySync(const TQuerySyncFunc& queryFunc, TRetryOperationSettings settings) {
    NRetry::Sync::TRetryWithSession ctx(*this, queryFunc, settings);
    return ctx.Execute();
}

TStatus TQueryClient::RetryQuerySync(const TQueryWithoutSessionSyncFunc& queryFunc, TRetryOperationSettings settings) {
    NRetry::Sync::TRetryWithoutSession ctx(*this, queryFunc, settings);
    return ctx.Execute();
}

TAsyncExecuteQueryResult TQueryClient::RetryQuery(const std::string& query, const TTxControl& txControl,
    TDuration timeout, bool isIndempotent)
{
    auto settings = GetRetrySettings(timeout, isIndempotent);
    auto queryFunc = [&query, &txControl](TSession session, TDuration duration) -> TAsyncExecuteQueryResult {
        return session.ExecuteQuery(query, txControl, TExecuteQuerySettings().ClientTimeout(duration));
    };
    TRetryContextResultAsync::TPtr ctx(new NRetry::Async::TRetryWithSession(*this, std::move(queryFunc), settings));
    return ctx->Execute();
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

TSession::TSession(std::shared_ptr<TQueryClient::TImpl> client)
    : Client_(client)
    , SessionImpl_(
        new TSession::TImpl(nullptr, "", "", client),
        TKqpSessionCommon::GetSmartDeleter(client)
    )
{}

TSession::TSession(std::shared_ptr<TQueryClient::TImpl> client, TSession::TImpl* session)
    : Client_(client)
    , SessionImpl_(session, TKqpSessionCommon::GetSmartDeleter(client))
{}

const std::string& TSession::GetId() const {
    return SessionImpl_->GetId();
}

TAsyncExecuteQueryResult TSession::ExecuteQuery(const std::string& query, const TTxControl& txControl,
    const TExecuteQuerySettings& settings)
{
    return NSessionPool::InjectSessionStatusInterception(
        SessionImpl_,
        Client_->ExecuteQuery(query, txControl, {}, settings, *this),
        true,
        Client_->Settings_.SessionPoolSettings_.CloseIdleThreshold_);
}

TAsyncExecuteQueryResult TSession::ExecuteQuery(const std::string& query, const TTxControl& txControl,
    const TParams& params, const TExecuteQuerySettings& settings)
{
    return NSessionPool::InjectSessionStatusInterception(
        SessionImpl_,
        Client_->ExecuteQuery(query, txControl, params, settings, *this),
        true,
        Client_->Settings_.SessionPoolSettings_.CloseIdleThreshold_);
}

TAsyncExecuteQueryIterator TSession::StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
    const TExecuteQuerySettings& settings)
{
    return NSessionPool::InjectSessionStatusInterception(
        SessionImpl_,
        Client_->StreamExecuteQuery(query, txControl, {}, settings, *this),
        true,
        Client_->Settings_.SessionPoolSettings_.CloseIdleThreshold_);
}

TAsyncExecuteQueryIterator TSession::StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
    const TParams& params, const TExecuteQuerySettings& settings)
{
    return NSessionPool::InjectSessionStatusInterception(
        SessionImpl_,
        Client_->StreamExecuteQuery(query, txControl, params, settings, *this),
        true,
        Client_->Settings_.SessionPoolSettings_.CloseIdleThreshold_);
}

TAsyncBeginTransactionResult TSession::BeginTransaction(const TTxSettings& txSettings,
        const TBeginTxSettings& settings)
{
    return NSessionPool::InjectSessionStatusInterception(
        SessionImpl_,
        Client_->BeginTransaction(txSettings, settings, *this),
        true,
        Client_->Settings_.SessionPoolSettings_.CloseIdleThreshold_);
}

TTransaction::TTransaction(const TSession& session, const std::string& txId)
    : Session_(session)
    , TxId_(txId)
{}

TAsyncCommitTransactionResult TTransaction::Commit(const NYdb::NQuery::TCommitTxSettings& settings) {
    return Session_.Client_->CommitTransaction(TxId_, settings, Session_);
}

TAsyncStatus TTransaction::Rollback(const TRollbackTxSettings& settings) {
    return Session_.Client_->RollbackTransaction(TxId_, settings, Session_);
}

TBeginTransactionResult::TBeginTransactionResult(TStatus&& status, TTransaction transaction)
    : TStatus(std::move(status))
    , Transaction_(transaction)
{}

const TTransaction& TBeginTransactionResult::GetTransaction() const {
    CheckStatusOk("TBeginTransactionResult::GetTransaction");
    return Transaction_;
}

const std::vector<TResultSet>& TExecuteQueryResult::GetResultSets() const {
    return ResultSets_;
}

TResultSet TExecuteQueryResult::GetResultSet(size_t resultIndex) const {
    if (resultIndex >= ResultSets_.size()) {
        RaiseError(std::string("Requested index out of range\n"));
    }

    return ResultSets_[resultIndex];
}

TResultSetParser TExecuteQueryResult::GetResultSetParser(size_t resultIndex) const {
    return TResultSetParser(GetResultSet(resultIndex));
}

} // namespace NYdb::NQuery
