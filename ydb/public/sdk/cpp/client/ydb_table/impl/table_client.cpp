#include "table_client.h"

namespace NYdb {

namespace NTable {

using namespace NThreading;

const TKeepAliveSettings TTableClient::TImpl::KeepAliveSettings = TKeepAliveSettings().ClientTimeout(KEEP_ALIVE_CLIENT_TIMEOUT);


TDuration GetMinTimeToTouch(const TSessionPoolSettings& settings) {
    return Min(settings.CloseIdleThreshold_, settings.KeepAliveIdleThreshold_);
}

TDuration GetMaxTimeToTouch(const TSessionPoolSettings& settings) {
    return Max(settings.CloseIdleThreshold_, settings.KeepAliveIdleThreshold_);
}

TTableClient::TImpl::TImpl(std::shared_ptr<TGRpcConnectionsImpl>&& connections, const TClientSettings& settings)
    : TClientImplCommon(std::move(connections), settings)
    , Settings_(settings)
    , SessionPool_(Settings_.SessionPoolSettings_.MaxActiveSessions_)
{
    if (!DbDriverState_->StatCollector.IsCollecting()) {
        return;
    }

    SetStatCollector(DbDriverState_->StatCollector.GetClientStatCollector());
    SessionPool_.SetStatCollector(DbDriverState_->StatCollector.GetSessionPoolStatCollector());
}

TTableClient::TImpl::~TImpl() {
    if (Connections_->GetDrainOnDtors()) {
        Drain().Wait();
    }
}

bool TTableClient::TImpl::LinkObjToEndpoint(const TEndpointKey& endpoint, TEndpointObj* obj, const void* tag) {
    return DbDriverState_->EndpointPool.LinkObjToEndpoint(endpoint, obj, tag);
}

void TTableClient::TImpl::InitStopper() {
    std::weak_ptr<TTableClient::TImpl> weak = shared_from_this();
    auto cb = [weak]() mutable {
        auto strong = weak.lock();
        if (!strong) {
            auto promise = NThreading::NewPromise<void>();
            promise.SetException("no more client");
            return promise.GetFuture();
        }
        return strong->Drain();
    };

    DbDriverState_->AddCb(std::move(cb), TDbDriverState::ENotifyType::STOP);
}

NThreading::TFuture<void> TTableClient::TImpl::Drain() {
    TVector<std::unique_ptr<TKqpSessionCommon>> sessions;
    // No realocations under lock
    sessions.reserve(Settings_.SessionPoolSettings_.MaxActiveSessions_);
    auto drainer = [&sessions](std::unique_ptr<TKqpSessionCommon>&& impl) mutable {
        sessions.push_back(std::move(impl));
        return true;
    };
    SessionPool_.Drain(drainer, true);
    TVector<TAsyncStatus> closeResults;
    for (auto& s : sessions) {
        if (s->GetId()) {
            closeResults.push_back(CloseInternal(s.get()));
        }
    }
    sessions.clear();
    return NThreading::WaitExceptionOrAll(closeResults);
}

NThreading::TFuture<void> TTableClient::TImpl::Stop() {
    return Drain();
}

void TTableClient::TImpl::ScheduleTaskUnsafe(std::function<void()>&& fn, TDuration timeout) {
    Connections_->ScheduleOneTimeTask(std::move(fn), timeout);
}

void TTableClient::TImpl::StartPeriodicSessionPoolTask() {
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

    auto keepAliveCmd = [this](TKqpSessionCommon* s) {
        auto strongClient = shared_from_this();
        TSession session(
            strongClient,
            std::shared_ptr<TSession::TImpl>(
                static_cast<TSession::TImpl*>(s),
                TSession::TImpl::GetSmartDeleter(strongClient)
            ));

        Y_ABORT_UNLESS(session.GetId());

        const auto sessionPoolSettings = session.Client_->Settings_.SessionPoolSettings_;
        const auto spentTime = session.SessionImpl_->GetTimeToTouchFast() - session.SessionImpl_->GetTimeInPastFast();

        const auto maxTimeToTouch = GetMaxTimeToTouch(session.Client_->Settings_.SessionPoolSettings_);
        const auto minTimeToTouch = GetMinTimeToTouch(session.Client_->Settings_.SessionPoolSettings_);

        auto calcTimeToNextTouch = [maxTimeToTouch, minTimeToTouch] (const TDuration spent) {
            auto timeToNextTouch = minTimeToTouch;
            if (maxTimeToTouch > spent) {
                auto t = maxTimeToTouch - spent;
                timeToNextTouch = Min(t, minTimeToTouch);
            }
            return timeToNextTouch;
        };

        if (spentTime >= sessionPoolSettings.KeepAliveIdleThreshold_) {

            // Handle of session status will be done inside InjectSessionStatusInterception routine.
            // We just need to reschedule time to next call because InjectSessionStatusInterception doesn't
            // update timeInPast for calls from internal keep alive routine
            session.KeepAlive(KeepAliveSettings)
                .Subscribe([spentTime, session, maxTimeToTouch, calcTimeToNextTouch](TAsyncKeepAliveResult asyncResult) {
                    if (!asyncResult.GetValue().IsSuccess())
                        return;

                    if (spentTime >= maxTimeToTouch) {
                        auto timeToNextTouch = calcTimeToNextTouch(spentTime);
                        session.SessionImpl_->ScheduleTimeToTouchFast(timeToNextTouch, true);
                    }
                });
            return;
        }

        auto timeToNextTouch = calcTimeToNextTouch(spentTime);
        session.SessionImpl_->ScheduleTimeToTouchFast(
            NSessionPool::RandomizeThreshold(timeToNextTouch),
            spentTime >= maxTimeToTouch
        );
    };

    std::weak_ptr<TTableClient::TImpl> weak = shared_from_this();
    Connections_->AddPeriodicTask(
        SessionPool_.CreatePeriodicTask(
            weak,
            std::move(keepAliveCmd),
            std::move(deletePredicate)
        ), NSessionPool::PERIODIC_ACTION_INTERVAL);
}

ui64 TTableClient::TImpl::ScanForeignLocations(std::shared_ptr<TTableClient::TImpl> client) {
    size_t max = 0;
    ui64 result = 0;

    auto cb = [&result, &max](ui64 nodeId, const IObjRegistryHandle& handle) {
        const auto sz = handle.Size();
        if (sz > max) {
            result = nodeId;
            max = sz;
        }
    };

    client->DbDriverState_->ForEachForeignEndpoint(cb, client.get());

    return result;
}

std::pair<ui64, size_t> TTableClient::TImpl::ScanLocation(std::shared_ptr<TTableClient::TImpl> client,
    std::unordered_map<ui64, size_t>& sessions, bool allNodes)
{
    std::pair<ui64, size_t> result = {0, 0};

    auto cb = [&result, &sessions](ui64 nodeId, const IObjRegistryHandle& handle) {
        const auto sz = handle.Size();
        sessions.insert({nodeId, sz});
        if (sz > result.second) {
            result.first = nodeId;
            result.second = sz;
        }
    };

    if (allNodes) {
        client->DbDriverState_->ForEachEndpoint(cb, client.get());
    } else {
        client->DbDriverState_->ForEachLocalEndpoint(cb, client.get());
    }

    return result;
}

NMath::TStats TTableClient::TImpl::CalcCV(const std::unordered_map<ui64, size_t>& in) {
    TVector<size_t> t;
    t.reserve(in.size());
    std::transform(in.begin(), in.end(), std::back_inserter(t), [](const std::pair<ui64, size_t>& pair) {
        return pair.second;
    });
    return NMath::CalcCV(t);
}

void TTableClient::TImpl::StartPeriodicHostScanTask() {
    std::weak_ptr<TTableClient::TImpl> weak = shared_from_this();

    // The future in completed when we have finished current migrate task
    // and ready to accept new one
    std::pair<ui64, size_t> winner = {0, 0};

    auto periodicCb = [weak, winner](NYql::TIssues&&, EStatus status) mutable -> bool {

        if (status != EStatus::SUCCESS) {
            return false;
        }

        auto strongClient = weak.lock();
        if (!strongClient) {
            return false;
        } else {
            TRequestMigrator& migrator = strongClient->RequestMigrator_;

            const auto balancingPolicy = strongClient->DbDriverState_->GetBalancingPolicy();

            // Try to find any host at foreign locations if prefer local dc
            const ui64 foreignHost = (balancingPolicy == EBalancingPolicy::UsePreferableLocation) ?
                ScanForeignLocations(strongClient) : 0;

            std::unordered_map<ui64, size_t> hostMap;

            winner = ScanLocation(strongClient, hostMap,
            balancingPolicy == EBalancingPolicy::UseAllNodes);

            bool forceMigrate = false;

            // There is host in foreign locations
            if (foreignHost) {
                // But no hosts at local
                if (hostMap.empty()) {
                    Y_ABORT_UNLESS(!winner.second);
                    // Scan whole cluster - we have no local dc
                    winner = ScanLocation(strongClient, hostMap, true);
                } else {
                    // We have local and foreign hosts, so force migration to local one
                    forceMigrate = true;
                    // Just replace source
                    winner.first = foreignHost;
                    winner.second++;
                }
            }

            const auto minCv = strongClient->Settings_.MinSessionCV_;

            const auto stats = CalcCV(hostMap);

            strongClient->DbDriverState_->StatCollector.SetSessionCV(stats.Cv);

            // Just scan to update monitoring counter ^^
            // Balancing feature is disabled.
            if (!minCv)
                return true;

            if (hostMap.size() < 2)
                return true;

            // Migrate last session only if move from foreign to local
            if (!forceMigrate && winner.second < 2)
                return true;

            if (stats.Cv > minCv || forceMigrate) {
                migrator.SetHost(winner.first);
            } else {
                migrator.SetHost(0);
            }
            return true;
        }
    };

    Connections_->AddPeriodicTask(std::move(periodicCb), HOSTSCAN_PERIODIC_ACTION_INTERVAL);
}

TAsyncCreateSessionResult TTableClient::TImpl::GetSession(const TCreateSessionSettings& settings) {
    using namespace NSessionPool;

    class TTableClientGetSessionCtx : public IGetSessionCtx {
    public:
        TTableClientGetSessionCtx(std::shared_ptr<TTableClient::TImpl> client, TDuration clientTimeout)
            : Promise(NewPromise<TCreateSessionResult>())
            , Client(client)
            , ClientTimeout(clientTimeout)
        {}

        TAsyncCreateSessionResult GetFuture() {
            return Promise.GetFuture();
        }

        void ReplyError(TStatus status) override {
            TSession session(Client, "", "", false);
            ScheduleReply(TCreateSessionResult(std::move(status), std::move(session)));
        }

        void ReplySessionToUser(TKqpSessionCommon* session) override {
            TCreateSessionResult val(
                TStatus(TPlainStatus()),
                TSession(
                    Client,
                    std::shared_ptr<TSession::TImpl>(
                        static_cast<TSession::TImpl*>(session),
                        TSession::TImpl::GetSmartDeleter(Client)
                    )
                )
            );

            ScheduleReply(std::move(val));
        }

        void ReplyNewSession() override {
            TCreateSessionSettings settings;
            settings.ClientTimeout(ClientTimeout);
            const auto& sessionResult = Client->CreateSession(settings, false);
            sessionResult.Subscribe(TSession::TImpl::GetSessionInspector(Promise, Client, settings, 0, true));
        }

    private:
        void ScheduleReply(TCreateSessionResult val) {
            //TODO: Do we realy need it?
            Client->ScheduleTaskUnsafe([promise{std::move(Promise)}, val{std::move(val)}]() mutable {
                promise.SetValue(std::move(val));
            }, TDuration());
        }
        NThreading::TPromise<TCreateSessionResult> Promise;
        std::shared_ptr<TTableClient::TImpl> Client;
        const TDuration ClientTimeout;
    };

    auto ctx = std::make_unique<TTableClientGetSessionCtx>(shared_from_this(), settings.ClientTimeout_);
    auto future = ctx->GetFuture();
    SessionPool_.GetSession(std::move(ctx));
    return future;
}

i64 TTableClient::TImpl::GetActiveSessionCount() const {
    return SessionPool_.GetActiveSessions();
}

i64 TTableClient::TImpl::GetActiveSessionsLimit() const {
    return SessionPool_.GetActiveSessionsLimit();
}

i64 TTableClient::TImpl::GetCurrentPoolSize() const {
    return SessionPool_.GetCurrentPoolSize();
}

TAsyncCreateSessionResult TTableClient::TImpl::CreateSession(const TCreateSessionSettings& settings, bool standalone,
    TString preferredLocation)
{
    auto request = MakeOperationRequest<Ydb::Table::CreateSessionRequest>(settings);

    auto createSessionPromise = NewPromise<TCreateSessionResult>();
    auto self = shared_from_this();
    auto rpcSettings = TRpcRequestSettings::Make(settings, TEndpointKey(preferredLocation, 0));
    rpcSettings.Header.push_back({NYdb::YDB_CLIENT_CAPABILITIES, NYdb::YDB_CLIENT_CAPABILITY_SESSION_BALANCER});

    auto createSessionExtractor = [createSessionPromise, self, standalone]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Table::CreateSessionResult result;
            if (any) {
                any->UnpackTo(&result);
            }
            auto session = TSession(self, result.session_id(), status.Endpoint, !standalone);
            if (status.Ok()) {
                if (!standalone) {
                    session.SessionImpl_->MarkActive();
                }
                self->DbDriverState_->StatCollector.IncSessionsOnHost(status.Endpoint);
            } else {
                // We do not use SessionStatusInterception for CreateSession request
                session.SessionImpl_->MarkBroken();
            }
            TCreateSessionResult val(TStatus(std::move(status)), std::move(session));
            createSessionPromise.SetValue(std::move(val));
        };

    Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::CreateSessionRequest, Ydb::Table::CreateSessionResponse>(
        std::move(request),
        createSessionExtractor,
        &Ydb::Table::V1::TableService::Stub::AsyncCreateSession,
        DbDriverState_,
        INITIAL_DEFERRED_CALL_DELAY,
        rpcSettings);

    std::weak_ptr<TDbDriverState> state = DbDriverState_;

    return createSessionPromise.GetFuture();
}

TAsyncKeepAliveResult TTableClient::TImpl::KeepAlive(const TSession::TImpl* session, const TKeepAliveSettings& settings) {
    auto request = MakeOperationRequest<Ydb::Table::KeepAliveRequest>(settings);
    request.set_session_id(session->GetId());

    auto keepAliveResultPromise = NewPromise<TKeepAliveResult>();
    auto self = shared_from_this();

    auto keepAliveExtractor = [keepAliveResultPromise, self]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Table::KeepAliveResult result;
            ESessionStatus sessionStatus = ESessionStatus::Unspecified;
            if (any) {
                any->UnpackTo(&result);

                switch (result.session_status()) {
                    case Ydb::Table::KeepAliveResult_SessionStatus_SESSION_STATUS_READY:
                        sessionStatus = ESessionStatus::Ready;
                    break;
                    case Ydb::Table::KeepAliveResult_SessionStatus_SESSION_STATUS_BUSY:
                        sessionStatus = ESessionStatus::Busy;
                    break;
                    default:
                        sessionStatus = ESessionStatus::Unspecified;
                }
            }
            TKeepAliveResult val(TStatus(std::move(status)), sessionStatus);
            keepAliveResultPromise.SetValue(std::move(val));
        };

    Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::KeepAliveRequest, Ydb::Table::KeepAliveResponse>(
        std::move(request),
        keepAliveExtractor,
        &Ydb::Table::V1::TableService::Stub::AsyncKeepAlive,
        DbDriverState_,
        INITIAL_DEFERRED_CALL_DELAY,
        TRpcRequestSettings::Make(settings, session->GetEndpointKey())
        );

    return keepAliveResultPromise.GetFuture();
}

TFuture<TStatus> TTableClient::TImpl::CreateTable(Ydb::Table::CreateTableRequest&& request, const TCreateTableSettings& settings)
{
    return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::CreateTableRequest,Ydb::Table::CreateTableResponse>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncCreateTable,
        TRpcRequestSettings::Make(settings));
}

TFuture<TStatus> TTableClient::TImpl::AlterTable(Ydb::Table::AlterTableRequest&& request, const TAlterTableSettings& settings)
{
    return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::AlterTableRequest, Ydb::Table::AlterTableResponse>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncAlterTable,
        TRpcRequestSettings::Make(settings));
}

TAsyncOperation TTableClient::TImpl::AlterTableLong(Ydb::Table::AlterTableRequest&& request, const TAlterTableSettings& settings)
{
    using Ydb::Table::V1::TableService;
    using Ydb::Table::AlterTableRequest;
    using Ydb::Table::AlterTableResponse;
    return RunOperation<TableService, AlterTableRequest, AlterTableResponse, TOperation>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncAlterTable,
        TRpcRequestSettings::Make(settings));
}

TFuture<TStatus> TTableClient::TImpl::CopyTable(const TString& sessionId, const TString& src, const TString& dst,
    const TCopyTableSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::CopyTableRequest>(settings);
    request.set_session_id(sessionId);
    request.set_source_path(src);
    request.set_destination_path(dst);

    return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::CopyTableRequest, Ydb::Table::CopyTableResponse>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncCopyTable,
        TRpcRequestSettings::Make(settings));
}

TFuture<TStatus> TTableClient::TImpl::CopyTables(Ydb::Table::CopyTablesRequest&& request, const TCopyTablesSettings& settings)
{
    return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::CopyTablesRequest, Ydb::Table::CopyTablesResponse>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncCopyTables,
        TRpcRequestSettings::Make(settings));
}

TFuture<TStatus> TTableClient::TImpl::RenameTables(Ydb::Table::RenameTablesRequest&& request, const TRenameTablesSettings& settings)
{
    return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::RenameTablesRequest, Ydb::Table::RenameTablesResponse>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncRenameTables,
        TRpcRequestSettings::Make(settings));
}

TFuture<TStatus> TTableClient::TImpl::DropTable(const TString& sessionId, const TString& path, const TDropTableSettings& settings) {
    auto request = MakeOperationRequest<Ydb::Table::DropTableRequest>(settings);
    request.set_session_id(sessionId);
    request.set_path(path);

    return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::DropTableRequest, Ydb::Table::DropTableResponse>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncDropTable,
        TRpcRequestSettings::Make(settings));
}

TAsyncDescribeTableResult TTableClient::TImpl::DescribeTable(const TString& sessionId, const TString& path, const TDescribeTableSettings& settings) {
    auto request = MakeOperationRequest<Ydb::Table::DescribeTableRequest>(settings);
    request.set_session_id(sessionId);
    request.set_path(path);

    if (settings.WithKeyShardBoundary_) {
        request.set_include_shard_key_bounds(true);
    }
    if (settings.WithIndexTableKeyShardBoundary_) {
        request.set_include_index_table_shard_key_bounds(true);
    }
    if (settings.WithTableStatistics_) {
        request.set_include_table_stats(true);
    }
    if (settings.WithPartitionStatistics_) {
        request.set_include_partition_stats(true);
    }

    auto promise = NewPromise<TDescribeTableResult>();

    auto extractor = [promise, settings]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            Ydb::Table::DescribeTableResult result;
            if (any) {
                any->UnpackTo(&result);
            }
            TDescribeTableResult describeTableResult(TStatus(std::move(status)),
                std::move(result), settings);
            promise.SetValue(std::move(describeTableResult));
        };

    Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::DescribeTableRequest, Ydb::Table::DescribeTableResponse>(
        std::move(request),
        extractor,
        &Ydb::Table::V1::TableService::Stub::AsyncDescribeTable,
        DbDriverState_,
        INITIAL_DEFERRED_CALL_DELAY,
        TRpcRequestSettings::Make(settings));

    return promise.GetFuture();
}

TAsyncPrepareQueryResult TTableClient::TImpl::PrepareDataQuery(const TSession& session, const TString& query,
    const TPrepareDataQuerySettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::PrepareDataQueryRequest>(settings);
    request.set_session_id(session.GetId());
    request.set_yql_text(query);

    auto promise = NewPromise<TPrepareQueryResult>();

    //See ExecuteDataQueryInternal for explanation
    auto sessionPtr = new TSession(session);
    auto extractor = [promise, sessionPtr, query]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            TDataQuery dataQuery(*sessionPtr, query, "");

            if (any) {
                Ydb::Table::PrepareQueryResult result;
                any->UnpackTo(&result);

                if (status.Ok()) {
                    dataQuery = TDataQuery(*sessionPtr, query, result.query_id(), result.parameters_types());
                    sessionPtr->SessionImpl_->AddQueryToCache(dataQuery);
                }
            }

            TPrepareQueryResult prepareQueryResult(TStatus(std::move(status)),
                dataQuery, false);
            delete sessionPtr;
            promise.SetValue(std::move(prepareQueryResult));
        };

    CollectQuerySize(query, QuerySizeHistogram);

    Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::PrepareDataQueryRequest, Ydb::Table::PrepareDataQueryResponse>(
        std::move(request),
        extractor,
        &Ydb::Table::V1::TableService::Stub::AsyncPrepareDataQuery,
        DbDriverState_,
        INITIAL_DEFERRED_CALL_DELAY,
        TRpcRequestSettings::Make(settings, session.SessionImpl_->GetEndpointKey())
        );

    return promise.GetFuture();
}

TAsyncStatus TTableClient::TImpl::ExecuteSchemeQuery(const TString& sessionId, const TString& query,
    const TExecSchemeQuerySettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::ExecuteSchemeQueryRequest>(settings);
    request.set_session_id(sessionId);
    request.set_yql_text(query);

    return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::ExecuteSchemeQueryRequest, Ydb::Table::ExecuteSchemeQueryResponse>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncExecuteSchemeQuery,
        TRpcRequestSettings::Make(settings));
}

TAsyncBeginTransactionResult TTableClient::TImpl::BeginTransaction(const TSession& session, const TTxSettings& txSettings,
    const TBeginTxSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::BeginTransactionRequest>(settings);
    request.set_session_id(session.GetId());
    SetTxSettings(txSettings, request.mutable_tx_settings());

    auto promise = NewPromise<TBeginTransactionResult>();

    auto extractor = [promise, session]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            TString txId;
            if (any) {
                Ydb::Table::BeginTransactionResult result;
                any->UnpackTo(&result);
                txId = result.tx_meta().id();
            }

            TBeginTransactionResult beginTxResult(TStatus(std::move(status)),
                TTransaction(session, txId));
            promise.SetValue(std::move(beginTxResult));
        };

    Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::BeginTransactionRequest, Ydb::Table::BeginTransactionResponse>(
        std::move(request),
        extractor,
        &Ydb::Table::V1::TableService::Stub::AsyncBeginTransaction,
        DbDriverState_,
        INITIAL_DEFERRED_CALL_DELAY,
        TRpcRequestSettings::Make(settings, session.SessionImpl_->GetEndpointKey())
        );

    return promise.GetFuture();
}

TAsyncCommitTransactionResult TTableClient::TImpl::CommitTransaction(const TSession& session, const TTransaction& tx,
    const TCommitTxSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::CommitTransactionRequest>(settings);
    request.set_session_id(session.GetId());
    request.set_tx_id(tx.GetId());
    request.set_collect_stats(GetStatsCollectionMode(settings.CollectQueryStats_));

    auto promise = NewPromise<TCommitTransactionResult>();

    auto extractor = [promise]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            TMaybe<TQueryStats> queryStats;
            if (any) {
                Ydb::Table::CommitTransactionResult result;
                any->UnpackTo(&result);

                if (result.has_query_stats()) {
                    queryStats = TQueryStats(result.query_stats());
                }
            }

            TCommitTransactionResult commitTxResult(TStatus(std::move(status)), queryStats);
            promise.SetValue(std::move(commitTxResult));
        };

    Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::CommitTransactionRequest, Ydb::Table::CommitTransactionResponse>(
        std::move(request),
        extractor,
        &Ydb::Table::V1::TableService::Stub::AsyncCommitTransaction,
        DbDriverState_,
        INITIAL_DEFERRED_CALL_DELAY,
        TRpcRequestSettings::Make(settings, session.SessionImpl_->GetEndpointKey())
        );

    return promise.GetFuture();
}

TAsyncStatus TTableClient::TImpl::RollbackTransaction(const TSession& session, const TTransaction& tx,
    const TRollbackTxSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::RollbackTransactionRequest>(settings);
    request.set_session_id(session.GetId());
    request.set_tx_id(tx.GetId());

    return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::RollbackTransactionRequest, Ydb::Table::RollbackTransactionResponse>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncRollbackTransaction,
        TRpcRequestSettings::Make(settings, session.SessionImpl_->GetEndpointKey())
        );
}

TAsyncExplainDataQueryResult TTableClient::TImpl::ExplainDataQuery(const TSession& session, const TString& query,
    const TExplainDataQuerySettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::ExplainDataQueryRequest>(settings);
    request.set_session_id(session.GetId());
    request.set_yql_text(query);
    request.set_collect_full_diagnostics(settings.WithCollectFullDiagnostics_);

    auto promise = NewPromise<TExplainQueryResult>();

    auto extractor = [promise]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            TString ast;
            TString plan;
            TString diagnostics;
            if (any) {
                Ydb::Table::ExplainQueryResult result;
                any->UnpackTo(&result);
                ast = result.query_ast();
                plan = result.query_plan();
                diagnostics = result.query_full_diagnostics();
            }
            TExplainQueryResult val(TStatus(std::move(status)),
                std::move(plan), std::move(ast), std::move(diagnostics));
            promise.SetValue(std::move(val));
        };

    Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::ExplainDataQueryRequest, Ydb::Table::ExplainDataQueryResponse>(
        std::move(request),
        extractor,
        &Ydb::Table::V1::TableService::Stub::AsyncExplainDataQuery,
        DbDriverState_,
        INITIAL_DEFERRED_CALL_DELAY,
        TRpcRequestSettings::Make(settings, session.SessionImpl_->GetEndpointKey())
        );

    return promise.GetFuture();
}

void TTableClient::TImpl::SetTypedValue(Ydb::TypedValue* protoValue, const TValue& value) {
    protoValue->mutable_type()->CopyFrom(TProtoAccessor::GetProto(value.GetType()));
    protoValue->mutable_value()->CopyFrom(TProtoAccessor::GetProto(value));
}

NThreading::TFuture<std::pair<TPlainStatus, TTableClient::TImpl::TReadTableStreamProcessorPtr>> TTableClient::TImpl::ReadTable(
    const TString& sessionId,
    const TString& path,
    const TReadTableSettings& settings)
{
    auto request = MakeRequest<Ydb::Table::ReadTableRequest>();
    request.set_session_id(sessionId);
    request.set_path(path);
    request.set_ordered(settings.Ordered_);
    if (settings.RowLimit_) {
        request.set_row_limit(settings.RowLimit_.GetRef());
    }
    for (const auto& col : settings.Columns_) {
        request.add_columns(col);
    }
    if (settings.UseSnapshot_) {
        request.set_use_snapshot(
            settings.UseSnapshot_.GetRef()
            ? Ydb::FeatureFlag::ENABLED
            : Ydb::FeatureFlag::DISABLED);
    }

    if (settings.From_) {
        const auto& from = settings.From_.GetRef();
        if (from.IsInclusive()) {
            SetTypedValue(request.mutable_key_range()->mutable_greater_or_equal(), from.GetValue());
        } else {
            SetTypedValue(request.mutable_key_range()->mutable_greater(), from.GetValue());
        }
    }

    if (settings.To_) {
        const auto& to = settings.To_.GetRef();
        if (to.IsInclusive()) {
            SetTypedValue(request.mutable_key_range()->mutable_less_or_equal(), to.GetValue());
        } else {
            SetTypedValue(request.mutable_key_range()->mutable_less(), to.GetValue());
        }
    }

    if (settings.BatchLimitBytes_) {
        request.set_batch_limit_bytes(*settings.BatchLimitBytes_);
    }

    if (settings.BatchLimitRows_) {
        request.set_batch_limit_rows(*settings.BatchLimitRows_);
    }

    if (settings.ReturnNotNullAsOptional_) {
        request.set_return_not_null_data_as_optional(
            settings.ReturnNotNullAsOptional_.GetRef()
            ? Ydb::FeatureFlag::ENABLED
            : Ydb::FeatureFlag::DISABLED);
    }

    auto promise = NewPromise<std::pair<TPlainStatus, TReadTableStreamProcessorPtr>>();

    Connections_->StartReadStream<Ydb::Table::V1::TableService, Ydb::Table::ReadTableRequest, Ydb::Table::ReadTableResponse>(
        std::move(request),
        [promise] (TPlainStatus status, TReadTableStreamProcessorPtr processor) mutable {
            promise.SetValue(std::make_pair(status, processor));
        },
        &Ydb::Table::V1::TableService::Stub::AsyncStreamReadTable,
        DbDriverState_,
        TRpcRequestSettings::Make(settings));

    return promise.GetFuture();

}

TAsyncReadRowsResult TTableClient::TImpl::ReadRows(const TString& path, TValue&& keys, const TVector<TString>& columns, const TReadRowsSettings& settings) {
    auto request = MakeRequest<Ydb::Table::ReadRowsRequest>();
    request.set_path(path);
    auto* protoKeys = request.mutable_keys();
    *protoKeys->mutable_type() = TProtoAccessor::GetProto(keys.GetType());
    *protoKeys->mutable_value() = TProtoAccessor::GetProto(keys);
    auto* protoColumns = request.mutable_columns();
    for (const auto& column: columns)
    {
        *protoColumns->Add() = column;
    }

    auto promise = NewPromise<TReadRowsResult>();

    auto responseCb = [promise] (Ydb::Table::ReadRowsResponse* response, TPlainStatus status) mutable {
        Ydb::ResultSet resultSet;
        // if there is no response status contains transport errors
        if (response) {
            Ydb::StatusIds::StatusCode msgStatus = response->status();
            NYql::TIssues issues;
            NYql::IssuesFromMessage(response->issues(), issues);
            status = TPlainStatus(static_cast<EStatus>(msgStatus), std::move(issues));
            resultSet = std::move(*response->mutable_result_set());
        }
        TReadRowsResult val(TStatus(std::move(status)), std::move(resultSet));
        promise.SetValue(std::move(val));
    };

    Connections_->Run<Ydb::Table::V1::TableService, Ydb::Table::ReadRowsRequest, Ydb::Table::ReadRowsResponse>(
        std::move(request),
        responseCb,
        &Ydb::Table::V1::TableService::Stub::AsyncReadRows,
        DbDriverState_,
        TRpcRequestSettings::Make(settings)
        );

    return promise.GetFuture();
}

TAsyncStatus TTableClient::TImpl::Close(const TKqpSessionCommon* sessionImpl, const TCloseSessionSettings& settings) {
    auto request = MakeOperationRequest<Ydb::Table::DeleteSessionRequest>(settings);
    request.set_session_id(sessionImpl->GetId());
    return RunSimple<Ydb::Table::V1::TableService, Ydb::Table::DeleteSessionRequest, Ydb::Table::DeleteSessionResponse>(
        std::move(request),
        &Ydb::Table::V1::TableService::Stub::AsyncDeleteSession,
        TRpcRequestSettings::Make(settings, sessionImpl->GetEndpointKey())
        );
}

TAsyncStatus TTableClient::TImpl::CloseInternal(const TKqpSessionCommon* sessionImpl) {
    static const auto internalCloseSessionSettings = TCloseSessionSettings()
            .ClientTimeout(TDuration::Seconds(2));

    auto driver = Connections_;
    return Close(sessionImpl, internalCloseSessionSettings)
        .Apply([driver{std::move(driver)}](TAsyncStatus status) mutable
        {
            driver.reset();
            return status;
        });
}

bool TTableClient::TImpl::ReturnSession(TKqpSessionCommon* sessionImpl) {
    Y_ABORT_UNLESS(sessionImpl->GetState() == TSession::TImpl::S_ACTIVE ||
            sessionImpl->GetState() == TSession::TImpl::S_IDLE);

    if (RequestMigrator_.DoCheckAndMigrate(sessionImpl)) {
        SessionRemovedDueBalancing.Inc();
        return false;
    }

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

void TTableClient::TImpl::DeleteSession(TKqpSessionCommon* sessionImpl) {
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

    if (sessionImpl->GetId()) {
        CloseInternal(sessionImpl);
        DbDriverState_->StatCollector.DecSessionsOnHost(sessionImpl->GetEndpoint());
    }

    delete sessionImpl;
}

ui32 TTableClient::TImpl::GetSessionRetryLimit() const {
    return Settings_.SessionPoolSettings_.RetryLimit_;
}

void TTableClient::TImpl::SetStatCollector(const NSdkStats::TStatCollector::TClientStatCollector& collector) {
    CacheMissCounter.Set(collector.CacheMiss);
    QuerySizeHistogram.Set(collector.QuerySize);
    ParamsSizeHistogram.Set(collector.ParamsSize);
    RetryOperationStatCollector = collector.RetryOperationStatCollector;
    SessionRemovedDueBalancing.Set(collector.SessionRemovedDueBalancing);
    RequestMigrated.Set(collector.RequestMigrated);
}

TAsyncBulkUpsertResult TTableClient::TImpl::BulkUpsert(const TString& table, TValue&& rows, const TBulkUpsertSettings& settings) {
    auto request = MakeOperationRequest<Ydb::Table::BulkUpsertRequest>(settings);
    request.set_table(table);
    *request.mutable_rows()->mutable_type() = TProtoAccessor::GetProto(rows.GetType());
    *request.mutable_rows()->mutable_value() = rows.GetProto();

    auto promise = NewPromise<TBulkUpsertResult>();

    auto extractor = [promise]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            Y_UNUSED(any);
            TBulkUpsertResult val(TStatus(std::move(status)));
            promise.SetValue(std::move(val));
        };

    Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>(
        std::move(request),
        extractor,
        &Ydb::Table::V1::TableService::Stub::AsyncBulkUpsert,
        DbDriverState_,
        INITIAL_DEFERRED_CALL_DELAY,
        TRpcRequestSettings::Make(settings));

    return promise.GetFuture();
}

TAsyncBulkUpsertResult TTableClient::TImpl::BulkUpsert(const TString& table, EDataFormat format,
    const TString& data, const TString& schema, const TBulkUpsertSettings& settings)
{
    auto request = MakeOperationRequest<Ydb::Table::BulkUpsertRequest>(settings);
    request.set_table(table);
    if (format == EDataFormat::ApacheArrow) {
        request.mutable_arrow_batch_settings()->set_schema(schema);
    } else if (format == EDataFormat::CSV) {
        auto* csv_settings = request.mutable_csv_settings();
        const auto& format_settings = settings.FormatSettings_;
        if (!format_settings.empty()) {
            bool ok = csv_settings->ParseFromString(format_settings);
            if (!ok) {
                return {};
            }
        }
    }
    request.set_data(data);

    auto promise = NewPromise<TBulkUpsertResult>();

    auto extractor = [promise]
        (google::protobuf::Any* any, TPlainStatus status) mutable {
            Y_UNUSED(any);
            TBulkUpsertResult val(TStatus(std::move(status)));
            promise.SetValue(std::move(val));
        };

    Connections_->RunDeferred<Ydb::Table::V1::TableService, Ydb::Table::BulkUpsertRequest, Ydb::Table::BulkUpsertResponse>(
        std::move(request),
        extractor,
        &Ydb::Table::V1::TableService::Stub::AsyncBulkUpsert,
        DbDriverState_,
        INITIAL_DEFERRED_CALL_DELAY,
        TRpcRequestSettings::Make(settings));

    return promise.GetFuture();
}

TFuture<std::pair<TPlainStatus, TTableClient::TImpl::TScanQueryProcessorPtr>> TTableClient::TImpl::StreamExecuteScanQueryInternal(const TString& query,
    const ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
    const TStreamExecScanQuerySettings& settings)
{
    auto request = MakeRequest<Ydb::Table::ExecuteScanQueryRequest>();
    request.mutable_query()->set_yql_text(query);

    if (params) {
        *request.mutable_parameters() = *params;
    }

    if (settings.Explain_) {
        request.set_mode(Ydb::Table::ExecuteScanQueryRequest::MODE_EXPLAIN);
    } else {
        request.set_mode(Ydb::Table::ExecuteScanQueryRequest::MODE_EXEC);
    }

    request.set_collect_stats(GetStatsCollectionMode(settings.CollectQueryStats_));
    request.set_collect_full_diagnostics(settings.CollectFullDiagnostics_);

    auto promise = NewPromise<std::pair<TPlainStatus, TScanQueryProcessorPtr>>();

    Connections_->StartReadStream<
        Ydb::Table::V1::TableService,
        Ydb::Table::ExecuteScanQueryRequest,
        Ydb::Table::ExecuteScanQueryPartialResponse>
    (
        std::move(request),
        [promise] (TPlainStatus status, TScanQueryProcessorPtr processor) mutable {
            promise.SetValue(std::make_pair(status, processor));
        },
        &Ydb::Table::V1::TableService::Stub::AsyncStreamExecuteScanQuery,
        DbDriverState_,
        TRpcRequestSettings::Make(settings)
    );

    return promise.GetFuture();
}

TAsyncScanQueryPartIterator TTableClient::TImpl::StreamExecuteScanQuery(const TString& query,
    const ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
    const TStreamExecScanQuerySettings& settings)
{
    auto promise = NewPromise<TScanQueryPartIterator>();

    auto iteratorCallback = [promise](TFuture<std::pair<TPlainStatus,
        TTableClient::TImpl::TScanQueryProcessorPtr>> future) mutable
    {
        Y_ASSERT(future.HasValue());
        auto pair = future.ExtractValue();
        promise.SetValue(TScanQueryPartIterator(
            pair.second
                ? std::make_shared<TScanQueryPartIterator::TReaderImpl>(pair.second, pair.first.Endpoint)
                : nullptr,
            std::move(pair.first))
        );
    };

    StreamExecuteScanQueryInternal(query, params, settings).Subscribe(iteratorCallback);
    return promise.GetFuture();
}

// void TTableClient::TImpl::CloseAndDeleteSession(
//     std::unique_ptr<TSession::TImpl>&& impl,
//     std::shared_ptr<TTableClient::TImpl> client);


void TTableClient::TImpl::SetParams(
    ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
    Ydb::Table::ExecuteDataQueryRequest* request)
{
    if (params) {
        request->mutable_parameters()->swap(*params);
    }
}

void TTableClient::TImpl::SetParams(
    const ::google::protobuf::Map<TString, Ydb::TypedValue>& params,
    Ydb::Table::ExecuteDataQueryRequest* request)
{
    *request->mutable_parameters() = params;
}

void TTableClient::TImpl::CollectParams(
    ::google::protobuf::Map<TString, Ydb::TypedValue>* params,
    NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> histgoram)
{

    if (params && histgoram.IsCollecting()) {
        size_t size = 0;
        for (auto& keyvalue: *params) {
            size += keyvalue.second.ByteSizeLong();
        }
        histgoram.Record(size);
    }
}

void TTableClient::TImpl::CollectParams(
    const ::google::protobuf::Map<TString, Ydb::TypedValue>& params,
    NSdkStats::TAtomicHistogram<::NMonitoring::THistogram> histgoram)
{

    if (histgoram.IsCollecting()) {
        size_t size = 0;
        for (auto& keyvalue: params) {
            size += keyvalue.second.ByteSizeLong();
        }
        histgoram.Record(size);
    }
}

void TTableClient::TImpl::CollectQuerySize(const TString& query, NSdkStats::TAtomicHistogram<::NMonitoring::THistogram>& querySizeHistogram) {
    if (querySizeHistogram.IsCollecting()) {
        querySizeHistogram.Record(query.size());
    }
}

void TTableClient::TImpl::CollectQuerySize(const TDataQuery&, NSdkStats::TAtomicHistogram<::NMonitoring::THistogram>&) {}

void TTableClient::TImpl::SetTxSettings(const TTxSettings& txSettings, Ydb::Table::TransactionSettings* proto)
{
    switch (txSettings.Mode_) {
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
        default:
            throw TContractViolation("Unexpected transaction mode.");
    }
}

void TTableClient::TImpl::SetQuery(const TString& queryText, Ydb::Table::Query* query) {
    query->set_yql_text(queryText);
}

void TTableClient::TImpl::SetQuery(const TDataQuery& queryData, Ydb::Table::Query* query) {
    query->set_id(queryData.GetId());
}

void TTableClient::TImpl::SetQueryCachePolicy(const TString&, const TExecDataQuerySettings& settings,
    Ydb::Table::QueryCachePolicy* queryCachePolicy)
{
    queryCachePolicy->set_keep_in_cache(settings.KeepInQueryCache_ ? settings.KeepInQueryCache_.GetRef() : false);
}

void TTableClient::TImpl::SetQueryCachePolicy(const TDataQuery&, const TExecDataQuerySettings& settings,
    Ydb::Table::QueryCachePolicy* queryCachePolicy) {
    queryCachePolicy->set_keep_in_cache(settings.KeepInQueryCache_ ? settings.KeepInQueryCache_.GetRef() : true);
}

TMaybe<TString> TTableClient::TImpl::GetQueryText(const TString& queryText) {
    return queryText;
}

TMaybe<TString> TTableClient::TImpl::GetQueryText(const TDataQuery& queryData) {
    return queryData.GetText();
}

void TTableClient::TImpl::CollectRetryStatAsync(EStatus status) {
    RetryOperationStatCollector.IncAsyncRetryOperation(status);
}

void TTableClient::TImpl::CollectRetryStatSync(EStatus status) {
    RetryOperationStatCollector.IncSyncRetryOperation(status);
}

} // namespace NTable
} // namespace NYdb
