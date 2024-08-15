#pragma once

#include "query.h"
#include "tx.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>
#include <ydb/public/sdk/cpp/client/impl/ydb_internal/retry/retry_sync.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

namespace NYdb {
    class TProtoAccessor;

    namespace NRetry::Async {
        template <typename TClient, typename TAsyncStatusType>
        class TRetryContext;
    }
}

namespace NYdb::NQuery {

struct TCreateSessionSettings : public TSimpleRequestSettings<TCreateSessionSettings> {
    TCreateSessionSettings();
};

class TCreateSessionResult;
using TAsyncCreateSessionResult = NThreading::TFuture<TCreateSessionResult>;
using TRetryOperationSettings = NYdb::NRetry::TRetryOperationSettings;

struct TSessionPoolSettings {
    using TSelf = TSessionPoolSettings;

    // Max number of sessions client can get from session pool
    FLUENT_SETTING_DEFAULT(ui32, MaxActiveSessions, 50);

    // Max time session to be in idle state before closing
    FLUENT_SETTING_DEFAULT(TDuration, CloseIdleThreshold, TDuration::Minutes(1));

    // Min number of session in session pool.
    // Sessions will not be closed by CloseIdleThreshold if the number of sessions less then this limit.
    FLUENT_SETTING_DEFAULT(ui32, MinPoolSize, 10);
};

struct TClientSettings : public TCommonClientSettingsBase<TClientSettings> {
    using TSessionPoolSettings = TSessionPoolSettings;
    using TSelf = TClientSettings;
    FLUENT_SETTING(TSessionPoolSettings, SessionPoolSettings);
};

// ! WARNING: Experimental API
// ! This API is currently in experimental state and is a subject for changes.
// ! No backward and/or forward compatibility guarantees are provided.
// ! DO NOT USE for production workloads.
class TSession;
class TQueryClient {
    friend class TSession;
    friend class NRetry::Async::TRetryContext<TQueryClient, TAsyncExecuteQueryResult>;
    friend class NRetry::Async::TRetryContext<TQueryClient, TAsyncStatus>;
    friend class NRetry::Sync::TRetryContext<TQueryClient, TStatus>;

public:
    using TQueryResultFunc = std::function<TAsyncExecuteQueryResult(TSession session)>;
    using TQueryStatusFunc = std::function<TAsyncStatus(TSession session)>;
    using TQuerySyncStatusFunc = std::function<TStatus(TSession session)>;
    using TQueryWithoutSessionFunc = std::function<TAsyncExecuteQueryResult(TQueryClient& client)>;
    using TQueryWithoutSessionStatusFunc = std::function<TAsyncStatus(TQueryClient& client)>;
    using TQueryWithoutSessionSyncStatusFunc = std::function<TStatus(TQueryClient& client)>;
    using TSettings = TClientSettings;
    using TSession = TSession;
    using TCreateSessionSettings = TCreateSessionSettings;
    using TAsyncCreateSessionResult = TAsyncCreateSessionResult;

public:
    TQueryClient(const TDriver& driver, const TClientSettings& settings = TClientSettings());

    TAsyncExecuteQueryResult ExecuteQuery(const TString& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryResult ExecuteQuery(const TString& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryIterator StreamExecuteQuery(const TString& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryIterator StreamExecuteQuery(const TString& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryResult RetryQuery(TQueryResultFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TAsyncStatus RetryQuery(TQueryStatusFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TAsyncStatus RetryQuery(TQueryWithoutSessionStatusFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TStatus RetryQuery(const TQuerySyncStatusFunc& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TStatus RetryQuery(const TQueryWithoutSessionSyncStatusFunc& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TAsyncExecuteQueryResult RetryQuery(const TString& query, const TTxControl& txControl,
        TDuration timeout, bool isIndempotent);

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const TString& script,
        const TExecuteScriptSettings& settings = TExecuteScriptSettings());

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const TString& script,
        const TParams& params, const TExecuteScriptSettings& settings = TExecuteScriptSettings());

    TAsyncFetchScriptResultsResult FetchScriptResults(const NKikimr::NOperationId::TOperationId& operationId, int64_t resultSetIndex,
        const TFetchScriptResultsSettings& settings = TFetchScriptResultsSettings());

    TAsyncCreateSessionResult GetSession(const TCreateSessionSettings& settings = TCreateSessionSettings());

    //! Returns number of active sessions given via session pool
    i64 GetActiveSessionCount() const;

    //! Returns the maximum number of sessions in session pool
    i64 GetActiveSessionsLimit() const;

    //! Returns the size of session pool
    i64 GetCurrentPoolSize() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

class TTransaction;
class TSession {
    friend class TQueryClient;
    friend class TTransaction;
public:
    const TString& GetId() const;

    TAsyncExecuteQueryResult ExecuteQuery(const TString& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryResult ExecuteQuery(const TString& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryIterator StreamExecuteQuery(const TString& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryIterator StreamExecuteQuery(const TString& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncBeginTransactionResult BeginTransaction(const TTxSettings& txSettings,
        const TBeginTxSettings& settings = TBeginTxSettings());

    class TImpl;
private:
    TSession();
    TSession(std::shared_ptr<TQueryClient::TImpl> client, TSession::TImpl* sessionImpl);

    std::shared_ptr<TQueryClient::TImpl> Client_;
    std::shared_ptr<TSession::TImpl> SessionImpl_;
};

class TCreateSessionResult: public TStatus {
    friend class TSession::TImpl;
public:
    TCreateSessionResult(TStatus&& status, TSession&& session);
    TSession GetSession() const;

private:
    TSession Session_;
};

class TTransaction {
    friend class TQueryClient;
    friend class TExecuteQueryIterator::TReaderImpl;
public:
    const TString& GetId() const {
        return TxId_;
    }

    bool IsActive() const {
        return !TxId_.empty();
    }

    TAsyncCommitTransactionResult Commit(const TCommitTxSettings& settings = TCommitTxSettings());
    TAsyncStatus Rollback(const TRollbackTxSettings& settings = TRollbackTxSettings());

    TSession GetSession() const {
        return Session_;
    }

private:
    TTransaction(const TSession& session, const TString& txId);

    TSession Session_;
    TString TxId_;
};

class TBeginTransactionResult : public TStatus {
public:
    TBeginTransactionResult(TStatus&& status, TTransaction transaction);

    const TTransaction& GetTransaction() const;

private:
    TTransaction Transaction_;
};

class TExecuteQueryPart : public TStreamPartStatus {
public:
    bool HasResultSet() const { return ResultSet_.Defined(); }
    ui64 GetResultSetIndex() const { return ResultSetIndex_; }
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }

    const TMaybe<TExecStats>& GetStats() const { return Stats_; }
    const TMaybe<TTransaction>& GetTransaction() const { return Transaction_; }

    TExecuteQueryPart(TStatus&& status, TMaybe<TExecStats>&& queryStats, TMaybe<TTransaction>&& tx)
        : TStreamPartStatus(std::move(status))
        , Stats_(std::move(queryStats))
        , Transaction_(std::move(tx))
    {}

    TExecuteQueryPart(TStatus&& status, TResultSet&& resultSet, i64 resultSetIndex,
        TMaybe<TExecStats>&& queryStats, TMaybe<TTransaction>&& tx)
        : TStreamPartStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
        , ResultSetIndex_(resultSetIndex)
        , Stats_(std::move(queryStats))
        , Transaction_(std::move(tx))
    {}

private:
    TMaybe<TResultSet> ResultSet_;
    i64 ResultSetIndex_ = 0;
    TMaybe<TExecStats> Stats_;
    TMaybe<TTransaction> Transaction_;
};

class TExecuteQueryResult : public TStatus {
public:
    const TVector<TResultSet>& GetResultSets() const;
    TResultSet GetResultSet(size_t resultIndex) const;
    TResultSetParser GetResultSetParser(size_t resultIndex) const;

    const TMaybe<TExecStats>& GetStats() const { return Stats_; }

    TMaybe<TTransaction> GetTransaction() const {return Transaction_; }

    TExecuteQueryResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    TExecuteQueryResult(TStatus&& status, TVector<TResultSet>&& resultSets,
        TMaybe<TExecStats>&& stats, TMaybe<TTransaction>&& tx)
        : TStatus(std::move(status))
        , ResultSets_(std::move(resultSets))
        , Stats_(std::move(stats))
        , Transaction_(std::move(tx))
    {}

private:
    TVector<TResultSet> ResultSets_;
    TMaybe<TExecStats> Stats_;
    TMaybe<TTransaction> Transaction_;
};

} // namespace NYdb::NQuery
