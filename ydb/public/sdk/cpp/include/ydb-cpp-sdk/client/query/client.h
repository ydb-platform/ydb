#pragma once

#include "fwd.h"

#include "query.h"
#include "tx.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/request_settings.h>

namespace NYdb::inline Dev {
    class TProtoAccessor;

    namespace NRetry::Async {
        template <typename TClient, typename TAsyncStatusType>
        class TRetryContext;
    } // namespace NRetry::Async
    namespace NRetry::Sync {
        template <typename TClient, typename TStatusType>
        class TRetryContext;
    } // namespace NRetry::Sync
}

namespace NYdb::inline Dev::NQuery {

struct TCreateSessionSettings : public TSimpleRequestSettings<TCreateSessionSettings> {
    TCreateSessionSettings();
};

using TAsyncCreateSessionResult = NThreading::TFuture<TCreateSessionResult>;
using TRetryOperationSettings = NYdb::NRetry::TRetryOperationSettings;

struct TSessionPoolSettings {
    using TSelf = TSessionPoolSettings;

    // Max number of sessions client can get from session pool
    FLUENT_SETTING_DEFAULT(uint32_t, MaxActiveSessions, 50);

    // Max time session to be in idle state before closing
    FLUENT_SETTING_DEFAULT(TDuration, CloseIdleThreshold, TDuration::Minutes(1));

    // Min number of session in session pool.
    // Sessions will not be closed by CloseIdleThreshold if the number of sessions less then this limit.
    FLUENT_SETTING_DEFAULT(uint32_t, MinPoolSize, 10);
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
class TQueryClient {
    friend class TSession;
    friend class NRetry::Async::TRetryContext<TQueryClient, TAsyncExecuteQueryResult>;
    friend class NRetry::Async::TRetryContext<TQueryClient, TAsyncStatus>;
    friend class NRetry::Sync::TRetryContext<TQueryClient, TStatus>;

public:
    using TQueryResultFunc = std::function<TAsyncExecuteQueryResult(TSession session)>;
    using TQueryFunc = std::function<TAsyncStatus(TSession session)>;
    using TQuerySyncFunc = std::function<TStatus(TSession session)>;
    using TQueryWithoutSessionFunc = std::function<TAsyncStatus(TQueryClient& client)>;
    using TQueryWithoutSessionSyncFunc = std::function<TStatus(TQueryClient& client)>;
    using TSettings = TClientSettings;
    using TSession = TSession;
    using TCreateSessionSettings = TCreateSessionSettings;
    using TAsyncCreateSessionResult = TAsyncCreateSessionResult;

public:
    TQueryClient(const TDriver& driver, const TClientSettings& settings = TClientSettings());

    TAsyncExecuteQueryResult ExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryResult ExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryIterator StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryIterator StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryResult RetryQuery(TQueryResultFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TAsyncStatus RetryQuery(TQueryFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TAsyncStatus RetryQuery(TQueryWithoutSessionFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TStatus RetryQuerySync(const TQuerySyncFunc& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TStatus RetryQuerySync(const TQueryWithoutSessionSyncFunc& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    TAsyncExecuteQueryResult RetryQuery(const std::string& query, const TTxControl& txControl,
        TDuration timeout, bool isIndempotent);

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const std::string& script,
        const TExecuteScriptSettings& settings = TExecuteScriptSettings());

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const std::string& script,
        const TParams& params, const TExecuteScriptSettings& settings = TExecuteScriptSettings());

    TAsyncFetchScriptResultsResult FetchScriptResults(const NKikimr::NOperationId::TOperationId& operationId, int64_t resultSetIndex,
        const TFetchScriptResultsSettings& settings = TFetchScriptResultsSettings());

    TAsyncCreateSessionResult GetSession(const TCreateSessionSettings& settings = TCreateSessionSettings());

    //! Returns number of active sessions given via session pool
    int64_t GetActiveSessionCount() const;

    //! Returns the maximum number of sessions in session pool
    int64_t GetActiveSessionsLimit() const;

    //! Returns the size of session pool
    int64_t GetCurrentPoolSize() const;

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

class TSession {
    friend class TQueryClient;
    friend class TTransaction;
    friend class TExecuteQueryIterator;
public:
    const std::string& GetId() const;

    TAsyncExecuteQueryResult ExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryResult ExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryIterator StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryIterator StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncBeginTransactionResult BeginTransaction(const TTxSettings& txSettings,
        const TBeginTxSettings& settings = TBeginTxSettings());

    class TImpl;
private:
    TSession();
    TSession(std::shared_ptr<TQueryClient::TImpl> client); // Create broken session
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
    const std::string& GetId() const {
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
    TTransaction(const TSession& session, const std::string& txId);

    TSession Session_;
    std::string TxId_;
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
    bool HasResultSet() const { return ResultSet_.has_value(); }
    uint64_t GetResultSetIndex() const { return ResultSetIndex_; }
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }

    bool HasStats() const { return Stats_.has_value(); }
    const std::optional<TExecStats>& GetStats() const { return Stats_; }
    TExecStats ExtractStats() const { return std::move(*Stats_); }
    
    const std::optional<TTransaction>& GetTransaction() const { return Transaction_; }

    TExecuteQueryPart(TStatus&& status, std::optional<TExecStats>&& queryStats, std::optional<TTransaction>&& tx)
        : TStreamPartStatus(std::move(status))
        , Stats_(std::move(queryStats))
        , Transaction_(std::move(tx))
    {}

    TExecuteQueryPart(TStatus&& status, TResultSet&& resultSet, int64_t resultSetIndex,
        std::optional<TExecStats>&& queryStats, std::optional<TTransaction>&& tx)
        : TStreamPartStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
        , ResultSetIndex_(resultSetIndex)
        , Stats_(std::move(queryStats))
        , Transaction_(std::move(tx))
    {}

private:
    std::optional<TResultSet> ResultSet_;
    int64_t ResultSetIndex_ = 0;
    std::optional<TExecStats> Stats_;
    std::optional<TTransaction> Transaction_;
};

class TExecuteQueryResult : public TStatus {
public:
    const std::vector<TResultSet>& GetResultSets() const;
    TResultSet GetResultSet(size_t resultIndex) const;
    TResultSetParser GetResultSetParser(size_t resultIndex) const;

    const std::optional<TExecStats>& GetStats() const { return Stats_; }

    std::optional<TTransaction> GetTransaction() const {return Transaction_; }

    TExecuteQueryResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    TExecuteQueryResult(TStatus&& status, std::vector<TResultSet>&& resultSets,
        std::optional<TExecStats>&& stats, std::optional<TTransaction>&& tx)
        : TStatus(std::move(status))
        , ResultSets_(std::move(resultSets))
        , Stats_(std::move(stats))
        , Transaction_(std::move(tx))
    {}

private:
    std::vector<TResultSet> ResultSets_;
    std::optional<TExecStats> Stats_;
    std::optional<TTransaction> Transaction_;
};

} // namespace NYdb::NQuery
