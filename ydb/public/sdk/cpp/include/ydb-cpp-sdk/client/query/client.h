#pragma once

#include "fwd.h"

#include "query.h"
#include "tx.h"

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/driver/driver.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/params/params.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/virtual_timestamp.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/tx/tx.h>
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
    namespace NRetry {
        template <typename TClient>
        class TRetryDeadlineHelper;
    } // namespace NRetry
}

namespace NYdb::inline Dev::NQuery {

//! Request settings for obtaining a query session.
struct TCreateSessionSettings : public TSimpleRequestSettings<TCreateSessionSettings> {
    //! Constructs settings with a five-second client timeout.
    TCreateSessionSettings() {
        ClientTimeout(TDuration::Seconds(5));
    }
};

using TAsyncCreateSessionResult = NThreading::TFuture<TCreateSessionResult>;
using TRetryOperationSettings = NYdb::NRetry::TRetryOperationSettings;

//! Settings for the query session pool.
struct TSessionPoolSettings {
    using TSelf = TSessionPoolSettings;

    //! Sets the maximum number of sessions that may be acquired from the pool; defaults to 50.
    FLUENT_SETTING_DEFAULT(uint32_t, MaxActiveSessions, 50);

    //! Sets how long an idle session may remain in the pool before closing; defaults to one minute.
    FLUENT_SETTING_DEFAULT(TDuration, CloseIdleThreshold, TDuration::Minutes(1));

    //! Sets the minimum pool size protected from idle eviction; defaults to 10 sessions.
    FLUENT_SETTING_DEFAULT(uint32_t, MinPoolSize, 10);

    //! Keeps creating a session in the background after the caller's timeout; disabled by default.
    FLUENT_SETTING_DEFAULT(bool, UseDeferredSessionCreation, false);
};

//! Settings shared by all operations performed through TQueryClient.
struct TClientSettings : public TCommonClientSettingsBase<TClientSettings> {
    using TSessionPoolSettings = TSessionPoolSettings;
    using TSelf = TClientSettings;
    //! Configures the query session pool.
    FLUENT_SETTING(TSessionPoolSettings, SessionPoolSettings);

    //! Sets the value of the ydb.query.session.pool.name OpenTelemetry tag.
    //! An empty value uses "<database>@<endpoint>".
    FLUENT_SETTING(std::string, PoolName);

    //! Sets the default retry policy for retry-capable query client operations.
    FLUENT_SETTING_DEFAULT(TRetryOperationSettings, RetrySettings, TRetryOperationSettings());
};

//! Client for executing queries and scripts through YDB Query Service.
class TQueryClient {
    friend class TSession;
    friend class NRetry::Async::TRetryContext<TQueryClient, TAsyncExecuteQueryResult>;
    friend class NRetry::Async::TRetryContext<TQueryClient, TAsyncStatus>;
    friend class NRetry::Async::TRetryContext<TQueryClient, NThreading::TFuture<TScriptExecutionOperation>>;
    friend class NRetry::Async::TRetryContext<TQueryClient, TAsyncFetchScriptResultsResult>;
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
    //! Constructs a query client that uses the supplied driver and settings.
    TQueryClient(const TDriver& driver, const TClientSettings& settings = TClientSettings());

    //! Executes a query without parameters or an explicit session and buffers all result sets.
    //! Multi-step interactive transactions must be executed through TSession.
    TAsyncExecuteQueryResult ExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    //! Executes a parameterized query without an explicit session and buffers all result sets.
    //! Multi-step interactive transactions must be executed through TSession.
    TAsyncExecuteQueryResult ExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    //! Starts streaming a query without parameters or an explicit session.
    //! Multi-step interactive transactions must be executed through TSession.
    TAsyncExecuteQueryIterator StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    //! Starts streaming a parameterized query without an explicit session.
    //! Multi-step interactive transactions must be executed through TSession.
    TAsyncExecuteQueryIterator StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    //! Runs an asynchronous result-returning callback with a pooled session and retries retryable failures.
    //! The callback may be invoked more than once and must honor the idempotency configured in settings.
    TAsyncExecuteQueryResult RetryQuery(TQueryResultFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    //! Runs an asynchronous status-returning callback with a pooled session and retries retryable failures.
    //! The callback may be invoked more than once and must honor the idempotency configured in settings.
    TAsyncStatus RetryQuery(TQueryFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    //! Runs an asynchronous callback without a session and retries retryable failures.
    //! The callback may be invoked more than once and must honor the idempotency configured in settings.
    TAsyncStatus RetryQuery(TQueryWithoutSessionFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    //! Runs a synchronous callback with a pooled session and retries retryable failures.
    //! The callback may be invoked more than once and must honor the idempotency configured in settings.
    TStatus RetryQuerySync(const TQuerySyncFunc& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    //! Runs a synchronous callback without a session and retries retryable failures.
    //! The callback may be invoked more than once and must honor the idempotency configured in settings.
    TStatus RetryQuerySync(const TQueryWithoutSessionSyncFunc& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    //! Executes a query with retries for up to timeout, using isIndempotent to select safe retry behavior.
    TAsyncExecuteQueryResult RetryQuery(const std::string& query, const TTxControl& txControl,
        TDuration timeout, bool isIndempotent);

    //! Starts asynchronous execution of a script without parameters.
    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const std::string& script,
        const TExecuteScriptSettings& settings = TExecuteScriptSettings(),
        const std::optional<TRetryOperationSettings>& retrySettings = std::nullopt);

    //! Starts asynchronous execution of a parameterized script.
    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const std::string& script,
        const TParams& params, const TExecuteScriptSettings& settings = TExecuteScriptSettings(),
        const std::optional<TRetryOperationSettings>& retrySettings = std::nullopt);

    //! Fetches one page of a completed script result set.
    TAsyncFetchScriptResultsResult FetchScriptResults(const NKikimr::NOperationId::TOperationId& operationId, int64_t resultSetIndex,
        const TFetchScriptResultsSettings& settings = TFetchScriptResultsSettings(),
        const std::optional<TRetryOperationSettings>& retrySettings = std::nullopt);

    //! Acquires a session from the internal session pool or creates a new one.
    TAsyncCreateSessionResult GetSession(const TCreateSessionSettings& settings = TCreateSessionSettings());

    //! Explicitly deletes the server-side session identified by sessionId.
    TAsyncStatus DeleteSession(const std::string& sessionId, const TDeleteSessionSettings& settings = TDeleteSessionSettings());

    //! Returns the number of sessions currently acquired from the session pool.
    int64_t GetActiveSessionCount() const;

    //! Returns the maximum number of sessions that may be acquired from the session pool.
    int64_t GetActiveSessionsLimit() const;

    //! Returns the number of idle sessions currently available in the session pool.
    int64_t GetCurrentPoolSize() const;

    //! Returns whether the current thread is already inside a query retry operation.
    //! This method is intended for SDK retry wrappers.
    bool GetInRetryOperationContext() const;
    //! Marks whether the current thread is inside a query retry operation.
    //! This method is intended for SDK retry wrappers.
    void SetInRetryOperationContext(bool value);

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

//! A Query Service session used for interactive transactions and session-bound queries.
class TSession {
    friend class TQueryClient;
    friend class TTransaction;
    friend class TExecuteQueryIterator;
    friend class NRetry::TRetryDeadlineHelper<TQueryClient>;
public:
    //! Returns the server-side session identifier.
    const std::string& GetId() const;

    //! Returns the deadline propagated to this session by a retry operation, when present.
    const std::optional<TDeadline>& GetPropagatedDeadline() const;

    //! Executes a query without parameters in this session and buffers all result sets.
    TAsyncExecuteQueryResult ExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    //! Executes a parameterized query in this session and buffers all result sets.
    TAsyncExecuteQueryResult ExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    //! Starts streaming a query without parameters in this session.
    TAsyncExecuteQueryIterator StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    //! Starts streaming a parameterized query in this session.
    TAsyncExecuteQueryIterator StreamExecuteQuery(const std::string& query, const TTxControl& txControl,
        const TParams& params, const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    //! Begins an interactive transaction in this session.
    TAsyncBeginTransactionResult BeginTransaction(const TTxSettings& txSettings,
        const TBeginTxSettings& settings = TBeginTxSettings());

    class TImpl;
private:
    TSession();
    TSession(std::shared_ptr<TQueryClient::TImpl> client); // Create broken session
    TSession(std::shared_ptr<TQueryClient::TImpl> client, TSession::TImpl* sessionImpl);

    void SetPropagatedDeadline(const TDeadline& deadline);

    std::shared_ptr<TQueryClient::TImpl> Client_;
    std::shared_ptr<TSession::TImpl> SessionImpl_;
};

//! Result of acquiring or creating a query session.
class TCreateSessionResult: public TStatus {
    friend class TSession::TImpl;
public:
    //! Constructs a session result from a status and session.
    TCreateSessionResult(TStatus&& status, TSession&& session);
    //! Returns the session, throwing if the operation status is not successful.
    TSession GetSession() const;

private:
    TSession Session_;
};

//! An interactive Query Service transaction bound to a TSession.
class TTransaction : public TTransactionBase {
    friend class TQueryClient;
    friend class TExecuteQueryIterator::TReaderImpl;
    friend class TExecQueryImpl;

public:
    //! Returns whether this transaction has a non-empty server-side identifier.
    bool IsActive() const;

    //! Runs precommit callbacks and asynchronously commits the transaction.
    TAsyncCommitTransactionResult Commit(const TCommitTxSettings& settings = TCommitTxSettings());
    //! Asynchronously rolls back the transaction and runs failure callbacks.
    TAsyncStatus Rollback(const TRollbackTxSettings& settings = TRollbackTxSettings());

    //! Returns the session to which this transaction is bound.
    TSession GetSession() const;

    //! Registers a callback that is run before the transaction is committed.
    void AddPrecommitCallback(TPrecommitTransactionCallback cb) override;
    //! Registers a callback that is run after commit failure or rollback.
    void AddOnFailureCallback(TOnFailureTransactionCallback cb) override;

private:
    TTransaction(const TSession& session, const std::string& txId);

    TAsyncStatus Precommit() const;
    NThreading::TFuture<void> ProcessFailure() const;

    class TImpl;

    std::shared_ptr<TImpl> TransactionImpl_;
};

//! Describes how a query participates in a transaction.
class TTxControl {
    friend class TExecQueryImpl;
    friend class TExecQueryInternal;

public:
    using TSelf = TTxControl;

    //! Continues an existing interactive transaction.
    static TTxControl Tx(const TTransaction& tx) {
        return TTxControl(tx);
    }

    //! Continues a transaction by its identifier.
    //! Prefer Tx(const TTransaction&) so that the owning session is retained.
    [[deprecated("This is bug-provoking API. Use TTxControl::Tx(TTransaction) instead. "
                 "This constructor will be removed in upcomming release")]]
    static TTxControl Tx(const std::string& txId) {
        return TTxControl(txId);
    }

    //! Begins a transaction with the supplied transaction settings.
    static TTxControl BeginTx(const TTxSettings& settings = TTxSettings()) {
        return TTxControl(settings);
    }

    //! Leaves transaction handling to YDB's implicit transaction rules.
    static TTxControl NoTx() {
        return TTxControl();
    }

    //! Requests that the selected or newly started transaction be committed after the query.
    FLUENT_SETTING_FLAG(CommitTx);

    //! Returns whether this control selects or starts an explicit transaction.
    bool HasTx() const { return !std::holds_alternative<std::monostate>(Tx_); }

private:
    TTxControl() {}

    TTxControl(const TTransaction& tx)
        : Tx_(tx) {}

    TTxControl(const TTxSettings& txSettings)
        : Tx_(txSettings) {}
    
    TTxControl(const std::string& txId)
        : Tx_(txId) {}

    const std::variant<std::monostate, TTransaction, TTxSettings, std::string> Tx_;
};

//! Result of beginning an interactive transaction.
class TBeginTransactionResult : public TStatus {
public:
    //! Constructs a begin-transaction result from a status and transaction.
    TBeginTransactionResult(TStatus&& status, TTransaction transaction);

    //! Returns the transaction, throwing if the operation status is not successful.
    const TTransaction& GetTransaction() const;

private:
    TTransaction Transaction_;
};

//! One part of a streaming query response.
class TExecuteQueryPart : public TStreamPartStatus {
public:
    //! Returns whether this response part contains a result set fragment.
    bool HasResultSet() const { return ResultSet_.has_value(); }
    //! Returns the zero-based index of the result set. HasResultSet() must be true.
    uint64_t GetResultSetIndex() const { return ResultSetIndex_; }
    //! Returns the result set fragment. HasResultSet() must be true.
    const TResultSet& GetResultSet() const { return *ResultSet_; }
    //! Moves the result set fragment out of this response part. HasResultSet() must be true.
    TResultSet ExtractResultSet() { return std::move(*ResultSet_); }

    //! Returns whether this response part contains execution statistics.
    bool HasStats() const { return Stats_.has_value(); }
    //! Returns the execution statistics carried by this response part, when present.
    const std::optional<TExecStats>& GetStats() const { return Stats_; }
    //! Returns the execution statistics. HasStats() must be true.
    TExecStats ExtractStats() const { return std::move(*Stats_); }
    
    //! Returns a transaction started by the query, when it was not committed in the same request.
    const std::optional<TTransaction>& GetTransaction() const { return Transaction_; }

    //! Returns the commit timestamp when the query committed a transaction with write effects.
    const std::optional<NScheme::TVirtualTimestamp>& GetCommitTimestamp() const { return CommitTimestamp_; }

    //! Constructs a response part without a result set fragment.
    TExecuteQueryPart(TStatus&& status, std::optional<TExecStats>&& queryStats, std::optional<TTransaction>&& tx,
        std::optional<NScheme::TVirtualTimestamp>&& commitTimestamp = {})
        : TStreamPartStatus(std::move(status))
        , Stats_(std::move(queryStats))
        , Transaction_(std::move(tx))
        , CommitTimestamp_(std::move(commitTimestamp))
    {}

    //! Constructs a response part containing a result set fragment.
    TExecuteQueryPart(TStatus&& status, TResultSet&& resultSet, int64_t resultSetIndex,
        std::optional<TExecStats>&& queryStats, std::optional<TTransaction>&& tx,
        std::optional<NScheme::TVirtualTimestamp>&& commitTimestamp = {})
        : TStreamPartStatus(std::move(status))
        , ResultSet_(std::move(resultSet))
        , ResultSetIndex_(resultSetIndex)
        , Stats_(std::move(queryStats))
        , Transaction_(std::move(tx))
        , CommitTimestamp_(std::move(commitTimestamp))
    {}

private:
    std::optional<TResultSet> ResultSet_;
    int64_t ResultSetIndex_ = 0;
    std::optional<TExecStats> Stats_;
    std::optional<TTransaction> Transaction_;
    std::optional<NScheme::TVirtualTimestamp> CommitTimestamp_;
};

//! Buffered result of a query execution.
class TExecuteQueryResult : public TStatus {
public:
    //! Returns all result sets in statement order.
    const std::vector<TResultSet>& GetResultSets() const;
    //! Returns a copy of the result set at resultIndex, throwing when the index is out of range.
    TResultSet GetResultSet(size_t resultIndex) const;
    //! Returns a parser for the result set at resultIndex, throwing when the index is out of range.
    TResultSetParser GetResultSetParser(size_t resultIndex) const;

    //! Returns execution statistics when statistics collection was enabled.
    const std::optional<TExecStats>& GetStats() const { return Stats_; }

    //! Returns a transaction started by the query, when it was not committed in the same request.
    std::optional<TTransaction> GetTransaction() const {return Transaction_; }

    //! Returns the commit timestamp when the query committed a transaction with write effects.
    const std::optional<NScheme::TVirtualTimestamp>& GetCommitTimestamp() const { return CommitTimestamp_; }

    //! Constructs a query result that contains only an operation status.
    TExecuteQueryResult(TStatus&& status)
        : TStatus(std::move(status))
    {}

    //! Constructs a query result from its status, result sets, statistics, and transaction metadata.
    TExecuteQueryResult(TStatus&& status, std::vector<TResultSet>&& resultSets,
        std::optional<TExecStats>&& stats, std::optional<TTransaction>&& tx,
        std::optional<NScheme::TVirtualTimestamp>&& commitTimestamp = {})
        : TStatus(std::move(status))
        , ResultSets_(std::move(resultSets))
        , Stats_(std::move(stats))
        , Transaction_(std::move(tx))
        , CommitTimestamp_(std::move(commitTimestamp))
    {}

private:
    std::vector<TResultSet> ResultSets_;
    std::optional<TExecStats> Stats_;
    std::optional<TTransaction> Transaction_;
    std::optional<NScheme::TVirtualTimestamp> CommitTimestamp_;
};

} // namespace NYdb::NQuery
