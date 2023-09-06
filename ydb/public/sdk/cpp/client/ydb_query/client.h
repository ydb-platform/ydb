#pragma once

#include "query.h"
#include "tx.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>
#include <ydb/public/sdk/cpp/client/ydb_retry/retry.h>
#include <ydb/public/sdk/cpp/client/ydb_types/request_settings.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

namespace NYdb {
    class TProtoAccessor;

    namespace NRetry {
        template <typename TClient, typename TStatusType>
        class TRetryContextAsync;
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
    friend class NRetry::TRetryContextAsync<TQueryClient, TExecuteQueryResult>;

public:
    using TQueryFunc = std::function<TAsyncExecuteQueryResult(TSession session)>;
    using TQueryWithoutSessionFunc = std::function<TAsyncExecuteQueryResult(TQueryClient& client)>;
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

    TAsyncExecuteQueryResult RetryQuery(TQueryFunc&& queryFunc, TRetryOperationSettings settings = TRetryOperationSettings());

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const TString& script,
        const TExecuteScriptSettings& settings = TExecuteScriptSettings());

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

class TSession {
    friend class TQueryClient;
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

} // namespace NYdb::NQuery
