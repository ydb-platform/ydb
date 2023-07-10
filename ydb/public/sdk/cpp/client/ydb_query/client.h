#pragma once

#include "query.h"
#include "tx.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>
#include <ydb/public/sdk/cpp/client/ydb_params/params.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

namespace NYdb {
    class TProtoAccessor;
}

namespace NYdb::NQuery {

struct TClientSettings : public TCommonClientSettingsBase<TClientSettings> {
    using TSelf = TClientSettings;
};

// ! WARNING: Experimental API
// ! This API is currently in experimental state and is a subject for changes.
// ! No backward and/or forward compatibility guarantees are provided.
// ! DO NOT USE for production workloads.
class TQueryClient {
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

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const TString& script,
        const TExecuteScriptSettings& settings = TExecuteScriptSettings());

    TAsyncFetchScriptResultsResult FetchScriptResults(const TString& executionId, int64_t resultSetId,
        const TFetchScriptResultsSettings& settings = TFetchScriptResultsSettings());

    TAsyncFetchScriptResultsResult FetchScriptResults(const TScriptExecutionOperation& scriptExecutionOperation, int64_t resultSetId,
        const TFetchScriptResultsSettings& settings = TFetchScriptResultsSettings());

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NQuery
