#pragma once

#include "query.h"

#include <ydb/public/sdk/cpp/client/ydb_driver/driver.h>

#include <util/generic/maybe.h>
#include <util/generic/ptr.h>

namespace NYdb {
    class TProtoAccessor;
}

namespace NYdb::NQuery {

struct TClientSettings : public TCommonClientSettingsBase<TClientSettings> {
    using TSelf = TClientSettings;
};

class TQueryClient {
public:
    TQueryClient(const TDriver& driver, const TClientSettings& settings = TClientSettings());

    TAsyncExecuteQueryResult ExecuteQuery(const TString& query,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    TAsyncExecuteQueryIterator StreamExecuteQuery(const TString& query,
        const TExecuteQuerySettings& settings = TExecuteQuerySettings());

    NThreading::TFuture<TScriptExecutionOperation> ExecuteScript(const TString& script,
        const TExecuteScriptSettings& settings = TExecuteScriptSettings());

    TAsyncFetchScriptResultsResult FetchScriptResults(const TString& executionId,
        const TFetchScriptResultsSettings& settings = TFetchScriptResultsSettings());

private:
    class TImpl;
    std::shared_ptr<TImpl> Impl_;
};

} // namespace NYdb::NQuery
