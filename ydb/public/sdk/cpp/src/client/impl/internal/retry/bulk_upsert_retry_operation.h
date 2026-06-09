#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/table/table.h>
#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/bulk_upsert_retry_state.h>

#include <memory>
#include <string>

namespace NYdb::inline Dev::NRetry {

class TBulkUpsertRetryOperation {
public:
    TBulkUpsertRetryOperation(
        NTable::TTableClient* client,
        std::string table,
        std::shared_ptr<TBulkUpsertRetryState> state,
        NTable::TBulkUpsertSettings settings);

    NTable::TAsyncBulkUpsertResult operator()(TDuration timeout) const;

private:
    NTable::TTableClient* Client_;
    std::string Table_;
    std::shared_ptr<TBulkUpsertRetryState> State_;
    NTable::TBulkUpsertSettings Settings_;
};

} // namespace NYdb::NRetry
