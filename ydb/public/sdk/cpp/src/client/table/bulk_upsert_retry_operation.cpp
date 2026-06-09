#include <ydb/public/sdk/cpp/src/client/impl/internal/retry/bulk_upsert_retry_operation.h>

#include <ydb/public/sdk/cpp/src/client/table/impl/table_client.h>

namespace NYdb::inline Dev::NRetry {

TBulkUpsertRetryOperation::TBulkUpsertRetryOperation(
    NTable::TTableClient* client,
    std::string table,
    std::shared_ptr<TBulkUpsertRetryState> state,
    NTable::TBulkUpsertSettings settings)
    : Client_(client)
    , Table_(std::move(table))
    , State_(std::move(state))
    , Settings_(std::move(settings))
{}

NTable::TAsyncBulkUpsertResult TBulkUpsertRetryOperation::operator()(TDuration timeout) const {
    auto opSettings = Settings_;
    if (timeout != TDuration::Max()) {
        opSettings.ClientTimeout(timeout);
    }

    if (State_->HasBackup()) {
        opSettings.RetryRowsState_ = nullptr;
        opSettings.RetryRowsStateHolder_.reset();
        return Client_->Impl_->BulkUpsert(Table_, State_->GetBackupCopy(), opSettings);
    }

    Y_ABORT_UNLESS(State_->HasFirstAttemptRows());
    opSettings.RetryRowsState_ = State_.get();
    opSettings.RetryRowsStateHolder_ = State_;
    return Client_->Impl_->BulkUpsert(Table_, State_->TakeFirstAttemptRows(), opSettings);
}

} // namespace NYdb::NRetry
