#include "bulk_upsert_retry_state.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NYdb::inline Dev::NRetry {

TBulkUpsertRetryState::TBulkUpsertRetryState(TRetryOperationSettings settings)
    : Settings_(std::move(settings))
{}

const TRetryOperationSettings& TBulkUpsertRetryState::Settings() const {
    return Settings_;
}

bool TBulkUpsertRetryState::HasBackup() const {
    return Backup_.has_value();
}

void TBulkUpsertRetryState::CreateBackup(const Ydb::Table::BulkUpsertRequest& request) {
    const auto& rows = request.rows();
    Backup_.emplace(NYdb::TType(rows.type()), rows.value());
}

NYdb::TValue TBulkUpsertRetryState::GetBackupCopy() const {
    Y_ABORT_UNLESS(Backup_.has_value());
    return NYdb::TValue(*Backup_);
}

} // namespace NYdb::NRetry
