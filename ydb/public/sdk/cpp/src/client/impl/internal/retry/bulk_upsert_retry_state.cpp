#include "bulk_upsert_retry_state.h"

#include <ydb/public/api/protos/ydb_table.pb.h>

namespace NYdb::inline Dev::NRetry {

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

bool IsRetryableBulkUpsertFailure(EStatus status, bool idempotent) {
    switch (status) {
        case EStatus::ABORTED:
        case EStatus::OVERLOADED:
        case EStatus::CLIENT_RESOURCE_EXHAUSTED:
        case EStatus::UNAVAILABLE:
        case EStatus::BAD_SESSION:
        case EStatus::SESSION_BUSY:
        case EStatus::NOT_FOUND:
            return true;
        case EStatus::UNDETERMINED:
        case EStatus::TRANSPORT_UNAVAILABLE:
            return idempotent;
        default:
            return false;
    }
}

} // namespace NYdb::NRetry
