#include "bulk_upsert_retry_state.h"

#include <ydb/public/api/protos/ydb_table.pb.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>

namespace NYdb::inline Dev::NRetry {

TBulkUpsertRetryState::TBulkUpsertRetryState() = default;
TBulkUpsertRetryState::~TBulkUpsertRetryState() = default;

void TBulkUpsertRetryState::ConsumeRows(NYdb::TValue&& rows) {
    FirstAttemptRows_ = std::move(rows);
}

bool TBulkUpsertRetryState::HasFirstAttemptRows() const {
    return FirstAttemptRows_.has_value();
}

NYdb::TValue TBulkUpsertRetryState::TakeFirstAttemptRows() {
    Y_ABORT_UNLESS(FirstAttemptRows_.has_value());
    auto rows = std::move(*FirstAttemptRows_);
    FirstAttemptRows_.reset();
    return rows;
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

google::protobuf::Arena* TBulkUpsertRetryState::EnsureArena() {
    if (!Arena_) {
        Arena_ = std::make_unique<google::protobuf::Arena>();
    }
    return Arena_.get();
}

void TBulkUpsertRetryState::SetRequest(Ydb::Table::BulkUpsertRequest* request) {
    Request_ = request;
}

Ydb::Table::BulkUpsertRequest* TBulkUpsertRetryState::GetRequest() const {
    return Request_;
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
