#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <google/protobuf/arena.h>
#include <memory>
#include <optional>

namespace Ydb::Table {
class BulkUpsertRequest;
}

namespace NYdb::inline Dev::NRetry {

class TBulkUpsertRetryState {
public:
    TBulkUpsertRetryState();
    ~TBulkUpsertRetryState();

    void ConsumeRows(NYdb::TValue&& rows);

    bool HasFirstAttemptRows() const;
    NYdb::TValue TakeFirstAttemptRows();

    bool HasBackup() const;
    void CreateBackup(const Ydb::Table::BulkUpsertRequest& request);
    NYdb::TValue GetBackupCopy() const;

    google::protobuf::Arena* EnsureArena();
    void SetRequest(Ydb::Table::BulkUpsertRequest* request);
    Ydb::Table::BulkUpsertRequest* GetRequest() const;

private:
    std::optional<NYdb::TValue> FirstAttemptRows_;
    std::optional<NYdb::TValue> Backup_;

    std::unique_ptr<google::protobuf::Arena> Arena_;
    Ydb::Table::BulkUpsertRequest* Request_ = nullptr;
};

bool IsRetryableBulkUpsertFailure(EStatus status, bool idempotent);

} // namespace NYdb::NRetry
