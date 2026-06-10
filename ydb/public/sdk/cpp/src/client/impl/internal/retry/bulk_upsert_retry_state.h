#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <optional>

namespace Ydb::Table {
class BulkUpsertRequest;
}

namespace NYdb::inline Dev::NRetry {

class TBulkUpsertRetryState {
public:
    bool HasBackup() const;
    void CreateBackup(const Ydb::Table::BulkUpsertRequest& request);
    NYdb::TValue GetBackupCopy() const;

private:
    std::optional<NYdb::TValue> Backup_;
};

bool IsRetryableBulkUpsertFailure(EStatus status, bool idempotent);

} // namespace NYdb::NRetry
