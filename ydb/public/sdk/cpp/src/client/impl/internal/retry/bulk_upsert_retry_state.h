#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/retry/retry.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/status/status.h>
#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/value/value.h>

#include <optional>

namespace Ydb::Table {
class BulkUpsertRequest;
}

namespace NYdb::inline Dev::NRetry {

class TBulkUpsertRetryState {
public:
    explicit TBulkUpsertRetryState(TRetryOperationSettings settings);

    const TRetryOperationSettings& Settings() const;

    bool HasBackup() const;
    void CreateBackup(const Ydb::Table::BulkUpsertRequest& request);
    NYdb::TValue GetBackupCopy() const;

private:
    TRetryOperationSettings Settings_;
    std::optional<NYdb::TValue> Backup_;
};

} // namespace NYdb::NRetry
