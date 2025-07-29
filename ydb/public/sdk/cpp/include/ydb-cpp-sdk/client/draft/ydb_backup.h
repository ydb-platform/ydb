#pragma once

#include <ydb/public/sdk/cpp/include/ydb-cpp-sdk/client/types/operation/operation.h>

#include <limits>

#include <cstdint>

namespace NYdb::inline Dev::NBackup {

enum class EBackupProgress {
    Unspecified = 0,
    Preparing = 1,
    TransferData = 2,
    Done = 3,
    Cancellation = 4,
    Cancelled = 5,

    Unknown = std::numeric_limits<int>::max(),
};

struct TBackupItemProgress {
    uint32_t PartsTotal;
    uint32_t PartsCompleted;
};

class TIncrementalBackupResponse : public TOperation {
public:
    struct TMetadata {
        EBackupProgress Progress;
        std::vector<TBackupItemProgress> ItemsProgress;
    };

public:
    using TOperation::TOperation;
    TIncrementalBackupResponse(TStatus&& status, Ydb::Operations::Operation&& operation);

    const TMetadata& Metadata() const;

private:
    TMetadata Metadata_;
};

} // namespace NYdb::NBackup
