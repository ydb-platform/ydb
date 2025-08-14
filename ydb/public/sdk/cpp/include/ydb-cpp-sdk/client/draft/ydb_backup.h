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

class TIncrementalBackupResponse : public TOperation {
public:
    struct TMetadata {
        EBackupProgress Progress;
        int32_t ProgressPercent = 0; // [0; 100]
    };

public:
    using TOperation::TOperation;
    TIncrementalBackupResponse(TStatus&& status, Ydb::Operations::Operation&& operation);

    const TMetadata& Metadata() const;

private:
    TMetadata Metadata_;
};

} // namespace NYdb::inline Dev::NBackup
