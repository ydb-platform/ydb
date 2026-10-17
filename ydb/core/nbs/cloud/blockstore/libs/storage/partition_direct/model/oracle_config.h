#pragma once

#include <ydb/core/nbs/cloud/blockstore/config/public.h>

#include <util/datetime/base.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

// Unset MaxInflightWritesForDirectWrite selects adaptive DirectWrite at or
// below this disk-wide in-flight count.
constexpr ui32 DefaultMaxInflightWritesForDirectWrite = 16;

class TOracleConfig
{
public:
    explicit TOracleConfig(TStorageConfigPtr storageConfig);

    [[nodiscard]] TDuration GetMaxDurationBeforeGoingTemporaryOffline() const;

    [[nodiscard]] TDuration GetMaxDurationBeforeGoingOffline() const;

    [[nodiscard]] ui32 GetMinErrorsCountBeforeGoingOffline() const;

    [[nodiscard]] ui32 GetErrorsCountForGoingOffline() const;

    [[nodiscard]] ui64 GetErrorsTotalSizeForGoingOffline() const;

    [[nodiscard]] ui32 GetTimePredictionHistorySize() const;

    [[nodiscard]] ui32 GetTimePredictionNthFromEnd() const;

    [[nodiscard]] TDuration GetMaxDurationBeforeReturningOnline() const;

    [[nodiscard]] ui32 GetMinSuccessesCountBeforeReturningOnline() const;

    // Disk-wide in-flight write count at or below which GetWriteMode selects
    // DirectWrite. Unset defaults to DefaultMaxInflightWritesForDirectWrite.
    // 0 keeps the configured static WriteMode.
    [[nodiscard]] ui32 GetMaxInflightWritesForDirectWrite() const;

private:
    TStorageConfigPtr StorageConfig;
};

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
