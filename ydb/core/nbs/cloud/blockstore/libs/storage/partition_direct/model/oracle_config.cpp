#include "oracle_config.h"

#include <ydb/core/nbs/cloud/blockstore/config/config.h>

#include <util/generic/size_literals.h>

namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect {

namespace {

TDuration GetFromConfig(const ui64 milliseconds, const TDuration defaultValue)
{
    return milliseconds ? TDuration::MilliSeconds(milliseconds) : defaultValue;
}

ui32 GetFromConfig(const ui32 value, const ui32 defaultValue)
{
    return value ? value : defaultValue;
}

ui64 GetFromConfig(const ui64 value, const ui64 defaultValue)
{
    return value ? value : defaultValue;
}

}   // namespace

TOracleConfig::TOracleConfig(TStorageConfigPtr storageConfig)
    : StorageConfig(std::move(storageConfig))
{}

TDuration TOracleConfig::GetMaxDurationBeforeGoingTemporaryOffline() const
{
    return GetFromConfig(
        StorageConfig->GetOracleConfig()
            .GetMaxDurationBeforeGoingTemporaryOffline(),
        TDuration::Seconds(10));
}

TDuration TOracleConfig::GetMaxDurationBeforeGoingOffline() const
{
    return GetFromConfig(
        StorageConfig->GetOracleConfig().GetMaxDurationBeforeGoingOffline(),
        TDuration::Seconds(10));
}

ui32 TOracleConfig::GetMinErrorsCountBeforeGoingOffline() const
{
    return GetFromConfig(
        StorageConfig->GetOracleConfig().GetMinErrorsCountBeforeGoingOffline(),
        10);
}

ui32 TOracleConfig::GetErrorsCountForGoingOffline() const
{
    return GetFromConfig(
        StorageConfig->GetOracleConfig().GetErrorsCountForGoingOffline(),
        1000);
}

ui64 TOracleConfig::GetErrorsTotalSizeForGoingOffline() const
{
    return GetFromConfig(
        StorageConfig->GetOracleConfig().GetErrorsTotalSizeForGoingOffline(),
        100_MB);
}

ui32 TOracleConfig::GetTimePredictionHistorySize() const
{
    return GetFromConfig(
        StorageConfig->GetOracleConfig().GetTimePredictionHistorySize(),
        0);
}

ui32 TOracleConfig::GetTimePredictionNthFromEnd() const
{
    return GetFromConfig(
        StorageConfig->GetOracleConfig().GetTimePredictionNthFromEnd(),
        0);
}

TDuration TOracleConfig::GetMaxDurationBeforeReturningOnline() const
{
    return GetFromConfig(
        StorageConfig->GetOracleConfig().GetMaxDurationBeforeReturningOnline(),
        TDuration::Seconds(10));
}

ui32 TOracleConfig::GetMinSuccessesCountBeforeReturningOnline() const
{
    return GetFromConfig(
        StorageConfig->GetOracleConfig()
            .GetMinSuccessesCountBeforeReturningOnline(),
        100);
}

}   // namespace NYdb::NBS::NBlockStore::NStorage::NPartitionDirect
