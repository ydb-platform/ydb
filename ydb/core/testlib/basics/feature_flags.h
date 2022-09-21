#pragma once

#include <ydb/core/base/feature_flags.h>

namespace NKikimr {

template <typename TDerived>
class TTestFeatureFlagsHolder {
public:
    TFeatureFlags FeatureFlags;

    #define FEATURE_FLAG_SETTER(name) \
        TDerived& Set##name(std::optional<bool> value) { \
            if (value) { \
                FeatureFlags.Set##name(*value); \
            } \
            return *static_cast<TDerived*>(this); \
        }

    FEATURE_FLAG_SETTER(AllowYdbRequestsWithoutDatabase)
    FEATURE_FLAG_SETTER(EnableSystemViews)
    FEATURE_FLAG_SETTER(CheckDatabaseAccessPermission)
    FEATURE_FLAG_SETTER(EnablePersistentQueryStats)
    FEATURE_FLAG_SETTER(EnablePersistentPartitionStats)
    FEATURE_FLAG_SETTER(AllowUpdateChannelsBindingOfSolomonPartitions)
    FEATURE_FLAG_SETTER(EnableDataColumnForIndexTable)
    FEATURE_FLAG_SETTER(EnableClockGettimeForUserCpuAccounting)
    FEATURE_FLAG_SETTER(EnableAsyncIndexes)
    FEATURE_FLAG_SETTER(EnableOlapSchemaOperations)
    FEATURE_FLAG_SETTER(EnableMvccSnapshotReads)
    FEATURE_FLAG_SETTER(EnableBackgroundCompaction)
    FEATURE_FLAG_SETTER(EnableBackgroundCompactionServerless)
    FEATURE_FLAG_SETTER(EnableNotNullColumns)
    FEATURE_FLAG_SETTER(EnableTtlOnAsyncIndexedTables)
    FEATURE_FLAG_SETTER(EnableBulkUpsertToAsyncIndexedTables)
    FEATURE_FLAG_SETTER(EnableChangefeeds)
    FEATURE_FLAG_SETTER(EnableKqpSessionActor)
    FEATURE_FLAG_SETTER(EnableKqpScanQueryStreamLookup)
    FEATURE_FLAG_SETTER(EnableMoveIndex)
    FEATURE_FLAG_SETTER(EnableNotNullDataColumns)

    TDerived& SetEnableMvcc(std::optional<bool> value) {
        if (value) {
            if (*value) {
                FeatureFlags.SetEnableMvcc(NKikimrConfig::TFeatureFlags::VALUE_TRUE);
            } else {
                FeatureFlags.SetEnableMvcc(NKikimrConfig::TFeatureFlags::VALUE_FALSE);
            }
        }

        return *static_cast<TDerived*>(this);
    }

    #undef FEATURE_FLAG_SETTER
};

} // NKikimr
