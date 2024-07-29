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
    FEATURE_FLAG_SETTER(EnableOlapSchemaOperations)
    FEATURE_FLAG_SETTER(EnableBackgroundCompaction)
    FEATURE_FLAG_SETTER(EnableBackgroundCompactionServerless)
    FEATURE_FLAG_SETTER(EnableBorrowedSplitCompaction)
    FEATURE_FLAG_SETTER(EnableNotNullColumns)
    FEATURE_FLAG_SETTER(EnableBulkUpsertToAsyncIndexedTables)
    FEATURE_FLAG_SETTER(EnableChangefeeds)
    FEATURE_FLAG_SETTER(EnableMoveIndex)
    FEATURE_FLAG_SETTER(EnableNotNullDataColumns)
    FEATURE_FLAG_SETTER(EnableArrowFormatAtDatashard)
    FEATURE_FLAG_SETTER(EnableGrpcAudit)
    FEATURE_FLAG_SETTER(EnableChangefeedInitialScan)
    FEATURE_FLAG_SETTER(EnableDataShardGenericReadSets)
    FEATURE_FLAG_SETTER(EnableAlterDatabaseCreateHiveFirst)
    FEATURE_FLAG_SETTER(EnableDataShardVolatileTransactions)
    FEATURE_FLAG_SETTER(EnableTopicServiceTx)
    FEATURE_FLAG_SETTER(EnableTopicDiskSubDomainQuota)
    FEATURE_FLAG_SETTER(EnablePQConfigTransactionsAtSchemeShard)
    FEATURE_FLAG_SETTER(EnableScriptExecutionOperations)
    FEATURE_FLAG_SETTER(EnableExternalDataSources)
    FEATURE_FLAG_SETTER(EnableForceImmediateEffectsExecution)
    FEATURE_FLAG_SETTER(EnableTopicSplitMerge)
    FEATURE_FLAG_SETTER(EnableTempTables)
    FEATURE_FLAG_SETTER(EnableChangefeedDynamoDBStreamsFormat)
    FEATURE_FLAG_SETTER(EnableChangefeedDebeziumJsonFormat)
    FEATURE_FLAG_SETTER(EnableUniqConstraint)
    FEATURE_FLAG_SETTER(EnableTopicMessageMeta)
    FEATURE_FLAG_SETTER(EnableUuidAsPrimaryKey)
    FEATURE_FLAG_SETTER(EnableTablePgTypes)
    FEATURE_FLAG_SETTER(EnableServerlessExclusiveDynamicNodes)
    FEATURE_FLAG_SETTER(EnableAccessServiceBulkAuthorization)
    FEATURE_FLAG_SETTER(EnableAddColumsWithDefaults)
    FEATURE_FLAG_SETTER(EnableReplaceIfExistsForExternalEntities)
    FEATURE_FLAG_SETTER(EnableCMSRequestPriorities)
    FEATURE_FLAG_SETTER(EnableTableDatetime64)
    FEATURE_FLAG_SETTER(EnableResourcePools)
    FEATURE_FLAG_SETTER(EnableChangefeedsOnIndexTables)
    FEATURE_FLAG_SETTER(EnableChangeNotNullConstraint)

    #undef FEATURE_FLAG_SETTER
};

} // NKikimr
