#include "batch_operations_helpers.h"

namespace NKikimr::NKqp::NBatchOperations {

// TBatchOperationPrunerSettings MakeBatchOperationPrunerSettings(TMaybe<TKeyDesc::TPartitionRangeInfo> , TMaybe<TKeyDesc::TPartitionRangeInfo> ) {
//     return TBatchOperationPrunerSettings{};
// }

TBatchOperationSettings SetBatchOperationSettings(const NKikimrConfig::TTableServiceConfig::TBatchOperationSettings& settings) {
    TBatchOperationSettings res;

    res.MaxBatchSize = settings.GetMaxBatchSize();
    res.MinBatchSize = settings.GetMinBatchSize();
    res.MaxRetryDelayMs = settings.GetMaxRetryDelayMs();
    res.StartRetryDelayMs = settings.GetStartRetryDelayMs();
    res.PartitionExecutionLimit = settings.GetPartitionExecutionLimit();

    return res;
}

} // namespace NKikimr::NKqp::NBatchOperations
