#include "batch_operation_settings.h"

namespace NKikimr::NKqp {

TBatchOperationSettings SetBatchOperationSettings(const NKikimrConfig::TTableServiceConfig::TBatchOperationSettings& settings) {
    TBatchOperationSettings res;

    res.MaxBatchSize = settings.GetMaxBatchSize();
    res.MinBatchSize = settings.GetMinBatchSize();
    res.MaxRetryDelayMs = settings.GetMaxRetryDelayMs();
    res.StartRetryDelayMs = settings.GetStartRetryDelayMs();
    res.PartitionExecutionLimit = settings.GetPartitionExecutionLimit();

    return res;
}

} // namespace NKikimr::NKqp
