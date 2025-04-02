#include "batch_operation_settings.h"

namespace NKikimr::NKqp {

TBatchOperationSettings SetBatchOperationSettings(const NKikimrConfig::TTableServiceConfig::TBatchOperationSettings& settings) {
    TBatchOperationSettings res;

    res.MaxBatchSize = settings.GetMaxBatchSize();
    res.MinBatchSize = settings.GetMinBatchSize();
    res.MaxRetryDelay = settings.GetMaxRetryDelay();
    res.MinRetryDelay = settings.GetMinRetryDelay();

    return res;
}

} // namespace NKikimr::NKqp
