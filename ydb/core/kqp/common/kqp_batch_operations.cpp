#include "kqp_batch_operations.h"

namespace NKikimr::NKqp::NBatchOperations {

TSerializedTableRange MakePartitionRange(TMaybe<TKeyDesc::TPartitionRangeInfo> begin, TMaybe<TKeyDesc::TPartitionRangeInfo> end, size_t keySize) {
    TVector<TCell> tableBegin;
    TVector<TCell> tableEnd;

    bool inclusiveTableBegin = (begin) ? begin->IsInclusive : false;
    bool inclusiveTableEnd = (end) ? end->IsInclusive : false;

    if (!begin || !begin->EndKeyPrefix) {
        inclusiveTableBegin = true;
        tableBegin.resize(keySize, TCell());
    } else {
        const auto& cells = begin->EndKeyPrefix.GetCells();
        tableBegin.assign(cells.begin(), cells.end());
    }

    if (!end || !end->EndKeyPrefix) {
        inclusiveTableEnd = true;
    } else {
        const auto& cells = end->EndKeyPrefix.GetCells();
        tableEnd.assign(cells.begin(), cells.end());
    }

    return TSerializedTableRange{tableBegin, inclusiveTableBegin, tableEnd, inclusiveTableEnd};
}

TSettings ImportSettingsFromProto(const NKikimrConfig::TTableServiceConfig::TBatchOperationSettings& settings) {
    TSettings res;

    res.MaxBatchSize = settings.GetMaxBatchSize();
    res.MinBatchSize = settings.GetMinBatchSize();
    res.MaxRetryDelayMs = settings.GetMaxRetryDelayMs();
    res.StartRetryDelayMs = settings.GetStartRetryDelayMs();
    res.PartitionExecutionLimit = settings.GetPartitionExecutionLimit();

    return res;
}

} // namespace NKikimr::NKqp::NBatchOperations
