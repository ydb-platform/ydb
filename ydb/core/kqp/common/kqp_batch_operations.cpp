#include "kqp_batch_operations.h"

namespace NKikimr::NKqp::NBatchOperations {

bool IsIndexSupported(NYql::TIndexDescription::EType type, bool enabledIndexStreamWrite) {
    switch (type) {
        case NYql::TIndexDescription::EType::GlobalSync:
        case NYql::TIndexDescription::EType::GlobalAsync:
            return true;
        case NYql::TIndexDescription::EType::GlobalSyncUnique:
            return enabledIndexStreamWrite;
        case NYql::TIndexDescription::EType::GlobalSyncVectorKMeansTree:
        case NYql::TIndexDescription::EType::GlobalFulltextPlain:
        case NYql::TIndexDescription::EType::GlobalFulltextRelevance:
        case NYql::TIndexDescription::EType::LocalBloomFilter:
        case NYql::TIndexDescription::EType::LocalBloomNgramFilter:
            return false;
    }
    Y_UNREACHABLE();
    return false;
}

TSerializedTableRange MakePartitionRange(TMaybe<TKeyDesc::TPartitionRangeInfo> begin, TMaybe<TKeyDesc::TPartitionRangeInfo> end, size_t keySize) {
    TVector<TCell> tableBegin;
    TVector<TCell> tableEnd;

    bool inclusiveTableBegin = !begin || begin->IsInclusive;
    bool inclusiveTableEnd = !end || end->IsInclusive;

    if (begin && begin->EndKeyPrefix) {
        const auto& cells = begin->EndKeyPrefix.GetCells();
        tableBegin.assign(cells.begin(), cells.end());
    } else {
        tableBegin.resize(keySize, TCell()); // -inf
    }

    if (end && end->EndKeyPrefix) {
        const auto& cells = end->EndKeyPrefix.GetCells();
        tableEnd.assign(cells.begin(), cells.end());
    } // else empty vector is +inf

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
