#include "accessor.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>

namespace NKikimr::NOlap::NIndexes::NHierarchical {

double TDefaultAccessor::RegisterPortion(ui64 portionId, const TIndexData& indexData) {
    AFL_VERIFY(indexData.Data.size() > 0);
    AFL_VERIFY(indexData.IndexMeta)("error", "IndexMeta is required for RegisterPortion");

    auto* skipBitmapIndex = dynamic_cast<const NIndexes::TSkipBitmapIndex*>(indexData.IndexMeta.get());
    AFL_VERIFY(skipBitmapIndex)("error", "index is not a TSkipBitmapIndex")("index_id", indexData.IndexId);

    double result = 0;
    if (CurrentCounter == 0) {
        Storages.push_back(skipBitmapIndex->GetBitsStorageConstructor()->Build(indexData.Data).GetResult());
    } else {
        result = Storages.back()->Or(indexData.Data);
    }
    AFL_VERIFY(PortionId2Position.emplace(portionId, Storages.size() - 1).second);
    AFL_VERIFY(Storages.back()->GetBitsCount() > 0);
    ++CurrentCounter;
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("register_portion_hier_index", portionId)("fraction", result)("index_id", indexData.IndexId);
    if (CurrentCounter == PortionsPerNode) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("close_index", portionId)("fraction", result);
        CurrentCounter = 0;
    }
    return result;
}

bool TDefaultAccessor::CheckValue(ui64 portionId, const NIndexes::TSkipIndex& indexMeta, const TIndexInfo& indexInfo,
    const std::shared_ptr<arrow::Scalar>& value,
    const NKikimr::NArrow::NSSA::TIndexCheckOperation& operation) {
    if (PortionsWithoutIndex.contains(portionId)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("hier_portion_without_index", portionId);
        return true;
    }
    if (!PortionId2Position.contains(portionId)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("hier_portion_not_contained", portionId);
        return true;
    }
    AFL_VERIFY(PortionId2Position[portionId] < Storages.size());

    const TString serializedData = Storages[PortionId2Position[portionId]]->SerializeToString();
    bool result = indexMeta.CheckValue(serializedData, std::nullopt, value, operation, indexInfo);

    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("hier_portion_checked", portionId)("result", result)("index_id", indexMeta.GetIndexId());
    return result;
}

}  // namespace NKikimr::NOlap::NIndexes::NHierarchical
