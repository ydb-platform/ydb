#include "index_access_stub.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>
#include <ydb/core/tx/columnshard/engines/scheme/versions/versioned_index.h>
#include <ydb/core/tx/columnshard/engines/storage/indexes/skip_index/meta.h>

namespace NKikimr::NOlap {

double TDefaultIndexAccessStub::RegisterPortion(ui64 portionId, const TIndexData& indexData) {
    AFL_VERIFY(indexData.Data.size() > 0);
    AFL_VERIFY(VersionedIndex)("error", "TVersionedIndex is required for RegisterPortion");

    auto schema = VersionedIndex->GetSchemaVerified(indexData.SchemaVersion);
    auto indexMeta = schema->GetIndexVerified(indexData.IndexId);
    auto* skipBitmapIndex = dynamic_cast<const NIndexes::TSkipBitmapIndex*>(indexMeta.GetObjectPtr().get());
    AFL_VERIFY(skipBitmapIndex)("error", "index is not a TSkipBitmapIndex")("index_id", indexData.IndexId);

    double result = 0;
    if (CurrentCounter == 0) {
        Storages.push_back(skipBitmapIndex->GetBitsStorageConstructor()->Build(indexData.Data).GetResult());
    } else {
        result = Storages.back()->Or(indexData.Data);
    }
    AFL_VERIFY(PortionId2Position.emplace(portionId, Storages.size() - 1).second);
    AFL_VERIFY(PortionId2IndexId.emplace(portionId, indexData.IndexId).second);
    AFL_VERIFY(Storages.back()->GetBitsCount() > 0);
    ++CurrentCounter;
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("register_portion_hier_index", portionId)("fraction", result)("index_id", indexData.IndexId);
    if (CurrentCounter == PortionsPerNode) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("close_index", portionId)("fraction", result);
        CurrentCounter = 0;
    }
    return result;
}

bool TDefaultIndexAccessStub::CheckValue(ui64 portionId, ui64 schemaVersion,
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
    AFL_VERIFY(PortionId2IndexId.contains(portionId));
    AFL_VERIFY(VersionedIndex)("error", "TVersionedIndex is required for CheckValue");

    const ui32 indexId = PortionId2IndexId[portionId];

    auto schema = VersionedIndex->GetSchemaVerified(schemaVersion);
    auto indexMeta = schema->GetIndexVerified(indexId);
    auto* skipIndex = dynamic_cast<const NIndexes::TSkipIndex*>(indexMeta.GetObjectPtr().get());
    AFL_VERIFY(skipIndex)("error", "index is not a TSkipIndex")("index_id", indexId);

    const TString serializedData = Storages[PortionId2Position[portionId]]->SerializeToString();
    bool result = skipIndex->CheckValue(serializedData, std::nullopt, value, operation, schema->GetIndexInfo());

    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("hier_portion_checked", portionId)("result", result)("index_id", indexId);
    return result;
}

}  // namespace NKikimr::NOlap
