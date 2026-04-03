#include "index_access_stub.h"

#include <ydb/core/tx/columnshard/engines/scheme/index_info.h>

namespace NKikimr::NOlap {

double TDefaultIndexAccessStub::RegisterPortion(ui64 portionId, const TIndexData& indexData) {
    AFL_VERIFY(indexData.Data.size() > 0);
    double result = 0;
    if (CurrentCounter == 0) {
        Storages.push_back(Constructor->Build(indexData.Data).GetResult());
    } else {
        result = Storages.back()->Or(indexData.Data);
    }
    AFL_VERIFY(PortionId2Position.emplace(portionId, Storages.size() - 1).second);
    AFL_VERIFY(PortionId2NGrammSize.emplace(portionId, indexData.NGrammSize).second);
    AFL_VERIFY(Storages.back()->GetBitsCount() > 0);
    ++CurrentCounter;
    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("register_portion_hier_index", portionId)("fraction", result)("ngramm_size", indexData.NGrammSize);
    if (CurrentCounter == PortionsPerNode) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("close_index", portionId)("fraction", result);
        CurrentCounter = 0;
    }
    return result;
}

bool TDefaultIndexAccessStub::CheckValue(ui64 portionId, const std::shared_ptr<arrow::Scalar>& value,
    const NKikimr::NArrow::NSSA::TIndexCheckOperation& operation) {
    if (PortionsWithoutIndex.contains(portionId)) {
        AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("hier_portion_without_index", portionId);
      return true;
    }
    // AFL_VERIFY(PortionId2Position.contains(portionId));
    if (!PortionId2Position.contains(portionId)) {
      AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("hier_portion_not_contained", portionId);
      return true;
    }
    AFL_VERIFY(PortionId2Position[portionId] < Storages.size());
    AFL_VERIFY(PortionId2NGrammSize.contains(portionId));

    ui32 nGrammSize = PortionId2NGrammSize[portionId];

    bool result = false;
    switch (nGrammSize) {
        case 3:
            result = Index3->DoCheckValueImpl(*Storages[PortionId2Position[portionId]], std::nullopt, value,
                operation, TIndexInfo());
            break;
        case 4:
            result = Index4->DoCheckValueImpl(*Storages[PortionId2Position[portionId]], std::nullopt, value,
                operation, TIndexInfo());
            break;
        case 5:
            result = Index5->DoCheckValueImpl(*Storages[PortionId2Position[portionId]], std::nullopt, value,
                operation, TIndexInfo());
            break;
        default:
            AFL_VERIFY(false)("unexpected_ngramm_size", nGrammSize);
    }

    AFL_ERROR(NKikimrServices::TX_COLUMNSHARD_SCAN)("hier_portion_checked", portionId)("result", result)("ngramm_size", nGrammSize);
    return result;
}

}  // namespace NKikimr::NOlap
