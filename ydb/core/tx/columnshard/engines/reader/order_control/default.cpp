#include "default.h"
#include <ydb/core/tx/columnshard/engines/reader/filling_context.h>

namespace NKikimr::NOlap::NIndexedReader {

void TAnySorting::DoFill(TGranulesFillingContext& context) {
    auto granulesOrder = ReadMetadata->SelectInfo->GranulesOrder(ReadMetadata->IsDescSorted());
    for (ui64 granule : granulesOrder) {
        TGranule& g = context.GetGranuleVerified(granule);
        GranulesOutOrder.emplace_back(&g);
    }
}

std::vector<TGranule*> TAnySorting::DoDetachReadyGranules(THashMap<ui64, NIndexedReader::TGranule*>& granulesToOut) {
    std::vector<TGranule*> result;
    while (GranulesOutOrder.size()) {
        NIndexedReader::TGranule* granule = GranulesOutOrder.front();
        if (!granule->IsReady()) {
            break;
        }
        result.emplace_back(granule);
        Y_VERIFY(granulesToOut.erase(granule->GetGranuleId()));
        GranulesOutOrder.pop_front();
    }
    return result;
}

}
