#include "default.h"
#include <ydb/core/tx/columnshard/engines/reader/filling_context.h>

namespace NKikimr::NOlap::NIndexedReader {

void TAnySorting::DoFill(TGranulesFillingContext& context) {
    for (auto&& granule : ReadMetadata->SelectInfo->GetGranulesOrdered(ReadMetadata->IsDescSorted())) {
        TGranule::TPtr g = context.GetGranuleVerified(granule.Granule);
        GranulesOutOrder.emplace_back(g);
    }
}

std::vector<TGranule::TPtr> TAnySorting::DoDetachReadyGranules(TResultController& granulesToOut) {
    std::vector<TGranule::TPtr> result;
    while (GranulesOutOrder.size()) {
        NIndexedReader::TGranule::TPtr granule = GranulesOutOrder.front();
        if (!granule->IsReady()) {
            break;
        }
        result.emplace_back(granule);
        Y_VERIFY(granulesToOut.ExtractResult(granule->GetGranuleId()));
        GranulesOutOrder.pop_front();
    }
    return result;
}

}
