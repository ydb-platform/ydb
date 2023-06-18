#include "result.h"

namespace NKikimr::NOlap::NIndexedReader {

TGranule::TPtr TResultController::ExtractFirst() {
    TGranule::TPtr result;
    if (GranulesToOut.size()) {
        result = GranulesToOut.begin()->second;
        GranulesToOut.erase(GranulesToOut.begin());
    }
    return result;
}

void TResultController::AddResult(TGranule::TPtr granule) {
    Y_VERIFY(GranulesToOut.emplace(granule->GetGranuleId(), granule).second);
    Y_VERIFY(ReadyGranulesAccumulator.emplace(granule->GetGranuleId()).second);
    BlobsSize += granule->GetBlobsDataSize();
}

TGranule::TPtr TResultController::ExtractResult(const ui64 granuleId) {
    auto it = GranulesToOut.find(granuleId);
    if (it == GranulesToOut.end()) {
        return nullptr;
    }
    TGranule::TPtr result = it->second;
    GranulesToOut.erase(it);
    BlobsSize -= result->GetBlobsDataSize();
    Y_VERIFY(BlobsSize >= 0);
    return result;
}

}
