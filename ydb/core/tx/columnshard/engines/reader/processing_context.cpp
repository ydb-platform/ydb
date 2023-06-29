#include "processing_context.h"

namespace NKikimr::NOlap::NIndexedReader {

void TProcessingController::DrainNotIndexedBatches(THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>* batches) {
    if (NotIndexedBatchesInitialized) {
        Y_VERIFY(!batches);
        return;
    }
    NotIndexedBatchesInitialized = true;
    GranulesInProcessing.erase(0);
    auto granules = GranulesWaiting;
    for (auto&& [_, gPtr] : granules) {
        if (!batches) {
            gPtr->AddNotIndexedBatch(nullptr);
        } else {
            auto it = batches->find(gPtr->GetGranuleId());
            if (it == batches->end()) {
                gPtr->AddNotIndexedBatch(nullptr);
            } else {
                gPtr->AddNotIndexedBatch(it->second);
            }
            batches->erase(it);
        }
    }
}

NKikimr::NOlap::NIndexedReader::TBatch* TProcessingController::GetBatchInfo(const TBatchAddress& address) {
    auto it = GranulesWaiting.find(address.GetGranuleId());
    if (it == GranulesWaiting.end()) {
        return nullptr;
    } else {
        return &it->second->GetBatchInfo(address.GetBatchGranuleIdx());
    }
}

TGranule::TPtr TProcessingController::ExtractReadyVerified(const ui64 granuleId) {
    Y_VERIFY(NotIndexedBatchesInitialized);
    auto it = GranulesWaiting.find(granuleId);
    Y_VERIFY(it != GranulesWaiting.end());
    TGranule::TPtr result = it->second;
    GranulesInProcessing.erase(granuleId);
    BlobsSize -= result->GetBlobsDataSize();
    Y_VERIFY(BlobsSize >= 0);
    GranulesWaiting.erase(it);
    Counters.Aggregations->RemoveGranuleProcessingInfo(result->GetBlobsDataSize());
    return result;
}

TGranule::TPtr TProcessingController::GetGranuleVerified(const ui64 granuleId) {
    auto it = GranulesWaiting.find(granuleId);
    Y_VERIFY(it != GranulesWaiting.end());
    return it->second;
}

TGranule::TPtr TProcessingController::GetGranule(const ui64 granuleId) {
    auto itGranule = GranulesWaiting.find(granuleId);
    if (itGranule == GranulesWaiting.end()) {
        return nullptr;
    }
    return itGranule->second;
}

TGranule::TPtr TProcessingController::InsertGranule(TGranule::TPtr g) {
    Y_VERIFY(GranulesWaiting.emplace(g->GetGranuleId(), g).second);
    return g;
}

void TProcessingController::StartBlobProcessing(const ui64 granuleId, const TBlobRange& range) {
    Counters.Aggregations->AddGranuleProcessingBytes(range.Size);
    if (GranulesInProcessing.emplace(granuleId).second) {
        if (granuleId) {
            Y_VERIFY(GranulesWaiting.contains(granuleId));
            Counters.Aggregations->AddGranuleProcessing();
        }
    }
    if (!granuleId) {
        Y_VERIFY(!NotIndexedBatchesInitialized);
    }
    BlobsSize += range.Size;
}

void TProcessingController::Abort() {
    GranulesWaiting.clear();
    GranulesInProcessing.clear();
    Counters.Aggregations->RemoveGranuleProcessingInfo(BlobsSize);
    BlobsSize = 0;
}

NKikimr::NOlap::NIndexedReader::TBatch& TProcessingController::GetBatchInfoVerified(const TBatchAddress& address) {
    NIndexedReader::TBatch* bInfo = GetBatchInfo(address);
    Y_VERIFY(bInfo);
    return *bInfo;
}

}
