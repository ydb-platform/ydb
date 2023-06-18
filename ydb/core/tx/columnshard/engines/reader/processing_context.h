#pragma once
#include "granule.h"

namespace NKikimr::NOlap::NIndexedReader {

class TProcessingController {
private:
    THashMap<ui64, TGranule::TPtr> GranulesWaiting;
    std::set<ui64> GranulesInProcessing;
    i64 BlobsSize = 0;
    bool NotIndexedBatchesInitialized = false;
public:
    void DrainNotIndexedBatches(THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>* batches);

    NIndexedReader::TBatch* GetBatchInfo(const TBatchAddress& address);

    bool IsInProgress(const ui64 granuleId) const {
        return GranulesInProcessing.contains(granuleId);
    }

    void Abort() {
        GranulesWaiting.clear();
        GranulesInProcessing.clear();
        BlobsSize = 0;
    }

    ui64 GetBlobsSize() const {
        return BlobsSize;
    }

    ui32 GetCount() const {
        return GranulesInProcessing.size();
    }

    void StartBlobProcessing(const ui64 granuleId, const TBlobRange& range) {
        GranulesInProcessing.emplace(granuleId);
        BlobsSize += range.Size;
    }

    TGranule::TPtr ExtractReadyVerified(const ui64 granuleId);

    TGranule::TPtr GetGranuleVerified(const ui64 granuleId);

    bool IsFinished() const { return GranulesWaiting.empty(); }

    TGranule::TPtr InsertGranule(TGranule::TPtr g);

    TGranule::TPtr GetGranule(const ui64 granuleId);

};

}
