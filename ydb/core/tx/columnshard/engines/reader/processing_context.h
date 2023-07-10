#pragma once
#include "granule.h"
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/resources/memory.h>

namespace NKikimr::NOlap::NIndexedReader {

class TProcessingController {
private:
    THashMap<ui64, TGranule::TPtr> GranulesWaiting;
    ui32 OriginalGranulesCount = 0;
    ui64 CommonGranuleData = 0;
    std::set<ui64> GranulesInProcessing;
    bool NotIndexedBatchesInitialized = false;
    const NColumnShard::TConcreteScanCounters Counters;
    TScanMemoryLimiter::TGuard GuardZeroGranuleData;
public:
    TString DebugString() const;
    bool IsGranuleActualForProcessing(const ui64 granuleId) const {
        return GranulesWaiting.contains(granuleId) || (granuleId == 0 && !NotIndexedBatchesInitialized);
    }

    TProcessingController(TScanMemoryLimiter::IMemoryAccessor::TPtr memoryAccessor, const NColumnShard::TConcreteScanCounters& counters)
        : Counters(counters)
        , GuardZeroGranuleData(memoryAccessor, Counters.Aggregations.GetGranulesProcessing())
    {
    }

    ~TProcessingController() {
        Abort();
    }

    void DrainNotIndexedBatches(THashMap<ui64, std::shared_ptr<arrow::RecordBatch>>* batches);

    NIndexedReader::TBatch* GetBatchInfo(const TBatchAddress& address);
    NIndexedReader::TBatch& GetBatchInfoVerified(const TBatchAddress& address);

    const std::set<ui64>& GetProcessingGranules() const {
        return GranulesInProcessing;
    }

    ui32 GetProcessingGranulesCount() const {
        return GranulesInProcessing.size();
    }

    bool IsInProgress(const ui64 granuleId) const {
        return GranulesInProcessing.contains(granuleId);
    }

    void Abort();

    ui32 GetCount() const {
        return GranulesInProcessing.size();
    }

    void StartBlobProcessing(const ui64 granuleId, const TBlobRange& range);

    TGranule::TPtr ExtractReadyVerified(const ui64 granuleId);

    TGranule::TPtr GetGranuleVerified(const ui64 granuleId) const;

    bool IsFinished() const { return GranulesWaiting.empty() && NotIndexedBatchesInitialized; }

    TGranule::TPtr InsertGranule(TGranule::TPtr g);

    TGranule::TPtr GetGranule(const ui64 granuleId) const;

};

}
