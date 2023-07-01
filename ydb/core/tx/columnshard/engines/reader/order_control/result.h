#pragma once
#include <ydb/core/tx/columnshard/engines/reader/granule.h>
#include <ydb/core/tx/columnshard/counters/scan.h>

namespace NKikimr::NOlap::NIndexedReader {

class TResultController {
protected:
    THashMap<ui64, TGranule::TPtr> GranulesToOut;
    std::set<ui64> ReadyGranulesAccumulator;
    i64 BlobsSize = 0;
    const NColumnShard::TConcreteScanCounters Counters;
public:
    TString DebugString() const {
        return TStringBuilder()
            << "to_out:" << GranulesToOut.size() << ";"
            << "ready:" << ReadyGranulesAccumulator.size() << ";"
            ;
    }

    TResultController(const NColumnShard::TConcreteScanCounters& counters)
        : Counters(counters)
    {

    }

    void Clear() {
        GranulesToOut.clear();
        BlobsSize = 0;
    }

    bool IsReady(const ui64 granuleId) const {
        return ReadyGranulesAccumulator.contains(granuleId);
    }

    ui64 GetBlobsSize() const {
        return BlobsSize;
    }

    ui32 GetCount() const {
        return GranulesToOut.size();
    }

    TGranule::TPtr ExtractFirst();

    void AddResult(TGranule::TPtr granule);

    TGranule::TPtr ExtractResult(const ui64 granuleId);
};

}
