#pragma once
#include <ydb/core/tx/columnshard/engines/reader/granule.h>
#include <ydb/core/tx/columnshard/counters/scan.h>

namespace NKikimr::NOlap::NIndexedReader {

class TResultController {
protected:
    THashMap<ui64, TGranule::TPtr> GranulesToOut;
    std::set<ui64> ReadyGranulesAccumulator;
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
    }

    bool IsReady(const ui64 granuleId) const {
        return ReadyGranulesAccumulator.contains(granuleId);
    }

    ui32 GetCount() const {
        return GranulesToOut.size();
    }

    TGranule::TPtr ExtractFirst();

    void AddResult(TGranule::TPtr granule);

    TGranule::TPtr ExtractResult(const ui64 granuleId);
};

}
