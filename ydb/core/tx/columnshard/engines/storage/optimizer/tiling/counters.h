#pragma once

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>

namespace NKikimr {
class ITilingCounter: public NColumnShard::TCommonCountersOwner {
    using TBase = NColumnShard::TCommonCountersOwner;

    NMonitoring::TDynamicCounters::TCounterPtr Count;
    NMonitoring::TDynamicCounters::TCounterPtr BlobBytes;
    NMonitoring::TDynamicCounters::TCounterPtr RawBytes;
    NMonitoring::TDynamicCounters::TCounterPtr RecordsCount;

public:
    ITilingCounter(ui32 i, bool isLevel);
    void AddPortion(const std::shared_ptr<const NOlap::TPortionInfo>& p);
    void RemovePortion(const std::shared_ptr<const NOlap::TPortionInfo>& p);
};

class TTilingLevelCounter : public ITilingCounter {
    using TBase = ITilingCounter;

    NMonitoring::TDynamicCounters::TCounterPtr IntersectionsCount;

public:
    TTilingLevelCounter(ui32 i);

    void SetIntersections(ui32 intersections);
};

}  // namespace NKikimr
