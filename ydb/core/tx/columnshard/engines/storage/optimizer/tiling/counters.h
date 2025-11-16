#pragma once

#include <ydb/core/tx/columnshard/engines/portions/portion_info.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/signals/owner.h>
#include <ydb/core/tx/scheme_cache/scheme_cache.h>

#include <ydb/public/api/protos/ydb_status_codes.pb.h>
#include <ydb/public/api/protos/ydb_value.pb.h>
#include <ydb/core/protos/tx_datashard.pb.h>

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

}   // namespace NKikimr
