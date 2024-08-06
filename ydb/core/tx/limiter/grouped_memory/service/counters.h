#pragma once
#include <ydb/core/tx/columnshard/counters/common/owner.h>

namespace NKikimr::NOlap::NGroupedMemoryManager {

class TCounters: public NColumnShard::TCommonCountersOwner {
private:
    using TBase = NColumnShard::TCommonCountersOwner;

public:
    using TBase::TBase;
};

}
