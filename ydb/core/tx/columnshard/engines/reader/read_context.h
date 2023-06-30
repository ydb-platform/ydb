#pragma once
#include "conveyor_task.h"
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NOlap {

class TReadContext {
private:
    YDB_ACCESSOR_DEF(NColumnShard::TDataTasksProcessorContainer, Processor);
    const NColumnShard::TConcreteScanCounters Counters;
public:
    const NColumnShard::TConcreteScanCounters& GetCounters() const {
        return Counters;
    }

    TReadContext(const NColumnShard::TDataTasksProcessorContainer& processor,
        const NColumnShard::TConcreteScanCounters& counters
        );

    TReadContext(const NColumnShard::TConcreteScanCounters& counters)
        : Counters(counters)
    {

    }
};

}
