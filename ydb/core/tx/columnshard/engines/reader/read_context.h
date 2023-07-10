#pragma once
#include "conveyor_task.h"
#include <ydb/core/tx/columnshard/counters/scan.h>
#include <ydb/core/tx/columnshard/resources/memory.h>
#include <library/cpp/actors/core/actor.h>

namespace NKikimr::NOlap {

class TActorBasedMemoryAccesor: public TScanMemoryLimiter::IMemoryAccessor {
private:
    using TBase = TScanMemoryLimiter::IMemoryAccessor;
    const NActors::TActorIdentity OwnerId;
protected:
    virtual void DoOnBufferReady() override;
public:
    TActorBasedMemoryAccesor(const NActors::TActorIdentity& ownerId, const TString& limiterName)
        : TBase(TMemoryLimitersController::GetLimiter(limiterName))
        , OwnerId(ownerId) {

    }
};

class TReadContext {
private:
    YDB_ACCESSOR_DEF(NColumnShard::TDataTasksProcessorContainer, Processor);
    const NColumnShard::TConcreteScanCounters Counters;
    YDB_READONLY_DEF(std::shared_ptr<NOlap::TActorBasedMemoryAccesor>, MemoryAccessor);
public:
    const NColumnShard::TConcreteScanCounters& GetCounters() const {
        return Counters;
    }

    TReadContext(const NColumnShard::TDataTasksProcessorContainer& processor,
        const NColumnShard::TConcreteScanCounters& counters,
        std::shared_ptr<NOlap::TActorBasedMemoryAccesor> memoryAccessor
        );

    TReadContext(const NColumnShard::TConcreteScanCounters& counters)
        : Counters(counters)
    {

    }
};

}
