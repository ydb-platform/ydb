#include "read_context.h"
#include <library/cpp/actors/core/events.h>

namespace NKikimr::NOlap {

TReadContext::TReadContext(const NColumnShard::TDataTasksProcessorContainer& processor,
    const NColumnShard::TConcreteScanCounters& counters,
    std::shared_ptr<NOlap::TActorBasedMemoryAccesor> memoryAccessor)
    : Processor(processor)
    , Counters(counters)
    , MemoryAccessor(memoryAccessor)
{

}

void TActorBasedMemoryAccesor::DoOnBufferReady() {
    OwnerId.Send(OwnerId, new NActors::TEvents::TEvWakeup(1));
}

}
