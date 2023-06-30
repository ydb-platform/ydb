#include "read_context.h"
#include <library/cpp/actors/core/events.h>

namespace NKikimr::NOlap {

TReadContext::TReadContext(const NColumnShard::TDataTasksProcessorContainer& processor,
    const NColumnShard::TConcreteScanCounters& counters)
    : Processor(processor)
    , Counters(counters)
{

}

}
