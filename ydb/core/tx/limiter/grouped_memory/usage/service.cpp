#include "service.h"

#include <ydb/core/tx/limiter/grouped_memory/tracing/probes.h>

#include <limits>

namespace NKikimr::NOlap::NGroupedMemoryManager {

LWTRACE_USING(YDB_GROUPED_MEMORY_PROVIDER);

void ProbeAllocationDisabled(const ui64 identifier) {
    LWPROBE(Allocated, "disabled", identifier, "", std::numeric_limits<ui64>::max(), std::numeric_limits<ui64>::max(), 0, 0,
        TDuration::Zero(), false, true);
}

}
