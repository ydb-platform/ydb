#include "memory_info.h"
#include "stats.h"
#include <ydb/library/actors/core/process_stats.h>

namespace NKikimr::NMemory {

TProcessMemoryInfo TProcessMemoryInfoProvider::Get() const {
    auto allocState = NKikimr::TAllocState::Get();
    
    TProcessMemoryInfo result{
        allocState.AllocatedMemory,
        allocState.AllocatorCachesMemory,
        {}, {}, {}, {}
    };

    NActors::TProcStat procStat;
    if (procStat.Fill(getpid())) {
        result.AnonRss.emplace(procStat.AnonRss);
        if (procStat.CGroupMemLim) {
            result.CGroupLimit.emplace(procStat.CGroupMemLim);
        }
        if (procStat.MemTotal) {
            result.MemTotal.emplace(procStat.MemTotal);
            result.MemAvailable.emplace(procStat.MemAvailable);
        }
    }

    return result;
}

}
