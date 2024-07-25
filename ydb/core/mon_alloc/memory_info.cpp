#include "memory_info.h"
#include "stats.h"
#include <ydb/library/actors/core/process_stats.h>

namespace NKikimr::NMemory {

TProcessMemoryInfo TProcessMemoryInfoProvider::Get() const {
    TProcessMemoryInfo result{
        NKikimr::TAllocState::GetAllocatedMemoryEstimate(),
        {}, {}
    };

    NActors::TProcStat procStat;
    if (procStat.Fill(getpid())) {
        result.AnonRss.emplace(procStat.AnonRss);
        if (procStat.CGroupMemLim) {
            result.CGroupLimit.emplace(procStat.CGroupMemLim);
        }
    }

    return result;
}

}