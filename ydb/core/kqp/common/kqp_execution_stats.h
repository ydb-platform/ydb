#pragma once

#include <util/system/types.h>

namespace NKikimr::NKqp {

struct TAggExecStat {
    ui64 CpuTimeMs = 0;
    ui64 DurationSeconds = 0;
    ui64 MemoryUsageBytes = 0;
    ui64 TasksCount = 0;
    ui64 InputBytes = 0;
    ui64 OutputBytes = 0;
};

} // namespace NKikimr::NKqp
