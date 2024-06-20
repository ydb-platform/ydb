#pragma once

#include <util/datetime/base.h>


namespace NKikimr::NResourcePool {

struct TPoolSettings {
    ui64 ConcurrentQueryLimit = 0;  // 0 = infinity
    ui64 QueryCountLimit = 0;  // 0 = infinity
    TDuration QueryCancelAfter = TDuration::Zero();  // 0 = disabled

    double QueryMemoryLimitRatioPerNode = 100;  // Percent from node memory capacity
};

}  // namespace NKikimr::NResourcePool
