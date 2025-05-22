#pragma once

#include <util/system/types.h>

namespace NYql {

struct TGcNodeSettings {
    ui64 NodeCountThreshold = 1000;
    double CollectRatio = 0.8;
};

struct TGcNodeStatistics {
    ui64 CollectCount = 0;
    ui64 TotalCollectedNodes = 0;
};

struct TGcNodeConfig {
    TGcNodeSettings Settings;
    TGcNodeStatistics Statistics;
};

}
