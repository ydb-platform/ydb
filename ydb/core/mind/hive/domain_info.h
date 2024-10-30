#pragma once

#include "hive.h"

namespace NKikimr {
namespace NHive {

struct TScaleRecommendation {
    ui64 Nodes = 0;
    TInstant Timestamp;
};

enum class ENodeSelectionPolicy : ui32 {
    Default,
    PreferObjectDomain,
};

struct TDomainInfo {
    TString Path;
    TTabletId HiveId = 0;
    TMaybeServerlessComputeResourcesMode ServerlessComputeResourcesMode;

    ui64 TabletsTotal = 0;
    ui64 TabletsAlive = 0;
    ui64 TabletsAliveInObjectDomain = 0;
    
    std::deque<double> AvgCpuUsageHistory;
    TMaybeFail<TScaleRecommendation> LastScaleRecommendation;

    ENodeSelectionPolicy GetNodeSelectionPolicy() const;
};

} // NHive
} // NKikimr
