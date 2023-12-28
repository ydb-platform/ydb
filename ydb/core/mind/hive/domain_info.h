#pragma once

#include "hive.h"

namespace NKikimr {
namespace NHive {

enum class ENodeSelectionPolicy : ui32 {
    Default,
    PreferObjectDomain,
};

struct TDomainInfo {
    TString Path;
    TTabletId HiveId = 0;
    TMaybeServerlessComputeResourcesMode ServerlessComputeResourcesMode;

    ENodeSelectionPolicy GetNodeSelectionPolicy() const;
};

} // NHive
} // NKikimr
