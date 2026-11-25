#pragma once

#include "autopartitioning_manager.h"

namespace NKikimr::NPQ {

class TSourceIdStorage;

std::unique_ptr<IAutopartitioningManager> CreateKeyRangeAutopartitioningManager(const NKikimrPQ::TPQTabletConfig& config, ui32 partitionId,
    const TSourceIdStorage& sourceIds);

}
