#pragma once

#include "hive_impl.h"

namespace NKikimr {
namespace NHive {

template<NKikimrConfig::THiveConfig::EHiveNodeBalanceStrategy EHiveNodeBalanceStrategy>
void BalanceNodes(std::vector<TNodeInfo*>& nodes, EResourceToBalance resourceTobalance);

template<NKikimrConfig::THiveConfig::EHiveTabletBalanceStrategy EHiveTabletBalanceStrategy>
void BalanceTablets(std::vector<TTabletInfo*>& tablets, EResourceToBalance resourceToBalance);

}
}
