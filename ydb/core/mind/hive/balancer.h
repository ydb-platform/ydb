#pragma once

#include "hive_impl.h"
#include "leader_tablet_info.h"

namespace NKikimr {
namespace NHive {

template<NKikimrConfig::THiveConfig::EHiveNodeBalanceStrategy EHiveNodeBalanceStrategy>
void BalanceNodes(std::vector<TNodeInfo*>& nodes, EResourceToBalance resourceTobalance);

template<NKikimrConfig::THiveConfig::EHiveTabletBalanceStrategy EHiveTabletBalanceStrategy>
void BalanceTablets(std::vector<TTabletInfo*>::iterator first, std::vector<TTabletInfo*>::iterator last, EResourceToBalance resourceToBalance);

template <NKikimrConfig::THiveConfig::EHiveChannelBalanceStrategy>
void BalanceChannels(std::vector<TLeaderTabletInfo::TChannel>& channels, NKikimrConfig::THiveConfig::EHiveStorageBalanceStrategy metricToBalance);

}
}
