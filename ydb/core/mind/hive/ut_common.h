#pragma once

#include <ydb/core/protos/counters_hive.pb.h>
#include <ydb/core/protos/tablet_counters.pb.h>
#include <ydb/core/testlib/tenant_runtime.h>

#include "hive_impl.h"

#ifdef NDEBUG
static constexpr bool ENABLE_DETAILED_HIVE_LOG = false;
#else
static constexpr bool ENABLE_DETAILED_HIVE_LOG = true;
#endif

namespace NKikimr {

NKikimrTabletBase::TEvGetCountersResponse GetCounters(TTestActorRuntime& runtime, ui64 tabletId);

ui64 GetSimpleCounter(TTestActorRuntime& runtime, ui64 tabletId,
    NHive::ESimpleCounters counter);

namespace NHive {

class TTestHive : public THive {
public:
    TTestHive(TTabletStorageInfo *info, const TActorId &tablet) : THive(info, tablet) {}

    template<typename F>
    void UpdateConfig(F func) {
        func(ClusterConfig);
        BuildCurrentConfig();
    }

    TBootQueue& GetBootQueue() {
        return BootQueue;
    }

    void MakeNodes(size_t numNodes) {
        for (TNodeId nodeId = 1; nodeId <= numNodes; ++nodeId) {
            TNodeInfo& node = Nodes.emplace(std::piecewise_construct, std::tuple<TNodeId>(nodeId), std::tuple<TNodeId, THive&>(nodeId, *this)).first->second;
            node.ServicedDomains.push_back({1, 2});
            node.Local = TActorId(nodeId, "LOCAL");
            node.ChangeVolatileState(TNodeInfo::EVolatileState::Connected);
            node.LocationAcquired = true;
            NKikimrLocal::TTabletAvailability dummyTabletAvailability;
            dummyTabletAvailability.SetType(TTabletTypes::Dummy);
            dummyTabletAvailability.SetPriority(nodeId % 3);
            node.TabletAvailability.emplace(std::piecewise_construct,
                                            std::tuple<TTabletTypes::EType>(TTabletTypes::Dummy),
                                            std::tuple<NKikimrLocal::TTabletAvailability>(dummyTabletAvailability));
        }
    }

    void MakeTablets(size_t numTablets) {
        for (TTabletId i = 1; i <= numTablets; i++) {
            TLeaderTabletInfo& tablet = Tablets.emplace(std::piecewise_construct, std::tuple<TTabletId>(i), std::tuple<TTabletId, THive&>(i, *this)).first->second;
            tablet.SetType(TTabletTypes::Dummy);
            tablet.AssignDomains({1, 1}, {});
            tablet.Weight = RandomNumber<double>();
            tablet.ChangeVolatileState(TTabletInfo::EVolatileState::TABLET_VOLATILE_STATE_BOOTING);
            BootQueue.AddToBootQueue(tablet, 0);
        }
    }

};

} // NHive
} // NKikimr
