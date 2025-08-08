#pragma once

#include "distconf.h"
#include <ydb/core/cms/sentinel_impl.h>

namespace NKikimr::NStorage {

    class TStateStoragePerPileGenerator {
    public:
        TStateStoragePerPileGenerator(THashMap<TString, std::vector<std::tuple<ui32, TNodeLocation>>>& nodes,
            const std::unordered_map<ui32, ui32>& selfHealNodesState,
            TBridgePileId pileId,
            std::unordered_set<ui32>& usedNodes,
            const NKikimrConfig::TDomainsConfig::TStateStorage& oldConfig);
        bool IsGoodConfig() const;
        void AddRingGroup(NKikimrConfig::TDomainsConfig::TStateStorage *ss);

    private:
        static constexpr ui32 NodeStatesSize = (ui32)NCms::NSentinel::TNodeStatusComputer::ENodeState::BAD + 2;

        struct TNodeGroup {
            std::vector<std::tuple<ui32, TNodeLocation>> Nodes;
            std::array<ui32, NodeStatesSize> State;
            bool Disconnected;
        };

        void FillNodeGroups(THashMap<TString, std::vector<std::tuple<ui32, TNodeLocation>>>& nodes);
        void CalculateRingsParameters();
        bool PickNodesSimpleStrategy(TNodeGroup& group, ui32 stateLimit, bool ignoreRacks);
        bool PickOldNodesStrategy(TNodeGroup& group);
        void PickNodes(TNodeGroup& group);
        ui32 CalcNodeState(ui32 nodeId, bool disconnected) const;

        const TBridgePileId PileId;
        const std::unordered_map<ui32, ui32>& SelfHealNodesState;
        std::vector<TNodeGroup> NodeGroups;
        std::unordered_set<ui32>& UsedNodes;
        const NKikimrConfig::TDomainsConfig::TStateStorage& OldConfig;
        std::vector<std::vector<ui32>> Rings;
        ui32 RingsInGroupCount = 1;
        ui32 ReplicasInRingCount = 1;
        ui32 NToSelect = 1;
    };
}
