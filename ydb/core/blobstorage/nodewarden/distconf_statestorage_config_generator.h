#pragma once

#include "distconf.h"

namespace NKikimr::NStorage {

    class TStateStoragePerPileGenerator {
    public:
        TStateStoragePerPileGenerator(THashMap<TString, std::vector<std::tuple<ui32, TNodeLocation>>>& nodes,
            const std::unordered_map<ui32, ui32>& selfHealNodesState,
            const std::optional<TBridgePileId>& pileId);
        bool IsGoodConfig() const;
        void AddRingGroup(NKikimrConfig::TDomainsConfig::TStateStorage *ss);

    private:
        struct TNodeGroup {
            std::vector<std::tuple<ui32, TNodeLocation>> Nodes;
            std::vector<ui32> State;
        };

        void FillNodeGroups(THashMap<TString, std::vector<std::tuple<ui32, TNodeLocation>>>& nodes);
        void CalculateRingsParameters();
        bool PickNodesSimpleStrategy(TNodeGroup& group, ui32 stateLimit, bool ignoreRacks);
        void PickNodes(TNodeGroup& group);

        const std::optional<TBridgePileId> PileId;
        const std::unordered_map<ui32, ui32>& SelfHealNodesState;
        std::vector<TNodeGroup> NodeGroups;
        std::unordered_map<TString, std::vector<ui32>> RackStates;
        std::vector<std::vector<ui32>> Rings;
        ui32 RingsInGroupCount = 1;
        ui32 ReplicasInRingCount = 1;
        ui32 NToSelect = 1;
        ui32 BadNodeState = 0;
        bool GoodConfig = true;
    };
}
