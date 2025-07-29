#include "distconf.h"
#include "distconf_statestorage_config_generator.h"

#include <ydb/core/mind/bscontroller/group_geometry_info.h>
#include <ydb/library/yaml_config/yaml_config_helpers.h>
#include <ydb/library/yaml_json/yaml_to_json.h>
#include <library/cpp/streams/zstd/zstd.h>

namespace NKikimr::NStorage {

    TStateStoragePerPileGenerator::TStateStoragePerPileGenerator(THashMap<TString, std::vector<std::tuple<ui32, TNodeLocation>>>& nodes
        , const std::unordered_map<ui32, ui32>& selfHealNodesState
        , const std::optional<TBridgePileId>& pileId)
        : PileId(pileId)
        , SelfHealNodesState(selfHealNodesState)
    {
        FillNodeGroups(nodes);
        CalculateRingsParameters();
        for (auto& group : NodeGroups) {
            PickNodes(group);
        }
    }

    void TStateStoragePerPileGenerator::FillNodeGroups(THashMap<TString, std::vector<std::tuple<ui32, TNodeLocation>>>& nodes) {
        NodeGroups.resize(nodes.size() < 3 ? 1 : 3);
        for (auto& [_, dc] : nodes) {
            for (auto& n : dc) {
                NodeGroups[0].Nodes.emplace_back(n);
                ui32 nodeId = std::get<0>(n);
                if (SelfHealNodesState.contains(nodeId)) {
                    ui32 state = SelfHealNodesState.at(nodeId);
                    BadNodeState = Max(BadNodeState, state);
                    auto& states = NodeGroups[0].State;
                    states.resize(BadNodeState + 1);
                    states[state]++;
                }
            }
            for (auto& ng : NodeGroups) {
                ng.State.resize(BadNodeState + 1);
            }
            std::ranges::sort(NodeGroups, [&](const auto& x, const auto& y) {
                for (ui32 idx : xrange(BadNodeState + 1)) {
                    if (x.State[idx] != y.State[idx]) {
                        return x.State[idx] < y.State[idx];
                    }
                }
                return x.Nodes.size() < y.Nodes.size() || (x.Nodes.size() > 0 && x.Nodes.size() == y.Nodes.size() && std::get<0>(x.Nodes[0]) < std::get<0>(y.Nodes[0]));
            });
        }
        BadNodeState++;
        Y_ABORT_UNLESS(NodeGroups.size() > 0 && NodeGroups[0].Nodes.size() > 0);
    }

    void TStateStoragePerPileGenerator::CalculateRingsParameters() {
        ui32 minNodesInGroup = NodeGroups[0].Nodes.size();
        if (NodeGroups.size() == 1) {
            if (minNodesInGroup < 5) {
                RingsInGroupCount = minNodesInGroup;
                NToSelect = minNodesInGroup < 3 ? 1 : 3;
            } else {
                RingsInGroupCount = minNodesInGroup < 8 ? minNodesInGroup : 8;
                NToSelect = 5;
            }
            ReplicasInRingCount = 1 + minNodesInGroup / 1000;
        } else {
            RingsInGroupCount = minNodesInGroup < 3 ? 1 : 3;
            NToSelect = RingsInGroupCount < 3 ? 3 : 9;
            ui32 nodesCnt = 0;
            for (auto& n : NodeGroups) {
                nodesCnt += n.Nodes.size();
            }
            ReplicasInRingCount = 1 + nodesCnt / 1000;
            if (ReplicasInRingCount * RingsInGroupCount > minNodesInGroup) {
                ReplicasInRingCount = 1;
            }
        }
    }

    bool TStateStoragePerPileGenerator::IsGoodConfig() const {
        return GoodConfig;
    }

    void TStateStoragePerPileGenerator::AddRingGroup(NKikimrConfig::TDomainsConfig::TStateStorage *ss) {
        auto *rg = ss->AddRingGroups();
        if (PileId) {
            PileId->CopyToProto(rg, &NKikimrConfig::TDomainsConfig::TStateStorage::TRing::SetBridgePileId);
        }
        rg->SetNToSelect(NToSelect);
        for (auto &nodes : Rings) {
            auto *ring = rg->AddRing();
            for(auto nodeId : nodes) {
                ring->AddNode(nodeId);
            }
        }
    }

    bool TStateStoragePerPileGenerator::PickNodesSimpleStrategy(TNodeGroup& group, ui32 stateLimit, bool ignoreRacks) {
        auto iter = group.Nodes.begin();
        TNodeLocation location;

        std::vector<std::vector<ui32>> result;
        result.resize(RingsInGroupCount);
        for (ui32 i : xrange(RingsInGroupCount)) {
            if (iter == group.Nodes.end()) {
                return false;
            }
            std::vector<ui32> &ring = result[i];
            while (ring.size() < ReplicasInRingCount) {
                if (iter == group.Nodes.end()) {
                    return false;
                }
                ui32 nodeId = std::get<0>(*iter);
                location = std::get<1>(*iter);
                ui32 state = SelfHealNodesState.contains(nodeId) ? SelfHealNodesState.at(nodeId) : BadNodeState;
                if (state <= stateLimit) {
                    ring.push_back(nodeId);
                }
                iter++;
            }
            if (!ignoreRacks) {
                while (iter != group.Nodes.end() && std::get<1>(*iter).GetRackId() == location.GetRackId()) {
                    ++iter;
                }
            }
        }
        for (auto& r : result) {
            Rings.emplace_back(r);
        }
        return true;
    }

    void TStateStoragePerPileGenerator::PickNodes(TNodeGroup& group) {
        std::unordered_map<TString, std::vector<ui32>> rackStates;
        for (auto& n : group.Nodes) {
            auto rack = std::get<1>(n).GetRackId();
            auto nodeId = std::get<0>(n);
            ui32 state = SelfHealNodesState.contains(nodeId) ? SelfHealNodesState.at(nodeId) : BadNodeState;
            auto& rackState = rackStates[rack];
            if (rackState.empty()) {
                rackState.resize(BadNodeState + 1);
            }
            rackState[state]++;
        }

        auto compByRack = [&](const auto& x, const auto& y) {
            auto rackX = std::get<1>(x).GetRackId();
            auto rackY = std::get<1>(y).GetRackId();
            if (rackX == rackY) {
                auto nodeX = std::get<0>(x);
                auto nodeY = std::get<0>(y);
                ui32 state1 = SelfHealNodesState.contains(nodeX) ? SelfHealNodesState.at(nodeX) : BadNodeState;
                ui32 state2 = SelfHealNodesState.contains(nodeY) ? SelfHealNodesState.at(nodeY) : BadNodeState;
                return state1 < state2 || (state1 == state2 && nodeX < nodeY);
            }
            auto& rackStateX = rackStates[rackX];
            auto& rackStateY = rackStates[rackY];
            for (ui32 idx : xrange(BadNodeState + 1)) {
                if (rackStateX[idx] != rackStateY[idx]) {
                    return rackStateX[idx] > rackStateY[idx];
                }
            }
            return rackX < rackY;
        };

        std::ranges::sort(group.Nodes, compByRack);
        for (ui32 stateLimit : xrange(BadNodeState + 1)) {
            if (PickNodesSimpleStrategy(group, stateLimit, rackStates.size() < RingsInGroupCount)) {
                return;
            }
        }
        Y_ABORT_UNLESS(PickNodesSimpleStrategy(group, Max<ui32>(), true));
    }

}
