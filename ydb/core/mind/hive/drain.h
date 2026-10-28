#pragma once

#include <util/generic/overloaded.h>

#include "hive_impl.h"

namespace NKikimr::NHive {

struct TGetNodes {
private:
    THive* const Hive;

public:
    bool SkippedNodeDrain = false;

    explicit TGetNodes(THive* Hive) 
        : Hive(Hive)
    {}

    std::unordered_set<TNodeId> operator()(TNodeId nodeId) const {
        return {nodeId};
    }

    std::unordered_set<TNodeId> operator()(TBridgePileId pileId) {
        std::unordered_set<TNodeId> nodes;
        for (const auto& [nodeId, node] : Hive->Nodes) {
            if (node.BridgePileId == pileId) {
                if (node.Drain) { // a node drain owns it; retry the pile when it ends
                    SkippedNodeDrain = true;
                } else {
                    nodes.insert(nodeId);
                }
            }
        }
        return nodes;
    }
};

} // namespace NKikimr::NHive

Y_DECLARE_OUT_SPEC(inline, NKikimr::NHive::TDrainTarget, o, drainTarget) {
    std::visit(TOverloaded{
        [&](NKikimr::NHive::TNodeId nodeId) { o << "node " << nodeId; },
        [&](NKikimr::TBridgePileId pileId) { o << "pile " << pileId; }
    }, drainTarget);
}
