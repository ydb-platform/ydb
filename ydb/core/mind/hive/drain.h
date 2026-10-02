#pragma once

#include <util/generic/overloaded.h>

#include "hive_impl.h"

namespace NKikimr::NHive {

struct TGetNodes {
private:
    THive* const Hive;
    std::unordered_set<TNodeId> Node;

public:
    explicit TGetNodes(THive* Hive) 
        : Hive(Hive)
    {}

    const std::unordered_set<TNodeId>& operator()(TNodeId nodeId) {
        Node = {nodeId};
        return Node;
    }

    const std::unordered_set<TNodeId>& operator()(TBridgePileId pileId) {
        return Hive->GetPile(pileId).Nodes;
    }
};

} // namespace NKikimr::NHive

Y_DECLARE_OUT_SPEC(inline, NKikimr::NHive::TDrainTarget, o, drainTarget) {
    std::visit(TOverloaded{
        [&](NKikimr::NHive::TNodeId nodeId) { o << "node " << nodeId; },
        [&](NKikimr::TBridgePileId pileId) { o << "pile " << pileId; }
    }, drainTarget);
}
