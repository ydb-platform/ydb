#pragma once

#include <util/generic/overloaded.h>

#include "hive_impl.h"

namespace NKikimr::NHive {

struct TGetNodes {
private:
    THive* Hive;
    std::unordered_set<TNodeId> Node;

public:
    TGetNodes(THive* Hive) : Hive(Hive) {}

    const std::unordered_set<TNodeId>& operator()(TNodeId nodeId) {
        Node = {nodeId};
        return Node;
    }

    const std::unordered_set<TNodeId>& operator()(TBridgePileId pileId) {
        return Hive->GetPile(pileId).Nodes;
    }
};

} // namespace NKikimr::NHive

template<>
inline void Out<NKikimr::NHive::TDrainTarget>(IOutputStream& o, const NKikimr::NHive::TDrainTarget& drainTarget) {
    std::visit(TOverloaded{
        [&](NKikimr::NHive::TNodeId nodeId) { o << "node " << nodeId; },
        [&](NKikimr::TBridgePileId pileId) { o << "pile " << pileId; }
    }, drainTarget);
}
