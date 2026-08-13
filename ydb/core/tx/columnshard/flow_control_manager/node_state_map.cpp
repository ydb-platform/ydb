#include "node_state_map.h"

#include <util/generic/utility.h>

namespace NKikimr::NColumnShard::NFlowControl {

bool TNodeStateMap::MarkHot(ui32 nodeId, ui64 generation) {
    const bool firstHot = HotNodes.empty();
    ui64& stored = HotNodes[nodeId];
    stored = Max(stored, generation);
    return firstHot;
}

bool TNodeStateMap::MarkReady(ui32 nodeId, ui64 generation) {
    const bool wasHot = !HotNodes.empty();
    auto it = HotNodes.find(nodeId);
    if (it != HotNodes.end() && generation >= it->second) {
        HotNodes.erase(it);
    }
    // "Was hot and is no longer", not merely "is not hot": the overload manager re-publishes the
    // current status to every FCM once a minute, so a healthy cluster delivers READY for nodes that
    // were never in the set. Reporting those as an edge would clamp tokens and freeze growth on
    // every node once a minute.
    return wasHot && HotNodes.empty();
}

bool TNodeStateMap::IsAdmitAllowed(const TVector<ui64>& tabletIds) const {
    for (const ui64 tabletId : tabletIds) {
        const auto* nodeId = TabletToNode.FindPtr(tabletId);
        if (!nodeId) {
            continue;   // fail open for an unknown location
        }
        if (HotNodes.contains(*nodeId)) {
            return false;
        }
    }
    return true;
}

TVector<ui32> TNodeStateMap::CollectTargetNodes(const TVector<ui64>& tabletIds) const {
    THashSet<ui32> seen;
    TVector<ui32> result;
    for (const ui64 tabletId : tabletIds) {
        const auto* nodeId = TabletToNode.FindPtr(tabletId);
        if (!nodeId) {
            continue;
        }
        if (seen.insert(*nodeId).second) {
            result.push_back(*nodeId);
        }
    }
    return result;
}

TVector<ui64> TNodeStateMap::PickTabletsForRecheck(const TVector<ui64>& tabletIds, TInstant now, TDuration period) {
    TVector<ui64> result;
    for (const ui64 tabletId : tabletIds) {
        const auto* nodeId = TabletToNode.FindPtr(tabletId);
        if (!nodeId || !HotNodes.contains(*nodeId)) {
            continue;
        }
        if (RecheckInFlight.contains(tabletId)) {
            continue;
        }
        if (const auto* last = LastRecheck.FindPtr(tabletId)) {
            if (now - *last < period) {
                continue;
            }
        }
        LastRecheck[tabletId] = now;
        RecheckInFlight.insert(tabletId);
        result.push_back(tabletId);
    }
    return result;
}

}   // namespace NKikimr::NColumnShard::NFlowControl
