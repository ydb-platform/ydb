#pragma once

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NColumnShard::NFlowControl {

// Where tablets live and which of those nodes are currently overloaded.
//
// The mapping is best-effort: FCM learns it from write paths and from tablet resolver replies, so
// an unknown tablet fails *open* (admitted) rather than being gated on a guess.
class TNodeStateMap {
    // nodeId -> last overload generation (present => hot)
    THashMap<ui32, ui64> HotNodes;
    THashMap<ui64, ui32> TabletToNode;
    THashMap<ui64, TInstant> LastRecheck;
    THashSet<ui64> RecheckInFlight;

public:
    bool AnyHot() const {
        return !HotNodes.empty();
    }

    size_t HotCount() const {
        return HotNodes.size();
    }

    size_t TabletCount() const {
        return TabletToNode.size();
    }

    // Returns true on the empty -> non-empty hot edge, which is what triggers a rate cut.
    bool MarkHot(ui32 nodeId, ui64 generation);
    // Ignores stale generations. Returns true only on the non-empty -> empty hot edge, i.e. when
    // this call cleared the last hot node; a READY for a node that was not hot reports nothing.
    bool MarkReady(ui32 nodeId, ui64 generation);

    void SetTabletNode(ui64 tabletId, ui32 nodeId) {
        TabletToNode[tabletId] = nodeId;
    }

    void ForgetTablet(ui64 tabletId) {
        TabletToNode.erase(tabletId);
    }

    // Every write in the request must be allowed: one hot node gates the whole request, since the
    // client cannot partially succeed. Tablets with unknown location are skipped (fail open).
    bool IsAdmitAllowed(const TVector<ui64>& tabletIds) const;
    // Distinct known nodes behind these tablets, in first-seen order.
    TVector<ui32> CollectTargetNodes(const TVector<ui64>& tabletIds) const;

    // Tablets on hot nodes whose location is worth re-resolving now: a tablet may have moved off
    // the hot node, and until we learn that, its requests keep being gated. Rate-limited per
    // tablet and deduplicated against in-flight resolves; the returned tablets are marked as
    // in-flight, so the caller must pair each one with FinishRecheck().
    TVector<ui64> PickTabletsForRecheck(const TVector<ui64>& tabletIds, TInstant now, TDuration period);

    void FinishRecheck(ui64 tabletId) {
        RecheckInFlight.erase(tabletId);
    }
};

}   // namespace NKikimr::NColumnShard::NFlowControl
