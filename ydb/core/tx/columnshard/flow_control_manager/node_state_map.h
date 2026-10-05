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
    // Survives READY: without it a delayed OVERLOADED with an older generation is accepted after
    // the watermark was erased and re-heats a cool node indefinitely.
    THashMap<ui32, ui64> LastGeneration;
    THashMap<ui64, ui32> TabletToNode;
    THashMap<ui64, TInstant> LastRecheck;
    THashSet<ui64> RecheckInFlight;

public:
    // The tablet-keyed maps only ever grow through the write path: nothing tells FCM that a tablet
    // it once wrote to will never be written to again, so on a long-lived node with a churning set
    // of tables they accumulate history rather than a working set. Far above any realistic fan-out,
    // so reaching it means the map has stopped describing the present. Dropping locations is safe
    // by construction — an unknown tablet fails open and is relearned from the next write.
    static constexpr size_t MaxTrackedTablets = 100'000;

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

    void SetTabletNode(ui64 tabletId, ui32 nodeId);

    // Forgets everything keyed by this tablet, not just its location: the recheck bookkeeping is
    // keyed the same way, and leaving it behind is how a map that is supposed to shrink grows.
    void ForgetTablet(ui64 tabletId);

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
