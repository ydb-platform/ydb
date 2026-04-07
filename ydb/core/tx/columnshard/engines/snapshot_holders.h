#pragma once

#include "portions/portion_info.h"

#include <algorithm>

namespace NKikimr::NOlap {

/**
The life looks like this:
time ---------[----------------------------------------]--------->
        ^scan1  ^minSnapshotForNewReads  ^scan2 ^scan3   ^now

In this class, we need only minSnapshotForNewReads and scan1.
We do not need scan2 and scan3 because we consider the whole range [minSnapshotForNewReads, now] as "in flight",
because any number of new scans may come there.
*/
class TSnapshotHolders {
    // It is the oldest snapshot new scans can start at
    const TSnapshot MinSnapshotForNewReads;
    // Sorted from older to younger
    const std::vector<TSnapshot> TxInFlight;

public:
    TSnapshotHolders(TSnapshot minSnapshotForNewReads, std::vector<TSnapshot> txInFlight)
        : MinSnapshotForNewReads(std::move(minSnapshotForNewReads))
        , TxInFlight(std::move(txInFlight)) {
        AFL_VERIFY(std::is_sorted(TxInFlight.begin(), TxInFlight.end()));
        if (!TxInFlight.empty()) {
            AFL_VERIFY(TxInFlight.back() < MinSnapshotForNewReads);
        }
    }

    TSnapshot GetMinSnapshotForNewReads() const {
        return MinSnapshotForNewReads;
    }

    bool CouldUsePortion(const TPortionInfo::TConstPtr& portion) const {
        return CouldUse(
            [&portion](const TSnapshot& snapshot) { return portion->IsRemovedFor(snapshot); },
            [&portion](const TSnapshot& snapshot) { return portion->IsVisible(snapshot, true); });
    }

    template <class TIsRemovedFor, class TIsVisible>
    bool CouldUse(const TIsRemovedFor& isRemovedFor, const TIsVisible& isVisible) const {
        // The object can be used by new scans.
        if (!isRemovedFor(MinSnapshotForNewReads)) {
            return true;
        }

        // Loop invariant: all txs older than txSnapshot couldn't use the object.
        for (const auto& txSnapshot : TxInFlight) {
            // This and all younger txs cannot see it.
            if (isRemovedFor(txSnapshot)) {
                return false;
            }
            // This tx could use it.
            if (isVisible(txSnapshot)) {
                return true;
            }
            // Current tx could not use it, maybe the next tx could.
        }
        // We have not found txs that could use it.
        return false;
    }
};

}   // namespace NKikimr::NOlap
