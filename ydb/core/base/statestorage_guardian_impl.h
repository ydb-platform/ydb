#pragma once
#include "defs.h"
#include <util/generic/map.h>
#include <util/generic/algorithm.h>

namespace NKikimr {
namespace NStateStorageGuardian {

struct TFollowerTracker {
    TVector<TVector<TActorId>> Reported; // reported followers by replica index
    TMap<TActorId, ui32> Merged; // follower -> referenced by

    bool AddMerged(TActorId x) {
        auto itPair = Merged.emplace(x, 1);
        if (itPair.second) {
            return true;
        } else {
            itPair.first->second++;
            return false;
        }
    }

    bool DelMerged(TActorId x) {
        auto it = Merged.find(x);
        Y_ABORT_UNLESS(it != Merged.end(), "follower tracker consistency broken");

        if (it->second == 1) {
            Merged.erase(it);
            return true;
        } else {
            it->second--;
            return false;
        }
    }
public:
    TFollowerTracker(ui32 replicas)
        : Reported(replicas)
    {}

    const TMap<TActorId, ui32>& GetMerged() const {
        return Merged;
    }

    // update reported list for replica, returns true if smth changed
    // reported must be sorted
    bool Merge(ui32 replicaIdx, TVector<TActorId> &reported) {
        bool changed = false;

        TVector<TActorId> &old = Reported[replicaIdx];
        const ui32 oldSz = old.size();
        bool gotDuplicates = false;

        ui32 oldIdx = 0;
        TActorId prevReported;
        for (TActorId x : reported) {
            if (x == prevReported) { // skip duplicated
                gotDuplicates = true;
                continue;
            }
            prevReported = x;

            // every x must be kept in merged

            while (oldIdx < oldSz && old[oldIdx] < x) {
                changed |= DelMerged(old[oldIdx]);
                ++oldIdx;
            }

            if (oldIdx < oldSz && old[oldIdx] == x) {
                ++oldIdx;
                // do nothing
            } else {
                // new entries
                changed |= AddMerged(x);
            }
        }

        // erase old tail
        for (; oldIdx < oldSz; ++oldIdx) {
            changed |= DelMerged(old[oldIdx]);
        }

        if (gotDuplicates)
            reported.erase(Unique(reported.begin(), reported.end()), reported.end());

        Reported[replicaIdx].swap(reported);
        return changed;
    }
};

}}
