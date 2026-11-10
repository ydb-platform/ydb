#pragma once

#include "barrier.h"

#include <util/generic/hash_set.h>
#include <util/generic/vector.h>

namespace NKikimr::NCdcAggregator {

// Parts of the stream changed by one command. The repository persists exactly
// these parts; a command without changes does not write anything.
struct TStreamChanges {
    // Configuration or phase.
    bool Config = false;
    // Sources added to the set or with a new progress.
    THashSet<TSourceId> UpsertedSources;
    // Sources removed from the set by a replacement.
    THashSet<TSourceId> ErasedSources;
    bool ReadyBarrier = false;
    // Parents of new replacements.
    TVector<TSourceId> AddedReplacements;

    bool IsEmpty() const {
        return !Config
            && UpsertedSources.empty()
            && ErasedSources.empty()
            && !ReadyBarrier
            && AddedReplacements.empty();
    }
};

} // namespace NKikimr::NCdcAggregator
