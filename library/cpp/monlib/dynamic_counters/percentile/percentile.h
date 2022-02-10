#pragma once

#include "percentile_base.h"

namespace NMonitoring {

////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
// Percentile tracker for monitoring
////////////////////////////////////////////////////////////////////////////////////////////////////////////////////////

template <size_t BUCKET_SIZE, size_t BUCKET_COUNT, size_t FRAME_COUNT>
struct TPercentileTracker : public TPercentileBase {
    TAtomic Items[BUCKET_COUNT];
    TAtomicBase Frame[FRAME_COUNT][BUCKET_COUNT];
    size_t CurrentFrame;

    TPercentileTracker()
        : CurrentFrame(0)
    {
        for (size_t i = 0; i < BUCKET_COUNT; ++i) {
            AtomicSet(Items[i], 0);
        }
        for (size_t frame = 0; frame < FRAME_COUNT; ++frame) {
            for (size_t bucket = 0; bucket < BUCKET_COUNT; ++bucket) {
                Frame[frame][bucket] = 0;
            }
        }
    }

    void Increment(size_t value) {
        AtomicIncrement(Items[Min((value + BUCKET_SIZE - 1) / BUCKET_SIZE, BUCKET_COUNT - 1)]);
    }

    // shift frame (call periodically)
    void Update() {
        TVector<TAtomicBase> totals(BUCKET_COUNT);
        totals.resize(BUCKET_COUNT);
        TAtomicBase total = 0;
        for (size_t i = 0; i < BUCKET_COUNT; ++i) {
            TAtomicBase item = AtomicGet(Items[i]);
            TAtomicBase prevItem = Frame[CurrentFrame][i];
            Frame[CurrentFrame][i] = item;
            total += item - prevItem;
            totals[i] = total;
        }

        for (size_t i = 0; i < Percentiles.size(); ++i) {
            TPercentile &percentile = Percentiles[i];
            auto threshold = (TAtomicBase)(percentile.first * (float)total);
            threshold = Min(threshold, total);
            auto it = LowerBound(totals.begin(), totals.end(), threshold);
            size_t index = it - totals.begin();
            (*percentile.second) = index * BUCKET_SIZE;
        }
        CurrentFrame = (CurrentFrame + 1) % FRAME_COUNT;
    }
};

} // NMonitoring
