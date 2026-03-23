#pragma once

#include "sketch.h"

#include <util/generic/vector.h>
#include <util/generic/yexception.h>
#include <util/system/types.h>

#include <random>

namespace NKikimr::NKll {

template <class T>
class TWindowedKll {
public:
    TWindowedKll(size_t k, ui64 windowSec, ui64 bucketSec, ui64 seed = std::random_device{}())
        : K_(k)
        , WindowSec_(windowSec)
        , BucketSec_(bucketSec)
        , NumBuckets_((windowSec + bucketSec - 1) / bucketSec)
        , Seed_(seed)
    {
        Y_ENSURE(WindowSec_ > 0, "windowSec must be > 0");
        Y_ENSURE(BucketSec_ > 0, "bucketSec must be > 0");
        Y_ENSURE(NumBuckets_ > 0, "NumBuckets must be > 0");

        Buckets_.reserve(NumBuckets_);
        for (size_t i = 0; i < NumBuckets_; ++i) {
            Buckets_.push_back(TBucket{TKllSketch<T>(K_, Seed_ + i), 0, false});
        }
    }

    // tsSec: текущее время в секундах (или "номер секунды" монотонный)
    void Add(const T& key, ui64 tsSec) {
        const ui64 bucketStart = (tsSec / BucketSec_) * BucketSec_;
        const size_t idx = static_cast<size_t>((tsSec / BucketSec_) % NumBuckets_);

        auto& b = Buckets_[idx];
        if (!b.Valid || b.StartSec != bucketStart) {
            b.Sketch.Clear();
            b.StartSec = bucketStart;
            b.Valid = true;
        }

        b.Sketch.Add(key);
    }

    T MedianInWindow(ui64 tsSec) const {
        TKllSketch<T> tmp(K_, Seed_ ^ 0x9e3779b97f4a7c15ULL);

        const ui64 nowBucketStart = (tsSec / BucketSec_) * BucketSec_;
        const ui64 windowStart = (tsSec >= WindowSec_) ? (tsSec - WindowSec_ + 1) : 0;
        const ui64 minBucketStart = (windowStart / BucketSec_) * BucketSec_;

        for (const auto& b : Buckets_) {
            if (!b.Valid) continue;
            if (b.StartSec >= minBucketStart && b.StartSec <= nowBucketStart) {
                tmp.Merge(b.Sketch);
            }
        }

        Y_ENSURE(tmp.Count() > 0, "MedianInWindow(): window is empty");
        return tmp.Median();
    }

private:
    struct TBucket {
        TKllSketch<T> Sketch;
        ui64 StartSec;
        bool Valid;
    };

private:
    const size_t K_;
    const ui64 WindowSec_;
    const ui64 BucketSec_;
    const size_t NumBuckets_;
    const ui64 Seed_;

    TVector<TBucket> Buckets_;
};
    

} // namespace NKikimr::NKll
