#pragma once

#include <util/datetime/base.h>

namespace NKikimr {

struct TCountedLeakyBucket {
    double BucketSize; // e.g. <1000> per hour
    TDuration BucketDuration; // e.g. 1000 per <hour>
    double Available; // available quota (may be larger than bucket size)
    TInstant LastUpdate;
    mutable bool Dirty = false;

    TCountedLeakyBucket() = default;

    TCountedLeakyBucket(double bucketSize, TDuration bucketDuration, TInstant now)
        : BucketSize(bucketSize)
        , BucketDuration(bucketDuration)
        , Available(bucketSize)
        , LastUpdate(now)
        , Dirty(true)
    { }

    bool IsFull() const {
        return Available >= BucketSize;
    }

    bool CanPush(double val) {
        if (Available < val) return false;
        return true;
    }

    bool TryPush(TInstant now, double val) {
        if (Available < val) {
            return false;
        } else {
            Push(now, val);
            return true;
        }
    }

    void Push(TInstant now, double val) {
        // We want full bucket to start filling after the decrement
        // Otherwise it might retroactively fill what we just consumed
        if (IsFull()) {
            LastUpdate = now;
        }

        Available -= val;
        Dirty = true;
    }

    void Update(TInstant now) {
        if (Y_UNLIKELY(now < LastUpdate)) {
            // If we ever detect time going backwards just remember current time
            LastUpdate = now;
            Dirty = true;
            return;
        }

        TDuration dt = now - LastUpdate;
        if (!dt || IsFull()) {
            // Don't update if bucket is full or no time has passed
            return;
        }

        // x = x0 + s * dt
        // where s = BucketSize / BucketDuration
        // x = BucketSize will happen at
        // max(dt) = BucketDuration * (1 - x0 / BucketSize)
        TDuration maxdt = BucketDuration * (1.0 - Available / BucketSize);
        if (dt >= maxdt) {
            Available = BucketSize;
        } else {
            double dx = BucketSize * (dt / BucketDuration);
            if (dx < 0.25) {
                // Avoid accumulating large errors from frequest small updates
                return;
            }
            Available += dx;
            if (Available > BucketSize) {
                Available = BucketSize;
            }
        }
        LastUpdate = now;
        Dirty = true;
    }
};


} // namespace NKikimr
