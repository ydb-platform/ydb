#pragma once

#include <util/generic/algorithm.h>
#include <util/system/types.h>

namespace NActors {

    // Power-of-two scratch allocation for one produce stream (copied bytes only, not aliased
    // payloads). The target starts at min and grows when a produce actually fills it; it shrinks
    // only if we offered a full-target slab and used little of it. Budget-limited produces must
    // not shrink the target (the socket was not given a chance to need more scratch).
    class TScratchTarget {
        size_t Target = 0;
        size_t MinSize = 0;
        size_t MaxSize = 0;

    public:
        TScratchTarget() = default;

        TScratchTarget(size_t minSize, size_t maxSize)
            : Target(minSize)
            , MinSize(minSize)
            , MaxSize(Max(minSize, maxSize))
        {}

        size_t GetSize() const {
            return Target;
        }

        size_t GetMinSize() const {
            return MinSize;
        }

        void SetMaxSize(size_t maxSize) {
            MaxSize = Max(MinSize, maxSize);
            Target = Min(Target, MaxSize);
        }

        size_t AllocSize(size_t remainingBudget) const {
            return Min(Target, Max(MinSize, remainingBudget));
        }

        void OnProduce(size_t bytesCopied, bool offeredFullTarget) {
            if (bytesCopied >= Target && Target < MaxSize) {
                Target = Min(Target * 2, MaxSize);
            } else if (offeredFullTarget && bytesCopied * 2 < Target && Target > MinSize) {
                Target = Max(Target / 2, MinSize);
            }
        }
    };

    // Main-socket read size. Grow when a full-target advertisement comes back at least 3/4 full.
    // Shrink only after several consecutive small completions, so a single short TCP read cannot
    // collapse the buffer. Filling a leftover smaller than the target does not grow. The provided-
    // buffer pool requires several large hits before the session graduates to a private buffer.
    class TReadTarget {
        size_t Target = 0;
        size_t MinSize = 0;
        size_t MaxSize = 0;
        ui32 SmallCompletions = 0;
        ui32 FullPoolReads = 0;

    public:
        static constexpr ui32 SmallCompletionsToShrink = 3;
        static constexpr ui32 PoolReadsToGraduate = 3;
        static constexpr size_t GrowNumerator = 3;
        static constexpr size_t GrowDenominator = 4;

        TReadTarget() = default;

        TReadTarget(size_t minSize, size_t maxSize)
            : Target(minSize)
            , MinSize(minSize)
            , MaxSize(Max(minSize, maxSize))
        {}

        size_t GetSize() const {
            return Target;
        }

        size_t GetMinSize() const {
            return MinSize;
        }

        bool AtMinimum() const {
            return Target == MinSize;
        }

        size_t Advertise(size_t available) const {
            return Min(available, Target);
        }

        // Returns true if the leftover private buffer should be dropped (back to the pool).
        bool OnCompletion(size_t num, size_t requested) {
            if (requested >= Target && num >= (requested * GrowNumerator) / GrowDenominator) {
                SmallCompletions = 0;
                if (Target < MaxSize) {
                    Target = Min(Target * 2, MaxSize);
                }
                return false;
            }
            if (requested >= Target && num * 2 < requested && Target > MinSize) {
                if (++SmallCompletions >= SmallCompletionsToShrink) {
                    SmallCompletions = 0;
                    Target = Max(Target / 2, MinSize);
                    return Target == MinSize;
                }
                return false;
            }
            SmallCompletions = 0;
            return false;
        }

        void OnPoolCompletion(size_t num, size_t poolBufSize) {
            if (num >= (poolBufSize * GrowNumerator) / GrowDenominator) {
                if (++FullPoolReads >= PoolReadsToGraduate && Target < MaxSize) {
                    Target = Min(Target * 2, MaxSize);
                    FullPoolReads = 0;
                }
            } else {
                FullPoolReads = 0;
            }
        }
    };

} // namespace NActors
