#pragma once

#include <util/generic/algorithm.h>
#include <util/system/types.h>

namespace NActors {

    // Adaptive I/O buffer size bounded by [MinSize, MaxSize]. All three are kept at a multiple of
    // 64 bytes: both the read path and the serializer give an unused tail back by trimming a whole
    // number of cache lines off its front, which keeps the data pointer's cache-line offset (and
    // thus the serializer's single-line copy test) stable only when the slab size is aligned too.
    class TAdaptiveIoSize {
        static constexpr size_t Align = 64;

    protected:
        size_t Target = 0;
        size_t MinSize = 0;
        size_t MaxSize = 0;

        static constexpr size_t AlignDown(size_t n) { return n & ~(Align - 1); }
        static constexpr size_t AlignUp(size_t n) { return AlignDown(n + Align - 1); }

        void Grow() {
            Target = Min(Target * 2, MaxSize);
        }

        // Returns true when the size actually went down, so the caller can release a slab that no
        // longer matches the target. Halving an odd number of cache lines has to be rounded, as the
        // target may have been cut to an arbitrary cap.
        bool Shrink() {
            if (AtMinimum()) {
                return false;
            }
            Target = Max(AlignDown(Target / 2), MinSize);
            return true;
        }

    public:
        TAdaptiveIoSize() = default;

        TAdaptiveIoSize(size_t minSize, size_t maxSize)
            : Target(AlignUp(Max(minSize, Align)))
            , MinSize(Target)
            , MaxSize(Max(MinSize, AlignDown(maxSize)))
        {}

        size_t GetSize() const { return Target; }
        size_t GetMinSize() const { return MinSize; }
        bool AtMinimum() const { return Target == MinSize; }
    };

    // Scratch allocation for one produce stream. Only bytes the serializer has to copy land here;
    // aliased payloads do not use it. The target starts at min and grows when a produce fills it.
    // It shrinks when a produce used less than half of it, but only when the serialize window would
    // have allowed a whole target to be copied -- a window-limited produce says nothing about how
    // much scratch the stream needs.
    class TScratchTarget : public TAdaptiveIoSize {
    public:
        using TAdaptiveIoSize::TAdaptiveIoSize;

        // Copying more than the socket may hold in flight is pointless, so the window caps us.
        void SetMaxSize(size_t maxSize) {
            MaxSize = Max(MinSize, AlignDown(maxSize));
            Target = Min(Target, MaxSize);
        }

        size_t AllocSize(size_t remainingBudget) const {
            return Min(Target, AlignUp(Max(MinSize, remainingBudget)));
        }

        void OnProduce(size_t bytesCopied, bool budgetAllowedFullTarget) {
            if (bytesCopied >= Target) {
                Grow();
            } else if (budgetAllowedFullTarget && bytesCopied * 2 < Target) {
                Shrink();
            }
        }
    };

    // Main-socket read size. Grows when a full-target read comes back at least 3/4 full. Shrinks
    // only after several consecutive completions that filled less than half of what was offered, so
    // a single short TCP read cannot collapse the buffer; filling a leftover smaller than the target
    // does not grow it. Sessions sitting at the minimum read into the shared provided-buffer pool,
    // and several large pool reads in a row graduate one to a private buffer.
    class TReadTarget : public TAdaptiveIoSize {
        ui32 SmallCompletions = 0;
        ui32 FullPoolReads = 0;

    public:
        static constexpr ui32 SmallCompletionsToShrink = 3;
        static constexpr ui32 PoolReadsToGraduate = 3;
        static constexpr size_t GrowNumerator = 3;
        static constexpr size_t GrowDenominator = 4;

        using TAdaptiveIoSize::TAdaptiveIoSize;

        // Returns true if the leftover private buffer should be dropped: it no longer matches the
        // target, and at the minimum keeping it would hold the session off the shared pool. Slices
        // already handed to the deserializer keep the underlying slab alive on their own.
        bool OnCompletion(size_t num, size_t bufferSize) {
            FullPoolReads = 0;
            if (num * 2 >= bufferSize || AtMinimum()) {
                SmallCompletions = 0;
                if (bufferSize >= Target && IsLargeRead(num, bufferSize)) {
                    Grow();
                }
            } else if (++SmallCompletions >= SmallCompletionsToShrink) {
                // A short read is evidence of low demand even when we offered just a leftover tail.
                SmallCompletions = 0;
                return Shrink();
            }
            return false;
        }

        void OnPoolCompletion(size_t num, size_t poolBufSize) {
            if (!IsLargeRead(num, poolBufSize)) {
                FullPoolReads = 0;
            } else if (++FullPoolReads >= PoolReadsToGraduate) {
                FullPoolReads = 0;
                Grow();
            }
        }

    private:
        // A read that came back at least 3/4 full is taken as a hint that more was waiting.
        static bool IsLargeRead(size_t num, size_t size) {
            return num >= size * GrowNumerator / GrowDenominator;
        }
    };

} // namespace NActors
