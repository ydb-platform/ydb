#include <ydb/library/actors/interconnect/v2_io_buffers.h>
#include <ydb/library/actors/util/rc_buf.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {
    constexpr size_t MinSize = 4096;
    constexpr size_t MaxSize = 64 * 1024;

    TScratchTarget GrownScratch() {
        TScratchTarget scratch(MinSize, MaxSize);
        while (scratch.GetSize() < MaxSize) {
            scratch.OnProduce(scratch.GetSize(), /*budgetAllowedFullTarget=*/ true);
        }
        return scratch;
    }

    TReadTarget GrownRead() {
        TReadTarget read(MinSize, MaxSize);
        while (read.GetSize() < MaxSize) {
            read.OnCompletion(read.GetSize(), read.GetSize());
        }
        return read;
    }
}

Y_UNIT_TEST_SUITE(InterconnectV2IoBuffers) {

    Y_UNIT_TEST(ScratchAllocFollowsBudgetAndTarget) {
        TScratchTarget scratch(MinSize, MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MinSize / 2), MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MaxSize), MinSize);

        scratch.OnProduce(MinSize, /*budgetAllowedFullTarget=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 2 * MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MaxSize), 2 * MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MinSize), MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(5000), 5056);
        for (size_t budget = 1; budget <= 2 * MinSize + 64; ++budget) {
            UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(budget) % 64, 0);
        }
    }

    // Every slab handed to the serializer must be a whole number of cache lines: it gives the unused
    // tail back by trimming 64-byte chunks off the front, which only keeps the data pointer's
    // alignment stable when the size is aligned as well.
    Y_UNIT_TEST(ScratchBoundsAreCacheLineAligned) {
        TScratchTarget scratch(4097, 5000);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 4160);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(1), 4160);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MaxSize), 4160);

        scratch.OnProduce(scratch.GetSize(), true);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 4992);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MaxSize), 4992);

        scratch.SetMaxSize(5000);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 4992);
    }

    // A target cut down to an arbitrary window cap can sit on an odd number of cache lines, and
    // halving that must not leave a size no allocation can be aligned to.
    Y_UNIT_TEST(ScratchStaysAlignedWhenHalvingAnOddCap) {
        TScratchTarget scratch(64, MaxSize);
        scratch.SetMaxSize(4160);
        while (scratch.GetSize() < 4160) {
            scratch.OnProduce(scratch.GetSize(), true);
        }
        scratch.OnProduce(0, /*budgetAllowedFullTarget=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 2048);
    }

    Y_UNIT_TEST(ScratchFullTargetUnderuseShrinks) {
        TScratchTarget scratch = GrownScratch();
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), MaxSize);

        scratch.OnProduce(MinSize, /*budgetAllowedFullTarget=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), MaxSize / 2);
    }

    // A produce the serialize window cut short never saw a chance to need the whole target.
    Y_UNIT_TEST(ScratchWindowLimitedProduceDoesNotShrink) {
        TScratchTarget scratch = GrownScratch();
        scratch.OnProduce(MinSize, /*budgetAllowedFullTarget=*/ false);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), MaxSize);
    }

    Y_UNIT_TEST(ScratchMaxFollowsSerializeCap) {
        TScratchTarget scratch = GrownScratch();
        scratch.SetMaxSize(8 * MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 8 * MinSize);
        scratch.OnProduce(8 * MinSize, true);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 8 * MinSize);
    }

    Y_UNIT_TEST(ReadGrowsOnlyOnFullTargetFill) {
        TReadTarget read(MinSize, MaxSize);
        // Filling a leftover tail says nothing about how big the next buffer should be.
        UNIT_ASSERT(!read.OnCompletion(100, 100));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), MinSize);

        const size_t filled = (MinSize * TReadTarget::GrowNumerator) / TReadTarget::GrowDenominator;
        UNIT_ASSERT(!read.OnCompletion(filled, MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
    }

    Y_UNIT_TEST(ReadNeedsConsecutiveSmallCompletionsToShrink) {
        TReadTarget read(MinSize, MaxSize);
        UNIT_ASSERT(!read.OnCompletion(MinSize, MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);

        bool drop = false;
        for (ui32 i = 0; i < TReadTarget::SmallCompletionsToShrink; ++i) {
            UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
            drop = read.OnCompletion(100, 2 * MinSize);
        }
        UNIT_ASSERT(drop); // back to the minimum: the leftover would keep us off the shared pool
        UNIT_ASSERT(read.AtMinimum());
    }

    Y_UNIT_TEST(ReadMediumCompletionResetsShrinkStreak) {
        TReadTarget read(MinSize, MaxSize);
        UNIT_ASSERT(!read.OnCompletion(MinSize, MinSize));

        UNIT_ASSERT(!read.OnCompletion(100, 2 * MinSize));
        UNIT_ASSERT(!read.OnCompletion(MinSize + 1, 2 * MinSize));
        UNIT_ASSERT(!read.OnCompletion(100, 2 * MinSize));
        UNIT_ASSERT(!read.OnCompletion(100, 2 * MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
    }

    // Short reads into a reused tail are still short reads; requiring a freshly allocated full-size
    // buffer here would keep a session that only ever reuses tails at its peak size forever.
    Y_UNIT_TEST(ReadShrinksWithReusedBufferTails) {
        TReadTarget read = GrownRead();

        TRcBuf buffer;
        for (size_t i = 0; i < 32 && !read.AtMinimum(); ++i) {
            if (buffer.size() < MinSize) {
                buffer = TRcBuf::Uninitialized(read.GetSize());
            }
            const size_t size = buffer.size();
            constexpr size_t num = 100;
            const size_t remain = size - num;
            buffer.TrimFront(remain - remain % 64);
            if (read.OnCompletion(num, size)) {
                buffer = {};
            }
        }
        UNIT_ASSERT(read.AtMinimum());
        UNIT_ASSERT(buffer.empty());
    }

    // With provided buffers disabled, dropping a minimum-sized slab would just force another
    // minimum-sized allocation for the next read.
    Y_UNIT_TEST(ReadAtMinimumKeepsReusableTail) {
        TReadTarget read(MinSize, MaxSize);
        UNIT_ASSERT(!read.OnCompletion(100, MinSize));
        UNIT_ASSERT(!read.OnCompletion(100, MinSize - 128));
        UNIT_ASSERT(!read.OnCompletion(100, MinSize - 256));
        UNIT_ASSERT(read.AtMinimum());
    }

    Y_UNIT_TEST(ReadPoolGraduationRequiresConsecutivePoolReads) {
        TReadTarget read(MinSize, MaxSize);
        for (ui32 i = 0; i < TReadTarget::PoolReadsToGraduate - 1; ++i) {
            read.OnPoolCompletion(MinSize, MinSize);
            UNIT_ASSERT(read.AtMinimum());
        }
        read.OnCompletion(100, MinSize); // a private fallback read interrupts the streak
        for (ui32 i = 0; i < TReadTarget::PoolReadsToGraduate - 1; ++i) {
            read.OnPoolCompletion(MinSize, MinSize);
            UNIT_ASSERT(read.AtMinimum());
        }
        read.OnPoolCompletion(MinSize, MinSize);
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
    }
}
