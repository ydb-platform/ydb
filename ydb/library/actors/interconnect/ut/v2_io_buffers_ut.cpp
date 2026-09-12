#include <ydb/library/actors/interconnect/v2_io_buffers.h>

#include <library/cpp/testing/unittest/registar.h>

using namespace NActors;

namespace {
    constexpr size_t MinSize = 4096;
    constexpr size_t MaxSize = 64 * 1024;
}

Y_UNIT_TEST_SUITE(InterconnectV2IoBuffers) {

    Y_UNIT_TEST(ScratchAllocFollowsBudgetAndTarget) {
        TScratchTarget scratch(MinSize, MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MinSize / 2), MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MaxSize), MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(5000) % 64, 0);

        scratch.OnProduce(MinSize, /*offeredFullTarget=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 2 * MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MaxSize), 2 * MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MinSize), MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(5000), 5056);
        for (size_t remaining = 1; remaining <= 2 * MinSize + 64; ++remaining) {
            UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(remaining) % 64, 0);
        }
    }

    Y_UNIT_TEST(ScratchAllocSizeStaysAlignedWhenTargetIsNot) {
        TScratchTarget scratch(MinSize, MaxSize);
        scratch.OnProduce(MinSize, /*offeredFullTarget=*/ true);
        scratch.SetMaxSize(5000);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 5000);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MaxSize), 4992);
        UNIT_ASSERT_VALUES_EQUAL(scratch.AllocSize(MaxSize) % 64, 0);
    }

    Y_UNIT_TEST(ScratchBudgetLimitedProduceDoesNotShrink) {
        TScratchTarget scratch(MinSize, MaxSize);
        for (int i = 0; i < 6; ++i) {
            scratch.OnProduce(scratch.GetSize(), /*offeredFullTarget=*/ true);
        }
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), MaxSize);

        scratch.OnProduce(MinSize, /*offeredFullTarget=*/ false);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), MaxSize);
    }

    Y_UNIT_TEST(ScratchFullOfferUnderuseShrinks) {
        TScratchTarget scratch(MinSize, MaxSize);
        for (int i = 0; i < 6; ++i) {
            scratch.OnProduce(scratch.GetSize(), true);
        }
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), MaxSize);

        scratch.OnProduce(MinSize, /*offeredFullTarget=*/ true);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), MaxSize / 2);
    }

    Y_UNIT_TEST(ScratchMaxFollowsSerializeCap) {
        TScratchTarget scratch(MinSize, MaxSize);
        for (int i = 0; i < 6; ++i) {
            scratch.OnProduce(scratch.GetSize(), true);
        }
        scratch.SetMaxSize(8 * MinSize);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 8 * MinSize);
        scratch.OnProduce(8 * MinSize, true);
        UNIT_ASSERT_VALUES_EQUAL(scratch.GetSize(), 8 * MinSize);
    }

    Y_UNIT_TEST(ReadAdvertiseIsCappedByTarget) {
        TReadTarget read(MinSize, MaxSize);
        UNIT_ASSERT_VALUES_EQUAL(read.Advertise(MaxSize), MinSize);
        UNIT_ASSERT_VALUES_EQUAL(read.Advertise(100), 100);
    }

    Y_UNIT_TEST(ReadDoesNotGrowOnLeftoverFill) {
        TReadTarget read(MinSize, MaxSize);
        UNIT_ASSERT(!read.OnCompletion(100, 100));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), MinSize);
    }

    Y_UNIT_TEST(ReadGrowsOnThreeQuarterFillOfTarget) {
        TReadTarget read(MinSize, MaxSize);
        const size_t filled = (MinSize * TReadTarget::GrowNumerator) / TReadTarget::GrowDenominator;
        UNIT_ASSERT(!read.OnCompletion(filled, MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
    }

    Y_UNIT_TEST(ReadNeedsConsecutiveSmallCompletionsToShrink) {
        TReadTarget read(MinSize, MaxSize);
        UNIT_ASSERT(!read.OnCompletion(MinSize, MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);

        UNIT_ASSERT(!read.OnCompletion(100, 2 * MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
        UNIT_ASSERT(!read.OnCompletion(100, 2 * MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
        UNIT_ASSERT(read.OnCompletion(100, 2 * MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), MinSize);
    }

    Y_UNIT_TEST(ReadMediumCompletionResetsShrinkStreak) {
        TReadTarget read(MinSize, MaxSize);
        UNIT_ASSERT(!read.OnCompletion(MinSize, MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);

        UNIT_ASSERT(!read.OnCompletion(100, 2 * MinSize));
        UNIT_ASSERT(!read.OnCompletion(MinSize + 1, 2 * MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
        UNIT_ASSERT(!read.OnCompletion(100, 2 * MinSize));
        UNIT_ASSERT(!read.OnCompletion(100, 2 * MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
    }

    Y_UNIT_TEST(ReadPoolNeedsSeveralHitsToGraduate) {
        TReadTarget read(MinSize, MaxSize);
        for (ui32 i = 0; i < TReadTarget::PoolReadsToGraduate - 1; ++i) {
            read.OnPoolCompletion(MinSize, MinSize);
            UNIT_ASSERT(read.AtMinimum());
        }
        read.OnPoolCompletion(MinSize, MinSize);
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
        UNIT_ASSERT(!read.AtMinimum());
    }

    Y_UNIT_TEST(ReadShrinkToMinDropsPrivateBuffer) {
        TReadTarget read(MinSize, MaxSize);
        UNIT_ASSERT(!read.OnCompletion(MinSize, MinSize));
        UNIT_ASSERT_VALUES_EQUAL(read.GetSize(), 2 * MinSize);
        bool drop = false;
        for (ui32 i = 0; i < TReadTarget::SmallCompletionsToShrink; ++i) {
            drop = read.OnCompletion(100, 2 * MinSize);
        }
        UNIT_ASSERT(drop);
        UNIT_ASSERT(read.AtMinimum());
    }
}
