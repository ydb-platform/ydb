#include <ydb/core/blobstorage/vdisk/synclog/phantom_flag_storage/phantom_flag_storage_merge.h>

#include <library/cpp/testing/unittest/registar.h>

#include <array>

namespace NKikimr::NSyncLog {

Y_UNIT_TEST_SUITE(TPhantomFlagRunMergeTest) {
    Y_UNIT_TEST(SortUsesTheFullBlobId) {
        const TLogoBlobID first(1, 1, 1, 0, 100, 0);
        const TLogoBlobID second(1, 1, 1, 0, 101, 0);
        const TLogoBlobID third(1, 1, 1, 0, 100, 1);
        std::array<TLogoBlobID, 6> keys{third, first, second, second, third, first};

        const std::span<TLogoBlobID> sorted = SortPhantomFlagKeys(keys);

        UNIT_ASSERT_VALUES_EQUAL(sorted.size(), 3U);
        UNIT_ASSERT_VALUES_EQUAL(sorted[0], first);
        UNIT_ASSERT_VALUES_EQUAL(sorted[1], second);
        UNIT_ASSERT_VALUES_EQUAL(sorted[2], third);
        UNIT_ASSERT(SortPhantomFlagKeys({}).empty());
    }

    Y_UNIT_TEST(WaitForNextBlockBeforeEmittingEqualKey) {
        const TLogoBlobID first(1, 1, 1, 0, 100, 0);
        const TLogoBlobID second(1, 1, 2, 0, 100, 0);
        const TLogoBlobID third(1, 1, 3, 0, 100, 0);
        const std::array<TLogoBlobID, 1> leftFirst{first};
        const std::array<TLogoBlobID, 1> leftLast{second};
        const std::array<TLogoBlobID, 2> right{second, third};
        std::array<TLogoBlobID, 2> output;

        const TPhantomFlagRunMergeProgress firstStep =
            MergeSortedPhantomFlagRuns(leftFirst, false, right, true, output);
        UNIT_ASSERT_VALUES_EQUAL(firstStep.LeftConsumed, 1U);
        UNIT_ASSERT_VALUES_EQUAL(firstStep.RightConsumed, 0U);
        UNIT_ASSERT_VALUES_EQUAL(firstStep.Written, 1U);
        UNIT_ASSERT_VALUES_EQUAL(output[0], first);

        const TPhantomFlagRunMergeProgress lastStep =
            MergeSortedPhantomFlagRuns(leftLast, true, right, true, output);
        UNIT_ASSERT_VALUES_EQUAL(lastStep.LeftConsumed, 1U);
        UNIT_ASSERT_VALUES_EQUAL(lastStep.RightConsumed, 2U);
        UNIT_ASSERT_VALUES_EQUAL(lastStep.Written, 2U);
        UNIT_ASSERT_VALUES_EQUAL(output[0], second);
        UNIT_ASSERT_VALUES_EQUAL(output[1], third);
    }

    Y_UNIT_TEST(OutputBoundaryConsumesBothEqualInputs) {
        const TLogoBlobID first(1, 1, 1, 0, 100, 0);
        const TLogoBlobID second(1, 1, 2, 0, 100, 0);
        const std::array<TLogoBlobID, 2> keys{first, second};
        const std::span<const TLogoBlobID> input(keys);
        std::array<TLogoBlobID, 1> output;

        const TPhantomFlagRunMergeProgress firstStep =
            MergeSortedPhantomFlagRuns(input, true, input, true, output);
        UNIT_ASSERT_VALUES_EQUAL(firstStep.LeftConsumed, 1U);
        UNIT_ASSERT_VALUES_EQUAL(firstStep.RightConsumed, 1U);
        UNIT_ASSERT_VALUES_EQUAL(firstStep.Written, 1U);
        UNIT_ASSERT_VALUES_EQUAL(output[0], first);

        const TPhantomFlagRunMergeProgress lastStep = MergeSortedPhantomFlagRuns(
            input.subspan(firstStep.LeftConsumed), true, input.subspan(firstStep.RightConsumed), true, output);
        UNIT_ASSERT_VALUES_EQUAL(lastStep.Written, 1U);
        UNIT_ASSERT_VALUES_EQUAL(output[0], second);
    }

    Y_UNIT_TEST(LeftDifferenceOmitsKeysAlreadyPresent) {
        const TLogoBlobID first(1, 1, 1, 0, 100, 0);
        const TLogoBlobID second(1, 1, 2, 0, 100, 0);
        const TLogoBlobID third(1, 1, 3, 0, 100, 0);
        const TLogoBlobID fourth(1, 1, 4, 0, 100, 0);
        const std::array<TLogoBlobID, 3> left{first, second, third};
        const std::array<TLogoBlobID, 2> right{second, fourth};
        std::array<TLogoBlobID, 3> output;

        const TPhantomFlagRunMergeProgress progress =
            MergeSortedPhantomFlagRuns(left, true, right, true, output, EPhantomFlagRunMerge::LeftDifference);

        UNIT_ASSERT_VALUES_EQUAL(progress.LeftConsumed, 3U);
        UNIT_ASSERT_VALUES_EQUAL(progress.RightConsumed, 1U);
        UNIT_ASSERT_VALUES_EQUAL(progress.Written, 2U);
        UNIT_ASSERT_VALUES_EQUAL(output[0], first);
        UNIT_ASSERT_VALUES_EQUAL(output[1], third);
    }
}

} // namespace NKikimr::NSyncLog
