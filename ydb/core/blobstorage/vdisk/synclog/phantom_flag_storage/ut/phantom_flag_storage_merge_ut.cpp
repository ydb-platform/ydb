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

    Y_UNIT_TEST(RefillCanPrecedeAnotherInputHead) {
        const TLogoBlobID first(1, 1, 1, 0, 100, 0);
        const TLogoBlobID second(1, 1, 2, 0, 100, 0);
        const TLogoBlobID third(1, 1, 3, 0, 100, 0);
        const TLogoBlobID fourth(1, 1, 4, 0, 100, 0);
        const TLogoBlobID fifth(1, 1, 5, 0, 100, 0);
        TPhantomFlagRunMerger merger(3);
        std::array<TLogoBlobID, 5> output;

        UNIT_ASSERT_VALUES_EQUAL(merger.Merge(output).InputToRefill, std::optional<size_t>{0});
        merger.SetInputBlock(0, {first}, false);
        UNIT_ASSERT_VALUES_EQUAL(merger.Merge(output).InputToRefill, std::optional<size_t>{1});
        merger.SetInputBlock(1, {fourth, fifth}, true);
        UNIT_ASSERT_VALUES_EQUAL(merger.Merge(output).InputToRefill, std::optional<size_t>{2});
        merger.SetInputBlock(2, {second, fourth}, true);

        const TPhantomFlagRunMerger::TProgress blocked = merger.Merge(output);
        UNIT_ASSERT_VALUES_EQUAL(blocked.Written, 1U);
        UNIT_ASSERT_VALUES_EQUAL(blocked.InputToRefill, std::optional<size_t>{0});
        UNIT_ASSERT(!blocked.Finished);
        UNIT_ASSERT_VALUES_EQUAL(output[0], first);

        merger.SetInputBlock(0, {third, fourth}, true);
        const TPhantomFlagRunMerger::TProgress lastStep = merger.Merge(output);
        UNIT_ASSERT_VALUES_EQUAL(lastStep.Written, 4U);
        UNIT_ASSERT(!lastStep.InputToRefill);
        UNIT_ASSERT(lastStep.Finished);
        UNIT_ASSERT_VALUES_EQUAL(output[0], second);
        UNIT_ASSERT_VALUES_EQUAL(output[1], third);
        UNIT_ASSERT_VALUES_EQUAL(output[2], fourth);
        UNIT_ASSERT_VALUES_EQUAL(output[3], fifth);
    }

    Y_UNIT_TEST(DeduplicatesAcrossInputAndOutputBoundaries) {
        const TLogoBlobID first(1, 1, 1, 0, 100, 0);
        const TLogoBlobID second(1, 1, 2, 0, 100, 0);
        TPhantomFlagRunMerger merger(3);
        std::array<TLogoBlobID, 1> output;

        merger.SetInputBlock(0, {first}, false);
        merger.SetInputBlock(1, {first, second}, true);
        merger.SetInputBlock(2, {first, second}, true);

        const TPhantomFlagRunMerger::TProgress firstStep = merger.Merge(output);
        UNIT_ASSERT_VALUES_EQUAL(firstStep.Written, 1U);
        UNIT_ASSERT_VALUES_EQUAL(output[0], first);
        UNIT_ASSERT(!firstStep.Finished);

        const TPhantomFlagRunMerger::TProgress needPage = merger.Merge(output);
        UNIT_ASSERT_VALUES_EQUAL(needPage.Written, 0U);
        UNIT_ASSERT_VALUES_EQUAL(needPage.InputToRefill, std::optional<size_t>{0});
        merger.SetInputBlock(0, {first, second}, true);

        const TPhantomFlagRunMerger::TProgress lastKey = merger.Merge(output);
        UNIT_ASSERT_VALUES_EQUAL(lastKey.Written, 1U);
        UNIT_ASSERT_VALUES_EQUAL(output[0], second);

        const TPhantomFlagRunMerger::TProgress end = merger.Merge(output);
        UNIT_ASSERT_VALUES_EQUAL(end.Written, 0U);
        UNIT_ASSERT(!end.InputToRefill);
        UNIT_ASSERT(end.Finished);
    }

    Y_UNIT_TEST(EmptyFinalPagesEndTheirInputs) {
        const TLogoBlobID key(1, 1, 1, 0, 100, 0);
        TPhantomFlagRunMerger merger(2);
        std::array<TLogoBlobID, 2> output;

        merger.SetInputBlock(0, {}, true);
        merger.SetInputBlock(1, {key}, false);

        const TPhantomFlagRunMerger::TProgress firstStep = merger.Merge(output);
        UNIT_ASSERT_VALUES_EQUAL(firstStep.Written, 1U);
        UNIT_ASSERT_VALUES_EQUAL(output[0], key);
        UNIT_ASSERT_VALUES_EQUAL(firstStep.InputToRefill, std::optional<size_t>{1});
        UNIT_ASSERT(!firstStep.Finished);
        merger.SetInputBlock(1, {}, true);

        const TPhantomFlagRunMerger::TProgress end = merger.Merge(output);
        UNIT_ASSERT_VALUES_EQUAL(end.Written, 0U);
        UNIT_ASSERT(!end.InputToRefill);
        UNIT_ASSERT(end.Finished);
    }
}

} // namespace NKikimr::NSyncLog
