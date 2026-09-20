#include "fresh_space_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TFreshSpaceTrackerTest) {
        // Chunk capacity is discounted by the largest item that packing can waste at
        // a boundary; here that clamps to half of the 1024-byte chunk.
        Y_UNIT_TEST(RoundsToSstBatchesOverUsableCapacity) {
            TFreshSpaceTracker tracker(true, 1024, 4, 0, 1);

            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateChunks(0), 0);
            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateChunks(1), 4);
            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateChunks(512 * 4), 4);
            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateChunks(512 * 4 + 1), 8);
        }

        Y_UNIT_TEST(ChargesIndexEntryPlusPayload) {
            UNIT_ASSERT_VALUES_EQUAL(TFreshSpaceTracker::RecordBytes(64, 0), 64);
            UNIT_ASSERT_VALUES_EQUAL(TFreshSpaceTracker::RecordBytes(64, 1), 64 + 8);
            UNIT_ASSERT_VALUES_EQUAL(TFreshSpaceTracker::RecordBytes(64, 4096), 64 + 4096);
        }

        // Writes admitted but not yet in Fresh must be visible to the projection, or a
        // burst of concurrent writes would all be judged against the same segment size.
        Y_UNIT_TEST(ProjectionCountsSegmentAndInFlight) {
            TFreshSpaceTracker tracker(true, 1024, 1, 0, 1);

            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(0, {}), 0);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(512, {}), 1);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(1, {.CurBytes = 512}), 2);

            tracker.Admit(512);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetInFlightBytes(), 512);
            // The in-flight write fills the first chunk, so the candidate starts a second.
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(1, {}), 2);

            tracker.CommitAdmission(512);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetInFlightBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(1, {}), 1);
        }

        // Each segment is compacted on its own and writes its own sst, so two segments
        // that would comfortably share a chunk by byte count still need one each.
        Y_UNIT_TEST(ProjectionRoundsEachSegmentSeparately) {
            TFreshSpaceTracker tracker(true, 1024, 1, 0, 1);

            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(0, {.OldBytes = 1}), 1);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(0, {.CurBytes = 1}), 1);
            // Summing first and rounding once would report a single chunk here.
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(0, {.OldBytes = 1, .CurBytes = 1}), 2);
            UNIT_ASSERT_VALUES_EQUAL(
                tracker.GetProjectedChunks(0, {.OldBytes = 1, .DregBytes = 1, .CurBytes = 1}), 3);

            // The candidate and anything in flight belong to Cur, not to a segment that
            // is already closed for writing.
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(512, {.OldBytes = 1}), 2);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(513, {.OldBytes = 1}), 3);
        }

        // A segment being compacted right now keeps owing its bytes: the sst is not
        // written yet, and several fresh segments can be outstanding at once.
        Y_UNIT_TEST(ProjectionCountsSstBatchesPerSegment) {
            TFreshSpaceTracker tracker(true, 1024, 4, 0, 1);

            // One byte in a segment still reserves a whole four-chunk sst batch.
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(0, {.OldBytes = 1}), 4);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(0, {.OldBytes = 1, .CurBytes = 1}), 8);
        }

        // Raising the in-place limit widens the boundary waste a chunk can suffer,
        // which shrinks usable capacity; lowering it must not relax the estimate.
        Y_UNIT_TEST(RuntimeInlineLimitOnlyRaisesDebtEstimate) {
            TFreshSpaceTracker tracker(true, 4096, 1, 128, 4);
            const ui64 before = tracker.CalculateChunks(11000);

            UNIT_ASSERT_VALUES_EQUAL(before, 5);

            tracker.UpdateMaxInPlaceLogoBlobSize(64);
            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateChunks(11000), before);

            tracker.UpdateMaxInPlaceLogoBlobSize(8192);
            UNIT_ASSERT(tracker.CalculateChunks(11000) > before);
        }

        Y_UNIT_TEST(DisabledTrackerProjectsNothing) {
            TFreshSpaceTracker tracker(false, 1024, 4, 0, 1);

            tracker.Admit(1000);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetInFlightBytes(), 0);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetProjectedChunks(1000, {.CurBytes = Max<ui64>() / 2}), 0);
        }
    }

} // NKikimr
