#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NRepl {

    Y_UNIT_TEST_SUITE(TBlobStorageReplEta) {
        Y_UNIT_TEST(Progress) {
            const TInstant start = TInstant::Seconds(100);
            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(1000, 750, start,
                start + TDuration::Seconds(10)), TDuration::Seconds(30));
            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(30, 10, start,
                start + TDuration::Seconds(1)), TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(1000, 0, start,
                start + TDuration::Seconds(10)), TDuration::Zero());
        }

        Y_UNIT_TEST(NoProgress) {
            const TInstant start = TInstant::Seconds(100);
            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(1000, 1000, start,
                start + TDuration::Seconds(5)), TDuration::Zero());
            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(0, 0, start,
                start + TDuration::Seconds(5)), TDuration::Zero());
        }

        Y_UNIT_TEST(WorkAdded) {
            const TInstant start = TInstant::Seconds(100);
            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(1000, 2000, start,
                start + TDuration::Seconds(5)), TDuration::Zero());
        }

        Y_UNIT_TEST(InvalidTime) {
            const TInstant start = TInstant::Seconds(100);
            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(1000, 750, start, start), TDuration::Zero());
            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(1000, 750, start,
                start - TDuration::Seconds(1)), TDuration::Zero());
        }

        Y_UNIT_TEST(WhiteboardRange) {
            const TInstant start = TInstant::Seconds(100);
            const TDuration maxSeconds = TDuration::Seconds(Max<ui32>());
            const TDuration estimate = EstimateReplicationTimeOfArrival(2, 1, start,
                start + maxSeconds + TDuration::MilliSeconds(500));
            UNIT_ASSERT_VALUES_EQUAL(estimate.Seconds(), Max<ui32>());
            UNIT_ASSERT(estimate > maxSeconds);

            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(2, 1, start,
                start + maxSeconds + TDuration::Seconds(1)), TDuration::Zero());
            UNIT_ASSERT_VALUES_EQUAL(EstimateReplicationTimeOfArrival(Max<ui64>(), Max<ui64>() - 1,
                start, TInstant::Max()), TDuration::Zero());
        }
    }

} // namespace NKikimr::NRepl
