#include <ydb/core/blobstorage/vdisk/repl/blobstorage_repl.h>
#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo_sets.h>

#include <library/cpp/testing/unittest/registar.h>

#include <bit>

namespace NKikimr::NRepl {

    namespace {
        TBlobStorageGroupInfo::TGroupVDisks MakeFailedDisks(const TBlobStorageGroupInfo& info,
                std::initializer_list<ui32> orderNumbers) {
            auto failed = TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology());
            for (const ui32 orderNumber : orderNumbers) {
                failed += TBlobStorageGroupInfo::TGroupVDisks(&info.GetTopology(), info.GetVDiskId(orderNumber));
            }
            return failed;
        }

        ui32 CalculateReplTokenPriorityByEnumeration(
                const TBlobStorageGroupInfo::TTopology& topology,
                const TBlobStorageGroupInfo::TGroupVDisks& failedDisks) {
            const auto& checker = topology.GetQuorumChecker();
            if (!checker.CheckFailModelForGroup(failedDisks)) {
                return 0;
            }

            const ui32 numDisks = topology.GetTotalVDisksNum();
            ui64 failedMask = 0;
            for (ui32 i = 0; i < numDisks; ++i) {
                failedMask |= static_cast<ui64>(failedDisks[i]) << i;
            }
            const ui64 healthyMask = ((ui64{1} << numDisks) - 1) & ~failedMask;
            ui32 tokenPriority = Max<ui32>();
            for (ui64 additional = healthyMask; additional; additional = (additional - 1) & healthyMask) {
                const auto failures = TBlobStorageGroupInfo::TGroupVDisks::CreateFromMask(
                    &topology, failedMask | additional);
                if (!checker.CheckFailModelForGroup(failures)) {
                    tokenPriority = Min(tokenPriority, static_cast<ui32>(std::popcount(additional)));
                }
            }
            return tokenPriority;
        }

        void CheckReplTokenPriorityForEveryFailureSet(const TBlobStorageGroupInfo& info) {
            const auto& topology = info.GetTopology();
            const auto& checker = topology.GetQuorumChecker();
            const ui64 endMask = ui64{1} << topology.GetTotalVDisksNum();
            for (ui64 mask = 0; mask < endMask; ++mask) {
                const auto failedDisks = TBlobStorageGroupInfo::TGroupVDisks::CreateFromMask(&topology, mask);
                UNIT_ASSERT_VALUES_EQUAL_C(checker.GetReplTokenPriority(failedDisks),
                    CalculateReplTokenPriorityByEnumeration(topology, failedDisks), "mask# " << mask);
            }
        }
    }

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

        Y_UNIT_TEST(ReplTokenPriorityForBlock42) {
            const TBlobStorageGroupInfo info(TBlobStorageGroupType::Erasure4Plus2Block);
            const auto& topology = info.GetTopology();
            const auto& checker = topology.GetQuorumChecker();

            UNIT_ASSERT_VALUES_EQUAL(checker.GetReplTokenPriority(MakeFailedDisks(info, {})), 3);
            UNIT_ASSERT_VALUES_EQUAL(checker.GetReplTokenPriority(MakeFailedDisks(info, {0})), 2);
            UNIT_ASSERT_VALUES_EQUAL(checker.GetReplTokenPriority(MakeFailedDisks(info, {0, 1})), 1);
            UNIT_ASSERT_VALUES_EQUAL(checker.GetReplTokenPriority(MakeFailedDisks(info, {0, 1, 2})), 0);
        }

        Y_UNIT_TEST(ReplTokenPriorityAccountsForFailureDomains) {
            const TBlobStorageGroupInfo info(TBlobStorageGroupType::ErasureMirror3dc, 1, 3, 3);
            const auto& topology = info.GetTopology();
            const auto& checker = topology.GetQuorumChecker();

            UNIT_ASSERT_VALUES_EQUAL(checker.GetReplTokenPriority(MakeFailedDisks(info, {})), 3);
            UNIT_ASSERT_VALUES_EQUAL(checker.GetReplTokenPriority(MakeFailedDisks(info, {0})), 2);
            UNIT_ASSERT_VALUES_EQUAL(checker.GetReplTokenPriority(MakeFailedDisks(info, {0, 3})), 1);
            UNIT_ASSERT_VALUES_EQUAL(checker.GetReplTokenPriority(MakeFailedDisks(info, {0, 1})), 2);
        }

        Y_UNIT_TEST(ReplTokenPriorityMatchesFailureModelForEveryFailureSet) {
            CheckReplTokenPriorityForEveryFailureSet(
                TBlobStorageGroupInfo(TBlobStorageGroupType::Erasure4Plus2Block));
            CheckReplTokenPriorityForEveryFailureSet(
                TBlobStorageGroupInfo(TBlobStorageGroupType::ErasureMirror3of4));
            CheckReplTokenPriorityForEveryFailureSet(
                TBlobStorageGroupInfo(TBlobStorageGroupType::ErasureMirror3dc, 1, 3, 3));
        }
    }

} // namespace NKikimr::NRepl
