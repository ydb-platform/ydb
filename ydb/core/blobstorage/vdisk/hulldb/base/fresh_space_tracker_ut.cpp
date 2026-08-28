#include "fresh_space_tracker.h"

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TFreshSpaceTrackerTest) {
        Y_UNIT_TEST(RoundsEachDatabaseToSstBatches) {
            TFreshSpaceTracker tracker(true, 1024, 4, 0, 1);

            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateSegmentChunks(EFreshDb::Blocks, 0), 0);
            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateSegmentChunks(EFreshDb::Blocks, 1), 4);
            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateSegmentChunks(EFreshDb::Blocks, 8), 4);
            UNIT_ASSERT_VALUES_EQUAL(tracker.CalculateSegmentChunks(EFreshDb::Blocks, 9), 8);
        }

        Y_UNIT_TEST(AdmissionStopsAtGrantedDebt) {
            TFreshSpaceTracker tracker(true, 1024, 4, 0, 1);

            const auto initialRequest = tracker.BeginCreditRequest(0);
            UNIT_ASSERT(initialRequest);
            UNIT_ASSERT_VALUES_EQUAL(*initialRequest, 12);
            tracker.CompleteCreditRequest(*initialRequest);
            tracker.ConsumeCredits(4);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetGrantedChunks(), 8);

            const auto first = tracker.MakeAdmission(EFreshDb::Blocks, 8);
            const auto overflow = tracker.MakeAdmission(EFreshDb::Blocks);
            UNIT_ASSERT(tracker.TryAdmit(first, 4));
            UNIT_ASSERT(!tracker.TryAdmit(overflow, 4));

            tracker.CommitAdmission(first);
            UNIT_ASSERT(tracker.TryAdmit(overflow, 4));
            tracker.CancelAdmission(overflow);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetRequiredChunks(4), 4);
        }

        Y_UNIT_TEST(AccountsConsumptionBeforeGrantReply) {
            TFreshSpaceTracker tracker(true, 1024, 4, 0, 1);

            const auto request = tracker.BeginCreditRequest(0);
            UNIT_ASSERT(request);
            tracker.ConsumeCredits(4);
            tracker.CompleteCreditRequest(*request);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetGrantedChunks(), *request - 4);

            const auto refill = tracker.BeginCreditRequest(0);
            UNIT_ASSERT(refill);
            tracker.ConsumeCredits(tracker.GetGrantedChunks());
            tracker.CompleteCreditRequest(*refill);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetGrantedChunks(), 4);
        }

        Y_UNIT_TEST(RuntimeInlineLimitOnlyRaisesDebtEstimate) {
            TFreshSpaceTracker tracker(true, 4096, 2, 128, 4);
            const ui64 before = tracker.EstimateBytes(EFreshDb::LogoBlobs, 1);

            UNIT_ASSERT_VALUES_EQUAL(before, 2 * (128 * 4 + 1024 + 4 * 32));

            tracker.UpdateMaxInPlaceLogoBlobSize(64);
            UNIT_ASSERT_VALUES_EQUAL(tracker.EstimateBytes(EFreshDb::LogoBlobs, 1), before);

            tracker.UpdateMaxInPlaceLogoBlobSize(8192);
            UNIT_ASSERT(tracker.EstimateBytes(EFreshDb::LogoBlobs, 1) > before);
        }

        Y_UNIT_TEST(ReleasesSurplusButKeepsRefillCushion) {
            TFreshSpaceTracker tracker(true, 1024, 4, 0, 1);

            const auto initialRequest = tracker.BeginCreditRequest(0);
            UNIT_ASSERT(initialRequest);
            tracker.CompleteCreditRequest(*initialRequest);

            const auto admission = tracker.MakeAdmission(EFreshDb::Blocks, 8);
            UNIT_ASSERT(tracker.TryAdmit(admission, 0));
            const auto debtRequest = tracker.BeginCreditRequest(0);
            UNIT_ASSERT(debtRequest);
            tracker.CompleteCreditRequest(*debtRequest);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetGrantedChunks(), 16);

            tracker.CommitAdmission(admission);
            const auto release = tracker.BeginCreditRelease(0);
            UNIT_ASSERT(release);
            UNIT_ASSERT_VALUES_EQUAL(*release, 4);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetGrantedChunks(), 12);
            UNIT_ASSERT(!tracker.BeginCreditRequest(0));
            tracker.CompleteCreditRelease(*release);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetGrantedChunks(), 12);
        }

        Y_UNIT_TEST(DisabledTrackerDoesNotGateWrites) {
            TFreshSpaceTracker tracker(false, 1024, 4, 0, 1);
            const auto admission = tracker.MakeAdmission(EFreshDb::Blocks, 1000);

            UNIT_ASSERT(tracker.TryAdmit(admission, Max<ui64>()));
            UNIT_ASSERT(!tracker.BeginCreditRequest(Max<ui64>()));
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetRequiredChunks(Max<ui64>()), 0);
        }

        Y_UNIT_TEST(MetadataAdmissionIsSmallerThanInPlaceLogoBlob) {
            TFreshSpaceTracker tracker(true, 4096, 2, 128, 4);
            UNIT_ASSERT(tracker.EstimateMetadataBytes(1) < tracker.EstimateBytes(EFreshDb::LogoBlobs, 1));
            UNIT_ASSERT_VALUES_EQUAL(tracker.EstimateMetadataBytes(3), 3 * 512);
        }

        Y_UNIT_TEST(ZeroGrantSuppressesImmediateRerequest) {
            TFreshSpaceTracker tracker(true, 1024, 4, 0, 1);

            const auto request = tracker.BeginCreditRequest(0);
            UNIT_ASSERT(request);
            UNIT_ASSERT(tracker.HasPendingCreditOperation());
            tracker.CompleteCreditRequest(0);
            UNIT_ASSERT(!tracker.HasPendingCreditOperation());
            UNIT_ASSERT(!tracker.BeginCreditRequest(0));

            tracker.AllowCreditRequest();
            const auto retry = tracker.BeginCreditRequest(0);
            UNIT_ASSERT(retry);
            tracker.CompleteCreditRequest(*retry);
            UNIT_ASSERT_VALUES_EQUAL(tracker.GetGrantedChunks(), *retry);
        }
    }

} // NKikimr
