#include "ddisk_actor_pdisk_common_ut.h"

namespace NKikimr {

namespace {

std::unique_ptr<NDDisk::TEvSync> MakeSync(
    const NDDisk::TQueryCredentials& creds)
{
    return std::make_unique<NDDisk::TEvSync>(
        creds,
        std::make_tuple(ui32(1), ui32(999), ui32(1)),
        42);
}

}   // namespace

Y_UNIT_TEST_SUITE(TDDiskActorSync) {
    Y_UNIT_TEST(EmptySources) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSync>();
        creds.Serialize(syncEv->Record.MutableCredentials());

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SourceWithoutSegments) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = MakeSync(creds);

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SourceWithoutDDiskId) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSync>();
        creds.Serialize(syncEv->Record.MutableCredentials());
        syncEv->Record.AddSources();
        syncEv->AddSegmentFromDDisk(
            0,
            NDDisk::TBlockSelector(0, 0, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SecondSourceWithoutDDiskId) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = MakeSync(creds);
        syncEv->AddSegmentFromDDisk(0, NDDisk::TBlockSelector(0, 0, MinBlockSize));
        syncEv->Record.AddSources();
        syncEv->AddSegmentFromDDisk(
            1,
            NDDisk::TBlockSelector(0, MinBlockSize, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SegmentWithoutKind) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = MakeSync(creds);
        auto* segment = syncEv->Record.MutableSources(0)->AddSegments();
        NDDisk::TBlockSelector(0, 0, MinBlockSize).Serialize(
            segment->MutableSelector());

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SegmentsFromDifferentVChunks) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = MakeSync(creds);
        syncEv->AddSegmentFromDDisk(0, NDDisk::TBlockSelector(0, 0, MinBlockSize));
        syncEv->AddSegmentFromDDisk(0, NDDisk::TBlockSelector(1, 0, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(UnalignedOffset) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = MakeSync(creds);
        syncEv->AddSegmentFromDDisk(0, NDDisk::TBlockSelector(0, 1, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(UnalignedSize) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = MakeSync(creds);
        syncEv->AddSegmentFromDDisk(0, NDDisk::TBlockSelector(0, 0, MinBlockSize + 1));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(ZeroSize) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = MakeSync(creds);
        syncEv->AddSegmentFromDDisk(0, NDDisk::TBlockSelector(0, 0, 0));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SessionMismatch) {
        TTestContext ctx;

        NDDisk::TQueryCredentials creds;
        creds.TabletId = 30;
        creds.Generation = 1;

        auto syncEv = MakeSync(creds);
        syncEv->AddSegmentFromDDisk(0, NDDisk::TBlockSelector(0, 0, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::SESSION_MISMATCH);
    }

    Y_UNIT_TEST(SyncRejectsOutOfBoundsSegment) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = MakeSync(creds);
        syncEv->AddSegmentFromDDisk(
            0,
            NDDisk::TBlockSelector(0, ChunkSize, ChunkSize / 2));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(
            result,
            TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SyncReturnsErrorWhenSourceUnreachable) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = MakeSync(creds);
        syncEv->AddSegmentFromDDisk(0, NDDisk::TBlockSelector(0, 0, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncResult>(result, TReplyStatus::ERROR);

        const auto& record = result->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::ERROR));
        UNIT_ASSERT_C(
            record.GetSegmentResults(0).GetErrorReason().Contains(
                "undelivered"),
            "Expected error reason to contain 'undelivered', got: "
                << record.GetSegmentResults(0).GetErrorReason());
    }

    Y_UNIT_TEST(Smoke_1Tablet_2VChunks_1Segment) {
        TestSync(1, 2, 8, 1);
    }

    Y_UNIT_TEST(MultiSegment_1Tablet_4VChunks_4Segments) {
        TestSync(1, 4, 64, 4);
    }

    Y_UNIT_TEST(MultiTablet_4Tablets_8VChunks_1Segment) {
        TestSync(4, 8, 64, 1);
    }

    Y_UNIT_TEST(MultiTablet_4Tablets_16VChunks_8Segments) {
        TestSync(4, 16, 64, 8);
    }
}

} // namespace NKikimr
