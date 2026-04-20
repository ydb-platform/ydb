#include "ddisk_actor_pdisk_ut.h"

Y_UNIT_TEST_SUITE(TDDiskActorSyncWithPersistentBuffer) {
    Y_UNIT_TEST(EmptySegments) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithPersistentBuffer>(
            creds,
            std::make_tuple(ui32(1), ui32(999), ui32(1)),
            std::optional<ui64>(42));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithPersistentBufferResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithPersistentBufferResult>(result, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(MissingDDiskId) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithPersistentBuffer>(
            creds,
            std::nullopt,
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, 0, MinBlockSize), 1, 1);

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithPersistentBufferResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithPersistentBufferResult>(result, TReplyStatus::INCORRECT_REQUEST);
    }
}

Y_UNIT_TEST_SUITE(TDDiskActorSyncWithDDisk) {
    Y_UNIT_TEST(EmptySegments) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(ui32(1), ui32(999), ui32(1)),
            std::optional<ui64>(42));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithDDiskResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(result, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(MissingDDiskId) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::nullopt,
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, 0, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithDDiskResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(result, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SegmentsFromDifferentVChunks) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(ui32(1), ui32(999), ui32(1)),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, 0, MinBlockSize));
        syncEv->AddSegment(NDDisk::TBlockSelector(1, 0, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithDDiskResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(result, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(UnalignedOffset) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(ui32(1), ui32(999), ui32(1)),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, 1, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithDDiskResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(result, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(UnalignedSize) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(ui32(1), ui32(999), ui32(1)),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, 0, MinBlockSize + 1));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithDDiskResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(result, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(ZeroSize) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(ui32(1), ui32(999), ui32(1)),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, 0, 0));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithDDiskResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(result, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SessionMismatch) {
        TTestContext ctx;

        NDDisk::TQueryCredentials creds;
        creds.TabletId = 30;
        creds.Generation = 1;

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(ui32(1), ui32(999), ui32(1)),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, 0, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithDDiskResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(result, TReplyStatus::SESSION_MISMATCH);
    }

    Y_UNIT_TEST(SyncRejectsOutOfBoundsSegment) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(ui32(1), ui32(999), ui32(1)),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, ChunkSize, ChunkSize / 2));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithDDiskResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(result, TReplyStatus::INCORRECT_REQUEST);
    }

    Y_UNIT_TEST(SyncReturnsErrorWhenSourceUnreachable) {
        TTestContext ctx;
        NDDisk::TQueryCredentials creds = Connect(ctx, 30, 1);

        auto syncEv = std::make_unique<NDDisk::TEvSyncWithDDisk>(
            creds,
            std::make_tuple(ui32(1), ui32(999), ui32(1)),
            std::optional<ui64>(42));
        syncEv->AddSegment(NDDisk::TBlockSelector(0, 0, MinBlockSize));

        auto result = ctx.SendAndGrab<NDDisk::TEvSyncWithDDiskResult>(syncEv.release());
        AssertStatus<NDDisk::TEvSyncWithDDiskResult>(result, TReplyStatus::ERROR);

        const auto& record = result->Get()->Record;
        UNIT_ASSERT_VALUES_EQUAL(record.SegmentResultsSize(), 1);
        UNIT_ASSERT_VALUES_EQUAL(
            static_cast<int>(record.GetSegmentResults(0).GetStatus()),
            static_cast<int>(TReplyStatus::ERROR));
        UNIT_ASSERT_C(record.GetSegmentResults(0).GetErrorReason().Contains("undelivered"),
            "Expected error reason to contain 'undelivered', got: " << record.GetSegmentResults(0).GetErrorReason());
    }

    Y_UNIT_TEST(Smoke_1Tablet_2VChunks_1Segment) {
        TestSyncWithDDisk(1, 2, 8, 1);
    }

    Y_UNIT_TEST(MultiSegment_1Tablet_4VChunks_4Segments) {
        TestSyncWithDDisk(1, 4, 64, 4);
    }

    Y_UNIT_TEST(MultiTablet_4Tablets_8VChunks_1Segment) {
        TestSyncWithDDisk(4, 8, 64, 1);
    }

    Y_UNIT_TEST(MultiTablet_4Tablets_16VChunks_8Segments) {
        TestSyncWithDDisk(4, 16, 64, 8);
    }
}

} // NKikimr
