#include "fresh_output_estimate.h"
#include "fresh_segment.h"
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/blobstorage_blob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr {

    Y_UNIT_TEST_SUITE(TFreshOutputEstimateTest) {

        constexpr ui32 ChunkSize = 1 << 20;
        constexpr ui32 AppendBlockSize = 4 << 10;
        constexpr ui32 RecordBytes = sizeof(TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>);

        TFreshOutputGeometry Geometry(ui32 chunksPerSst = 1) {
            return {
                .ChunkSize = ChunkSize,
                .AppendBlockSize = AppendBlockSize,
                .ChunksPerSst = chunksPerSst,
                .TotalPartCount = 6,
                .IndexRecordBytes = RecordBytes,
            };
        }

        // What one chunk-sized SST takes before anything could be wasted at a boundary.
        constexpr ui64 SingleSstCapacity = ChunkSize - sizeof(TIdxDiskPlaceHolder) - AppendBlockSize;

        Y_UNIT_TEST(EmptySegmentNeedsNothing) {
            TFreshOutputEstimate estimate;
            UNIT_ASSERT(estimate.Empty());
            UNIT_ASSERT_VALUES_EQUAL(estimate.GetCharge(Geometry()), 0);
            UNIT_ASSERT_VALUES_EQUAL(estimate.GetChunks(Geometry()), 0);
        }

        // Each kind of record is charged what the writer lays out for it.
        Y_UNIT_TEST(ChargesFollowWriterLayout) {
            TFreshOutputEstimate estimate;
            estimate.AddInline(13); // aligned to 4 like TBaseWriter::AppendAligned()
            UNIT_ASSERT_VALUES_EQUAL(estimate.GetCharge(Geometry()), RecordBytes + 16);
            estimate.AddHuge(); // an index record plus, at most, one outbound TDiskPart
            UNIT_ASSERT_VALUES_EQUAL(estimate.GetCharge(Geometry()), 2 * RecordBytes + 16 + sizeof(TDiskPart));
            estimate.AddIndexOnly(3);
            UNIT_ASSERT_VALUES_EQUAL(estimate.GetCharge(Geometry()), 5 * RecordBytes + 16 + sizeof(TDiskPart));
            UNIT_ASSERT_VALUES_EQUAL(estimate.GetRecords(), 5);
            UNIT_ASSERT_VALUES_EQUAL(estimate.GetMaxInlineBytes(), 16);
        }

        // With nothing past the first chunk there is no boundary to waste anything at, so the first
        // chunk is available whole, however large the records in it are.
        Y_UNIT_TEST(FirstChunkIsAvailableWhole) {
            for (ui32 maxInline : {0u, 4u << 10, 64u << 10}) {
                UNIT_ASSERT_VALUES_EQUAL(TFreshOutputEstimate::CalculateChunks(1, maxInline, Geometry()), 1);
                UNIT_ASSERT_VALUES_EQUAL(
                    TFreshOutputEstimate::CalculateChunks(SingleSstCapacity, maxInline, Geometry()), 1);
                UNIT_ASSERT_VALUES_EQUAL(
                    TFreshOutputEstimate::CalculateChunks(SingleSstCapacity + 1, maxInline, Geometry()), 2);
            }
        }

        // Past the first chunk, each boundary may waste up to one merged record, so larger records
        // can only ever need more chunks for the same payload.
        Y_UNIT_TEST(LargerRecordsNeverNeedFewerChunks) {
            const ui64 charge = 10 * ui64(ChunkSize);
            ui64 previous = 0;
            for (ui32 maxInline = 0; maxInline <= (32u << 10); maxInline += 1024) {
                const ui64 chunks = TFreshOutputEstimate::CalculateChunks(charge, maxInline, Geometry());
                UNIT_ASSERT_C(chunks >= previous, "maxInline# " << maxInline);
                UNIT_ASSERT_C(chunks >= 10, "maxInline# " << maxInline);
                previous = chunks;
            }
        }

        Y_UNIT_TEST(MorePayloadNeverNeedsFewerChunks) {
            const ui32 maxInline = 8 << 10;
            ui64 previous = 0;
            for (ui64 charge = 0; charge <= 8 * ui64(ChunkSize); charge += 997) {
                const ui64 chunks = TFreshOutputEstimate::CalculateChunks(charge, maxInline, Geometry());
                UNIT_ASSERT_C(chunks >= previous, "charge# " << charge);
                previous = chunks;
            }
        }

        // An SST of several chunks is reserved whole: the writer may use every one of them.
        Y_UNIT_TEST(MultiChunkSstsAreCountedWhole) {
            for (ui32 chunksPerSst : {2u, 4u}) {
                for (ui64 charge : {ui64(1), ui64(ChunkSize), 3 * ui64(ChunkSize), 11 * ui64(ChunkSize)}) {
                    const ui64 chunks = TFreshOutputEstimate::CalculateChunks(charge, 4 << 10, Geometry(chunksPerSst));
                    UNIT_ASSERT_VALUES_EQUAL_C(chunks % chunksPerSst, 0, "charge# " << charge);
                    UNIT_ASSERT_C(chunks * ChunkSize >= charge, "charge# " << charge);
                }
            }
        }

        // A chunk that cannot even hold an empty SST cannot be compacted into: report it as needing
        // more than any disk has, rather than dividing by zero.
        Y_UNIT_TEST(DegenerateGeometryDoesNotCrash) {
            TFreshOutputGeometry geometry = Geometry();
            geometry.ChunkSize = 64;
            UNIT_ASSERT_VALUES_EQUAL(TFreshOutputEstimate::CalculateChunks(1, 0, geometry), Max<ui32>());
            UNIT_ASSERT_VALUES_EQUAL(TFreshOutputEstimate::CalculateChunks(0, 0, geometry), 0);
        }

        // Every SST but the last holds more than capacity - footprint, and nothing better can be said of it
        // however large the records get: that is the step, even for records past half an SST.
        Y_UNIT_TEST(LargeRecordsBoundTheStep) {
            TFreshOutputGeometry geometry = Geometry();
            geometry.TotalPartCount = 1;
            const ui32 maxInline = AlignDown<ui32>(ui32(SingleSstCapacity / 4 * 3 - RecordBytes - sizeof(TDiskPart)), 4);
            const ui64 step = SingleSstCapacity - (RecordBytes + maxInline + sizeof(TDiskPart));
            UNIT_ASSERT_VALUES_EQUAL(
                TFreshOutputEstimate::CalculateChunks(SingleSstCapacity + 3 * step, maxInline, geometry), 4);
            UNIT_ASSERT_VALUES_EQUAL(
                TFreshOutputEstimate::CalculateChunks(SingleSstCapacity + 3 * step + 1, maxInline, geometry), 5);
        }

        // A record that might not fit an SST cannot be compacted, however many chunks there are.
        Y_UNIT_TEST(RecordLargerThanSstCannotBeCompacted) {
            TFreshOutputGeometry geometry = Geometry();
            geometry.TotalPartCount = 1;
            UNIT_ASSERT_VALUES_EQUAL(
                TFreshOutputEstimate::CalculateChunks(SingleSstCapacity + 1, ui32(SingleSstCapacity), geometry), Max<ui32>());
        }

        // An operation's admission is taken back one record at a time, as its log records are replayed.
        Y_UNIT_TEST(AdmissionIsTakenBackRecordByRecord) {
            TFreshAdmission record;
            record.LogoBlobs.AddInline(100);
            TFreshAdmission admission;
            for (ui32 i = 0; i < 3; ++i) {
                admission.Merge(record);
            }
            UNIT_ASSERT_VALUES_EQUAL(admission.LogoBlobs.GetRecords(), 3);
            for (ui32 i = 0; i < 3; ++i) {
                UNIT_ASSERT(!admission.Empty());
                admission.Subtract(record);
            }
            UNIT_ASSERT(admission.Empty());
        }

        Y_UNIT_TEST(MergeIsTheSumOfBoth) {
            TFreshOutputEstimate a, b;
            a.AddInline(100);
            a.AddIndexOnly(2);
            b.AddHuge();
            b.AddInline(1000);
            TFreshOutputEstimate merged = a;
            merged.Merge(b);
            UNIT_ASSERT_VALUES_EQUAL(merged.GetRecords(), a.GetRecords() + b.GetRecords());
            UNIT_ASSERT_VALUES_EQUAL(merged.GetCharge(Geometry()), a.GetCharge(Geometry()) + b.GetCharge(Geometry()));
            UNIT_ASSERT_VALUES_EQUAL(merged.GetMaxInlineBytes(), 1000);
        }
    }

    // Every Fresh insert, whatever path it comes by, is charged as it lands in the segment.
    Y_UNIT_TEST_SUITE(TFreshSegmentOutputEstimate) {

        using TFreshSegment = ::NKikimr::TFreshSegment<TKeyLogoBlob, TMemRecLogoBlob>;
        using TAppendix = ::NKikimr::TFreshAppendix<TKeyLogoBlob, TMemRecLogoBlob>;
        constexpr ui32 RecordBytes = sizeof(TIndexRecord<TKeyLogoBlob, TMemRecLogoBlob>);

        Y_UNIT_TEST(SegmentChargesEveryInsert) {
            TTestContexts ctx(8 << 20);
            auto hullCtx = ctx.GetHullCtx();
            auto arena = std::make_shared<TRopeArena>(&TRopeArenaBackend::Allocate);
            auto seg = MakeIntrusive<TFreshSegment>(hullCtx, ui64(8) << 20, TInstant::Zero(), arena);

            const TFreshOutputGeometry& geometry = seg->GetOutputGeometry();
            UNIT_ASSERT_VALUES_EQUAL(geometry.ChunkSize, hullCtx->ChunkSize);
            UNIT_ASSERT_VALUES_EQUAL(geometry.AppendBlockSize, hullCtx->AppendBlockSize);
            UNIT_ASSERT_VALUES_EQUAL(geometry.ChunksPerSst, hullCtx->HullSstSizeInChunksFresh);
            UNIT_ASSERT_VALUES_EQUAL(geometry.TotalPartCount, hullCtx->VCtx->Top->GType.TotalPartCount());
            UNIT_ASSERT_VALUES_EQUAL(geometry.IndexRecordBytes, RecordBytes);
            UNIT_ASSERT(seg->GetOutputEstimate().Empty());
            UNIT_ASSERT_VALUES_EQUAL(seg->GetOutputChunks(), 0);

            ui64 lsn = 1;
            ui64 expected = 0;

            // An inline blob: charged as the DiskBlob Fresh builds around it, header included.
            {
                const TLogoBlobID id(1, 1, 1, 0, 1000, 0);
                const ui32 partSize = hullCtx->VCtx->Top->GType.PartSize(TLogoBlobID(id, 1));
                seg->PutLogoBlobWithData(lsn++, TKeyLogoBlob(id), 1, TIngress(), TRope(TString(partSize, 'x')),
                    std::nullopt);
                const ui32 blobSize = TDiskBlob::GetBlobHeaderSize(hullCtx->VCfg->BlobHeaderMode) + partSize;
                expected += RecordBytes + AlignUp<ui32>(blobSize, 4);
                UNIT_ASSERT_VALUES_EQUAL(seg->GetOutputEstimate().GetCharge(geometry), expected);
            }

            // A huge blob's index record, and the outbound entry merging may give it.
            {
                TMemRecLogoBlob memRec;
                memRec.SetHugeBlob(TDiskPart(1, 0, 1 << 20));
                seg->Put(lsn++, TKeyLogoBlob(TLogoBlobID(1, 1, 2, 0, 1 << 20, 0)), memRec);
                expected += RecordBytes + sizeof(TDiskPart);
                UNIT_ASSERT_VALUES_EQUAL(seg->GetOutputEstimate().GetCharge(geometry), expected);
            }

            // Metadata only.
            {
                TMemRecLogoBlob memRec;
                memRec.SetNoBlob();
                seg->Put(lsn++, TKeyLogoBlob(TLogoBlobID(1, 1, 3, 0, 0, 0)), memRec);
                expected += RecordBytes;
                UNIT_ASSERT_VALUES_EQUAL(seg->GetOutputEstimate().GetCharge(geometry), expected);
            }

            // A sync appendix: one index entry per record.
            {
                auto appendix = std::make_shared<TAppendix>(ctx.GetVCtx()->FreshIndex);
                TMemRecLogoBlob memRec;
                memRec.SetNoBlob();
                for (ui32 step = 10; step < 15; ++step) {
                    appendix->Add(TKeyLogoBlob(TLogoBlobID(1, 1, step, 0, 0, 0)), memRec);
                }
                seg->PutAppendix(std::move(appendix), lsn, lsn + 4);
                lsn += 5;
                expected += 5 * RecordBytes;
                UNIT_ASSERT_VALUES_EQUAL(seg->GetOutputEstimate().GetCharge(geometry), expected);
            }

            UNIT_ASSERT_VALUES_EQUAL(seg->GetOutputEstimate().GetRecords(), 8);
            UNIT_ASSERT_VALUES_EQUAL(seg->GetOutputChunks(), 1);
        }
    }

} // NKikimr
