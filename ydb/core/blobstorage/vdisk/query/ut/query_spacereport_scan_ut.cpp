#include <ydb/core/blobstorage/vdisk/query/query_spacereport_scan.h>

#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullds_ut.h>

#include <library/cpp/testing/unittest/registar.h>

namespace NKikimr::NVDiskSpaceReport {
namespace {

    TBlobStorageGroupType MakeGroupType() {
        TTestContexts contexts;
        return contexts.GetVCtx()->Top->GType;
    }

    TKeyLogoBlob MakeLogoBlobKey(ui32 blobSize = 1024) {
        return TKeyLogoBlob(TLogoBlobID(1, 1, 1, 0, blobSize, 0));
    }

    TIngress MakeLocalIngress(TBlobStorageGroupType gtype, std::initializer_list<ui8> partIndexes) {
        NMatrix::TVectorType local(0, gtype.TotalPartCount());
        for (ui8 partIdx : partIndexes) {
            local.Set(partIdx);
        }
        return TIngress().ReplaceLocal(gtype, local);
    }

    TMemRecLogoBlob MakeDiskBlob(
            TBlobStorageGroupType gtype,
            std::initializer_list<ui8> partIndexes,
            TDiskPart location)
    {
        TMemRecLogoBlob memRec(MakeLocalIngress(gtype, partIndexes));
        memRec.SetDiskBlob(location);
        return memRec;
    }

    TMemRecLogoBlob MakeHugeBlob(
            TBlobStorageGroupType gtype,
            ui8 partIdx,
            TDiskPart location)
    {
        TMemRecLogoBlob memRec(MakeLocalIngress(gtype, {partIdx}));
        memRec.SetHugeBlob(location);
        return memRec;
    }

    TMemRecLogoBlob MakeManyHugeBlob(
            TBlobStorageGroupType gtype,
            std::initializer_list<ui8> partIndexes,
            ui32 outboundIndex,
            ui32 outboundCount,
            ui32 totalSize)
    {
        TMemRecLogoBlob memRec(MakeLocalIngress(gtype, partIndexes));
        memRec.SetManyHugeBlobs(outboundIndex, outboundCount, totalSize);
        return memRec;
    }

    TMemRecLogoBlob MakeMemBlob(
            TBlobStorageGroupType gtype,
            ui8 partIdx,
            ui32 payloadBytes)
    {
        TMemRecLogoBlob memRec(MakeLocalIngress(gtype, {partIdx}));
        memRec.SetMemBlob(1, payloadBytes);
        return memRec;
    }

    Y_UNIT_TEST_SUITE(TSpaceReportAdditiveCoreTest) {
        Y_UNIT_TEST(BreakdownAddsAndTotalsFields) {
            TSpaceBreakdown breakdown;
            breakdown.UsefulBlobDataBytes = 10;
            breakdown.LiveMetadataBytes = 5;

            TSpaceBreakdown increment;
            increment.GcDeadBlobDataBytes = 7;
            increment.ChunkTailBytes = 3;
            breakdown += increment;

            UNIT_ASSERT_VALUES_EQUAL(breakdown.UsefulBlobDataBytes, 10);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.LiveMetadataBytes, 5);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.GcDeadBlobDataBytes, 7);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.ChunkTailBytes, 3);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.TotalBytes(), 25);
        }

        Y_UNIT_TEST(HugeExtentIsSplitIntoPayloadAndRecordMetadata) {
            TSpaceBreakdown breakdown;
            AddClassifiedHugeBlob(breakdown, {
                .Part = TDiskPart(1, 0, 120),
                .PayloadBytes = 100,
                .Classification = EHugeBlobClassification::Useful,
            });
            AddClassifiedHugeBlob(breakdown, {
                .Part = TDiskPart(1, 128, 80),
                .PayloadBytes = 60,
                .Classification = EHugeBlobClassification::GcDead,
            });
            AddClassifiedHugeBlob(breakdown, {
                .Part = TDiskPart(1, 256, 50),
                .PayloadBytes = 40,
                .Classification = EHugeBlobClassification::MergeRedundant,
            });

            UNIT_ASSERT_VALUES_EQUAL(breakdown.UsefulBlobDataBytes, 100);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.LiveMetadataBytes, 20);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.GcDeadBlobDataBytes, 60);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.GcDeadMetadataBytes, 20);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.MergeRedundantBlobDataBytes, 40);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.MergeRedundantMetadataBytes, 10);
            UNIT_ASSERT_VALUES_EQUAL(breakdown.TotalBytes(), 250);
        }

        Y_UNIT_TEST(PhysicalSstIsSampledOnlyAtItsLastKey) {
            using TSst = TLevelSegment<TKeyBlock, TMemRecBlock>;

            TTestContexts contexts;
            const TKeyBlock firstKey(10);
            const TKeyBlock lastKey(20);

            auto sst = MakeIntrusive<TSst>(contexts.GetVCtx());
            sst->LoadedIndex.emplace_back(firstKey, TMemRecBlock(1));
            sst->LoadedIndex.emplace_back(lastKey, TMemRecBlock(2));
            sst->AllChunks = {11, 12, 13};
            sst->Info.IndexParts = 3;

            TPhysicalSstEstimate estimate;
            estimate.AddIfLastKey<TKeyBlock, TMemRecBlock>(firstKey, sst.Get());
            UNIT_ASSERT_VALUES_EQUAL(estimate.SstCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(estimate.ChunkCount, 0);
            UNIT_ASSERT_VALUES_EQUAL(estimate.StructuralMetadataBytes, 0);

            estimate.AddIfLastKey<TKeyBlock, TMemRecBlock>(lastKey, sst.Get());
            UNIT_ASSERT_VALUES_EQUAL(estimate.SstCount, 1);
            UNIT_ASSERT_VALUES_EQUAL(estimate.ChunkCount, 3);
            UNIT_ASSERT_VALUES_EQUAL(
                estimate.StructuralMetadataBytes,
                sizeof(TIdxDiskPlaceHolder) + 2 * sizeof(TIdxDiskLinker));
        }
    }

    Y_UNIT_TEST_SUITE(TMetadataSpaceMergerTest) {
        Y_UNIT_TEST(FreshAffectsWinnerButOnlySstRecordsConsumeChunkSpace) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            TBlocksSpaceMerger merger(gtype, nullptr, true, true);
            const TKeyBlock key(42);

            merger.Clear();
            merger.AddFromSegment(TMemRecBlock(1), nullptr, key, 1, nullptr);
            merger.AddFromSegment(TMemRecBlock(2), nullptr, key, 2, nullptr);
            merger.AddFromFresh(TMemRecBlock(3), nullptr, key, 3);
            merger.Finish();

            const auto& estimate = merger.GetConclusion();
            const ui64 recordBytes = sizeof(TKeyBlock) + sizeof(TMemRecBlock);
            UNIT_ASSERT_VALUES_EQUAL(estimate.PhysicalSstRecords, 2);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Breakdown.LiveMetadataBytes, recordBytes);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Breakdown.MergeRedundantMetadataBytes, recordBytes);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Breakdown.TotalBytes(), 2 * recordBytes);

            TSemanticScanCounters counters;
            counters.AddBlocksKey(estimate);
            UNIT_ASSERT_VALUES_EQUAL(counters.Blocks.TotalBytes(), 2 * recordBytes);
        }

        Y_UNIT_TEST(BarrierRecordsUseTheSameMetadataAccounting) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            TBarriersSpaceMerger merger(gtype, nullptr, true, true);
            const TKeyBarrier key(42, 1, 2, 3, false);
            const TMemRecBarrier memRec(1, 10, TBarrierIngress());

            merger.Clear();
            merger.AddFromSegment(memRec, nullptr, key, 1, nullptr);
            merger.AddFromSegment(memRec, nullptr, key, 2, nullptr);
            merger.Finish();

            const auto& estimate = merger.GetConclusion();
            const ui64 recordBytes = sizeof(TKeyBarrier) + sizeof(TMemRecBarrier);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Breakdown.LiveMetadataBytes, recordBytes);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Breakdown.MergeRedundantMetadataBytes, recordBytes);

            TSemanticScanCounters counters;
            counters.AddBarriersKey(estimate);
            UNIT_ASSERT_VALUES_EQUAL(counters.Barriers.TotalBytes(), 2 * recordBytes);
        }
    }

    Y_UNIT_TEST_SUITE(TLogoBlobSpaceMergerTest) {
        Y_UNIT_TEST(InplacedDataIsSplitIntoLiveAndMergeRedundantPhysicalBytes) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            const TKeyLogoBlob key = MakeLogoBlobKey();
            const ui32 payloadBytes = gtype.PartSize(TLogoBlobID(key.LogoBlobID(), 1));
            constexpr ui32 headerBytes = 16;
            TLogoBlobSpaceMerger merger(gtype, nullptr, true, true, 32);

            merger.Clear();
            merger.AddFromSegment(
                MakeDiskBlob(gtype, {0}, TDiskPart(10, 0, payloadBytes + headerBytes)),
                nullptr, key, 1, nullptr);
            merger.AddFromSegment(
                MakeDiskBlob(gtype, {0}, TDiskPart(11, 0, payloadBytes + headerBytes)),
                nullptr, key, 2, nullptr);
            merger.Finish();

            const auto& estimate = merger.GetConclusion();
            const ui64 indexRecordBytes = sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob);
            const ui64 recordSize = payloadBytes + headerBytes;
            const ui64 writePadding = AlignUp<ui64>(recordSize, 4) - recordSize;
            UNIT_ASSERT_VALUES_EQUAL(estimate.PhysicalSstRecords, 2);
            UNIT_ASSERT_VALUES_EQUAL(estimate.PhysicalMetadataBytes, 2 * indexRecordBytes);
            UNIT_ASSERT_VALUES_EQUAL(estimate.PhysicalInplacedBytes, 2 * (payloadBytes + headerBytes));
            UNIT_ASSERT_VALUES_EQUAL(estimate.Hull.UsefulBlobDataBytes, payloadBytes);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Hull.MergeRedundantBlobDataBytes, payloadBytes);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Hull.LiveMetadataBytes, indexRecordBytes + headerBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                estimate.Hull.MergeRedundantMetadataBytes,
                indexRecordBytes + headerBytes);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Hull.WritePaddingBytes, 2 * writePadding);
        }

        Y_UNIT_TEST(FreshPayloadWinsWithoutAddingCommonLogBytes) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            const TKeyLogoBlob key = MakeLogoBlobKey();
            const ui32 payloadBytes = gtype.PartSize(TLogoBlobID(key.LogoBlobID(), 1));
            constexpr ui32 headerBytes = 8;
            TLogoBlobSpaceMerger merger(gtype, nullptr, true, true, 32);

            merger.Clear();
            merger.AddFromSegment(
                MakeDiskBlob(gtype, {0}, TDiskPart(10, 0, payloadBytes + headerBytes)),
                nullptr, key, 1, nullptr);
            merger.AddFromFresh(MakeMemBlob(gtype, 0, payloadBytes), nullptr, key, 2);
            merger.Finish();

            const auto& estimate = merger.GetConclusion();
            const ui64 indexRecordBytes = sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob);
            const ui64 recordSize = payloadBytes + headerBytes;
            const ui64 writePadding = AlignUp<ui64>(recordSize, 4) - recordSize;
            UNIT_ASSERT_VALUES_EQUAL(estimate.Hull.UsefulBlobDataBytes, 0);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Hull.MergeRedundantBlobDataBytes, payloadBytes);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Hull.LiveMetadataBytes, indexRecordBytes);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Hull.MergeRedundantMetadataBytes, headerBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                estimate.Hull.TotalBytes(),
                indexRecordBytes + payloadBytes + headerBytes + writePadding);
        }

        Y_UNIT_TEST(InplacedWinnerUsesHighestCircaLsnIndependentOfTraversalOrder) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            const TKeyLogoBlob key = MakeLogoBlobKey();
            const ui32 payloadBytes = gtype.PartSize(TLogoBlobID(key.LogoBlobID(), 1));
            constexpr ui32 oldHeaderBytes = 8;
            constexpr ui32 newHeaderBytes = 20;
            TLogoBlobSpaceMerger merger(gtype, nullptr, true, true, 32);

            merger.Clear();
            merger.AddFromSegment(
                MakeDiskBlob(gtype, {0}, TDiskPart(10, 0, payloadBytes + newHeaderBytes)),
                nullptr, key, 2, nullptr);
            merger.AddFromSegment(
                MakeDiskBlob(gtype, {0}, TDiskPart(11, 0, payloadBytes + oldHeaderBytes)),
                nullptr, key, 1, nullptr);
            merger.Finish();

            const auto& estimate = merger.GetConclusion();
            const ui64 indexRecordBytes = sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob);
            UNIT_ASSERT_VALUES_EQUAL(estimate.Hull.UsefulBlobDataBytes, payloadBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                estimate.Hull.LiveMetadataBytes,
                indexRecordBytes + newHeaderBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                estimate.Hull.MergeRedundantMetadataBytes,
                indexRecordBytes + oldHeaderBytes);
        }

        Y_UNIT_TEST(HugeReferencesAreDeduplicatedByPhysicalSlot) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            const TKeyLogoBlob key = MakeLogoBlobKey();
            const ui32 payloadBytes = gtype.PartSize(TLogoBlobID(key.LogoBlobID(), 1));
            const TDiskPart oldPart(20, 0, payloadBytes + 16);
            const TDiskPart winningPart(20, 4096, payloadBytes + 16);
            TLogoBlobSpaceMerger merger(gtype, nullptr, true, true, 32);

            merger.Clear();
            merger.AddFromSegment(MakeHugeBlob(gtype, 0, oldPart), nullptr, key, 1, nullptr);
            merger.AddFromSegment(MakeHugeBlob(gtype, 0, winningPart), nullptr, key, 2, nullptr);
            // A copied SST index record still points to the same physical
            // slot. It must not produce another allocator-side extent.
            merger.AddFromSegment(MakeHugeBlob(gtype, 0, winningPart), nullptr, key, 2, nullptr);
            merger.Finish();

            const auto& estimate = merger.GetConclusion();
            UNIT_ASSERT(!estimate.HugeRefsOverflow);
            UNIT_ASSERT_VALUES_EQUAL(estimate.HugeRefCountSeen, 3);
            UNIT_ASSERT_VALUES_EQUAL(
                estimate.HugeReferencedBytesSeen,
                oldPart.Size + 2 * winningPart.Size);
            UNIT_ASSERT_VALUES_EQUAL(estimate.HugeBlobs.size(), 2);

            ui32 useful = 0;
            ui32 redundant = 0;
            for (const TClassifiedHugeBlob& blob : estimate.HugeBlobs) {
                useful += blob.Classification == EHugeBlobClassification::Useful;
                redundant += blob.Classification == EHugeBlobClassification::MergeRedundant;
            }
            UNIT_ASSERT_VALUES_EQUAL(useful, 1);
            UNIT_ASSERT_VALUES_EQUAL(redundant, 1);

            TSemanticScanCounters counters;
            counters.AddLogoBlobKey(estimate);
            UNIT_ASSERT_VALUES_EQUAL(counters.HugeBlobs.UsefulBlobDataBytes, payloadBytes);
            UNIT_ASSERT_VALUES_EQUAL(counters.HugeBlobs.MergeRedundantBlobDataBytes, payloadBytes);
            UNIT_ASSERT_VALUES_EQUAL(counters.HugeBlobs.TotalBytes(), oldPart.Size + winningPart.Size);
        }

        Y_UNIT_TEST(MixedInplacedAndHugeDataFollowsTargetLayout) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            const TKeyLogoBlob key = MakeLogoBlobKey();
            const ui32 payloadBytes = gtype.PartSize(TLogoBlobID(key.LogoBlobID(), 1));
            constexpr ui32 headerBytes = 16;
            const TDiskPart inplacedPart(10, 0, payloadBytes + headerBytes);
            const TDiskPart hugePart(20, 0, payloadBytes + headerBytes);
            const THugeBlobCtx hugeBlobCtx("", nullptr, EBlobHeaderMode::OLD_HEADER);

            TLogoBlobSpaceMerger inplacedTarget(gtype, nullptr, true, true, 32);
            inplacedTarget.Clear();
            inplacedTarget.AddFromSegment(
                MakeDiskBlob(gtype, {0}, inplacedPart), nullptr, key, 2, nullptr);
            inplacedTarget.AddFromSegment(
                MakeHugeBlob(gtype, 0, hugePart), nullptr, key, 1, nullptr);
            inplacedTarget.Finish();

            const auto& inplacedEstimate = inplacedTarget.GetConclusion();
            UNIT_ASSERT_VALUES_EQUAL(inplacedEstimate.Hull.UsefulBlobDataBytes, payloadBytes);
            UNIT_ASSERT_VALUES_EQUAL(inplacedEstimate.Hull.MergeRedundantBlobDataBytes, 0);
            UNIT_ASSERT_VALUES_EQUAL(inplacedEstimate.HugeBlobs.size(), 1);
            UNIT_ASSERT(
                inplacedEstimate.HugeBlobs.front().Classification ==
                EHugeBlobClassification::MergeRedundant);

            TLogoBlobSpaceMerger hugeTarget(
                gtype, nullptr, true, true, 32, &hugeBlobCtx, 1);
            hugeTarget.Clear();
            hugeTarget.AddFromSegment(
                MakeDiskBlob(gtype, {0}, inplacedPart), nullptr, key, 2, nullptr);
            hugeTarget.AddFromSegment(
                MakeHugeBlob(gtype, 0, hugePart), nullptr, key, 1, nullptr);
            hugeTarget.Finish();

            const auto& hugeEstimate = hugeTarget.GetConclusion();
            UNIT_ASSERT_VALUES_EQUAL(hugeEstimate.Hull.UsefulBlobDataBytes, 0);
            UNIT_ASSERT_VALUES_EQUAL(hugeEstimate.Hull.MergeRedundantBlobDataBytes, payloadBytes);
            UNIT_ASSERT_VALUES_EQUAL(hugeEstimate.HugeBlobs.size(), 1);
            UNIT_ASSERT(
                hugeEstimate.HugeBlobs.front().Classification ==
                EHugeBlobClassification::Useful);
        }

        Y_UNIT_TEST(TargetHugeLayoutRetainsExistingHugeDataOverFreshData) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            const TKeyLogoBlob key = MakeLogoBlobKey();
            const ui32 payloadBytes = gtype.PartSize(TLogoBlobID(key.LogoBlobID(), 1));
            const TDiskPart hugePart(20, 0, payloadBytes + 16);
            const THugeBlobCtx hugeBlobCtx("", nullptr, EBlobHeaderMode::OLD_HEADER);

            TLogoBlobSpaceMerger merger(
                gtype, nullptr, true, true, 32, &hugeBlobCtx, 1);
            merger.Clear();
            merger.AddFromFresh(MakeMemBlob(gtype, 0, payloadBytes), nullptr, key, 2);
            merger.AddFromSegment(
                MakeHugeBlob(gtype, 0, hugePart), nullptr, key, 1, nullptr);
            merger.Finish();

            const auto& estimate = merger.GetConclusion();
            UNIT_ASSERT_VALUES_EQUAL(estimate.HugeBlobs.size(), 1);
            UNIT_ASSERT(
                estimate.HugeBlobs.front().Classification ==
                EHugeBlobClassification::Useful);
        }

        Y_UNIT_TEST(ManyHugeOutboundMetadataKeepsOneMergedArray) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            const TKeyLogoBlob key = MakeLogoBlobKey();
            const ui32 firstPayloadBytes = gtype.PartSize(TLogoBlobID(key.LogoBlobID(), 1));
            const ui32 secondPayloadBytes = gtype.PartSize(TLogoBlobID(key.LogoBlobID(), 2));
            const std::array<TDiskPart, 4> outbound = {
                TDiskPart(20, 0, firstPayloadBytes + 8),
                TDiskPart(20, 4096, secondPayloadBytes + 8),
                TDiskPart(21, 0, firstPayloadBytes + 8),
                TDiskPart(21, 4096, secondPayloadBytes + 8),
            };
            TLogoBlobSpaceMerger merger(gtype, nullptr, true, true, 32);

            merger.Clear();
            merger.AddFromSegment(
                MakeManyHugeBlob(
                    gtype, {0, 1}, 0, 2, firstPayloadBytes + secondPayloadBytes + 16),
                outbound.data(), key, 1, nullptr);
            merger.AddFromSegment(
                MakeManyHugeBlob(
                    gtype, {0, 1}, 2, 2, firstPayloadBytes + secondPayloadBytes + 16),
                outbound.data(), key, 2, nullptr);
            merger.Finish();

            const auto& estimate = merger.GetConclusion();
            const ui64 indexRecordBytes = sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob);
            const ui64 outboundArrayBytes = 2 * sizeof(TDiskPart);
            UNIT_ASSERT(!estimate.HugeRefsOverflow);
            UNIT_ASSERT_VALUES_EQUAL(
                estimate.PhysicalMetadataBytes,
                2 * indexRecordBytes + 2 * outboundArrayBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                estimate.Hull.LiveMetadataBytes,
                indexRecordBytes + outboundArrayBytes);
            UNIT_ASSERT_VALUES_EQUAL(
                estimate.Hull.MergeRedundantMetadataBytes,
                indexRecordBytes + outboundArrayBytes);

            ui32 useful = 0;
            ui32 redundant = 0;
            for (const TClassifiedHugeBlob& blob : estimate.HugeBlobs) {
                useful += blob.Classification == EHugeBlobClassification::Useful;
                redundant += blob.Classification == EHugeBlobClassification::MergeRedundant;
            }
            UNIT_ASSERT_VALUES_EQUAL(useful, 2);
            UNIT_ASSERT_VALUES_EQUAL(redundant, 2);
        }

        Y_UNIT_TEST(HugeReferenceLimitFallsBackWithoutRetainingPartialClassification) {
            const TBlobStorageGroupType gtype = MakeGroupType();
            const TKeyLogoBlob key = MakeLogoBlobKey();
            const ui32 payloadBytes = gtype.PartSize(TLogoBlobID(key.LogoBlobID(), 1));
            const TDiskPart first(20, 0, payloadBytes + 8);
            const TDiskPart second(20, 4096, payloadBytes + 8);
            TLogoBlobSpaceMerger merger(gtype, nullptr, true, true, 1);

            merger.Clear();
            merger.AddFromSegment(MakeHugeBlob(gtype, 0, first), nullptr, key, 1, nullptr);
            merger.AddFromSegment(MakeHugeBlob(gtype, 0, second), nullptr, key, 2, nullptr);
            merger.Finish();

            const auto& estimate = merger.GetConclusion();
            UNIT_ASSERT(estimate.HugeRefsOverflow);
            UNIT_ASSERT(estimate.HugeBlobs.empty());
            UNIT_ASSERT_VALUES_EQUAL(estimate.HugeRefCountSeen, 2);
            UNIT_ASSERT_VALUES_EQUAL(estimate.HugeReferencedBytesSeen, first.Size + second.Size);

            TSemanticScanCounters counters;
            counters.AddLogoBlobKey(estimate);
            UNIT_ASSERT_VALUES_EQUAL(counters.HugeOverflowKeys, 1);
            UNIT_ASSERT_VALUES_EQUAL(counters.HugeOverflowRefCount, 2);
            UNIT_ASSERT_VALUES_EQUAL(counters.HugeOverflowReferencedBytes, first.Size + second.Size);
            UNIT_ASSERT_VALUES_EQUAL(counters.HugeBlobs.TotalBytes(), 0);
        }
    }

} // anonymous namespace
} // namespace NKikimr::NVDiskSpaceReport
