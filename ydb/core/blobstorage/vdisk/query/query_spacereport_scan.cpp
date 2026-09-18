#include "query_spacereport_scan.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_hugeblobctx.h>

#include <algorithm>

namespace NKikimr::NVDiskSpaceReport {
namespace {

    bool SameSlot(const TDiskPart& left, const TDiskPart& right) {
        return left.ChunkIdx == right.ChunkIdx && left.Offset == right.Offset;
    }

} // anonymous namespace

    TSpaceBreakdown& TSpaceBreakdown::operator+=(const TSpaceBreakdown& other) {
        UsefulBlobDataBytes += other.UsefulBlobDataBytes;
        LiveMetadataBytes += other.LiveMetadataBytes;
        LiveAuxiliaryDataBytes += other.LiveAuxiliaryDataBytes;
        GcDeadBlobDataBytes += other.GcDeadBlobDataBytes;
        GcDeadMetadataBytes += other.GcDeadMetadataBytes;
        MergeRedundantBlobDataBytes += other.MergeRedundantBlobDataBytes;
        MergeRedundantMetadataBytes += other.MergeRedundantMetadataBytes;
        WritePaddingBytes += other.WritePaddingBytes;
        SlotInternalFragmentationBytes += other.SlotInternalFragmentationBytes;
        FreeSlotBytes += other.FreeSlotBytes;
        ChunkTailBytes += other.ChunkTailBytes;
        FreeChunkReserveBytes += other.FreeChunkReserveBytes;
        LockedOrQuarantinedBytes += other.LockedOrQuarantinedBytes;
        UnclassifiedBytes += other.UnclassifiedBytes;
        return *this;
    }

    ui64 TSpaceBreakdown::TotalBytes() const {
        return UsefulBlobDataBytes
            + LiveMetadataBytes
            + LiveAuxiliaryDataBytes
            + GcDeadBlobDataBytes
            + GcDeadMetadataBytes
            + MergeRedundantBlobDataBytes
            + MergeRedundantMetadataBytes
            + WritePaddingBytes
            + SlotInternalFragmentationBytes
            + FreeSlotBytes
            + ChunkTailBytes
            + FreeChunkReserveBytes
            + LockedOrQuarantinedBytes
            + UnclassifiedBytes;
    }

    void AddClassifiedHugeBlob(TSpaceBreakdown& breakdown, const TClassifiedHugeBlob& blob) {
        const ui64 payloadBytes = Min<ui64>(blob.PayloadBytes, blob.Part.Size);
        const ui64 metadataBytes = blob.Part.Size - payloadBytes;
        switch (blob.Classification) {
            case EHugeBlobClassification::Useful:
                breakdown.UsefulBlobDataBytes += payloadBytes;
                breakdown.LiveMetadataBytes += metadataBytes;
                break;

            case EHugeBlobClassification::GcDead:
                breakdown.GcDeadBlobDataBytes += payloadBytes;
                breakdown.GcDeadMetadataBytes += metadataBytes;
                break;

            case EHugeBlobClassification::MergeRedundant:
                breakdown.MergeRedundantBlobDataBytes += payloadBytes;
                breakdown.MergeRedundantMetadataBytes += metadataBytes;
                break;
        }
    }

    void TSemanticScanCounters::AddLogoBlobKey(const TLogoBlobKeyEstimate& estimate) {
        LogoBlobs += estimate.Hull;
        if (estimate.HugeRefsOverflow) {
            ++HugeOverflowKeys;
            HugeOverflowRefCount += estimate.HugeRefCountSeen;
            HugeOverflowReferencedBytes += estimate.HugeReferencedBytesSeen;
            return;
        }

        for (const TClassifiedHugeBlob& blob : estimate.HugeBlobs) {
            AddClassifiedHugeBlob(HugeBlobs, blob);
        }
    }

    TLogoBlobSpaceMerger::TLogoBlobSpaceMerger(
            TBlobStorageGroupType gtype,
            const NGcOpt::TBarriersEssence* barriers,
            bool allowKeepFlags,
            bool allowGarbageCollection,
            size_t maxHugeRefsPerKey,
            const THugeBlobCtx* hugeBlobCtx,
            ui32 minHugeBlobInBytes)
        : GType(gtype)
        , Barriers(barriers)
        , AllowKeepFlags(allowKeepFlags)
        , AllowGarbageCollection(allowGarbageCollection)
        , MaxHugeRefsPerKey(maxHugeRefsPerKey)
        , HugeBlobCtx(hugeBlobCtx)
        , MinHugeBlobInBytes(minHugeBlobInBytes)
        , IndexMerger(gtype)
    {
        HugeRefs.reserve(Min<size_t>(MaxHugeRefsPerKey, 64));
        Clear();
    }

    void TLogoBlobSpaceMerger::Clear() {
        IndexMerger.Clear();
        Key.reset();
        HasMemSource.fill(false);
        InplacedWinners.fill({});
        HugeWinners.fill({});
        HugeRefs.clear();
        BaseMetadataBytes = 0;
        OutboundMetadataBytes = 0;
        InplacedPayloadBytes = 0;
        InplacedMetadataBytes = 0;
        InplacedUnclassifiedBytes = 0;
        NextInplacedRecordId = 1;
        HugeRefCountSeen = 0;
        HugeReferencedBytesSeen = 0;
        HugeRefsOverflow = false;
        TargetingHugeBlob = false;
        ProducingHugeBlob = false;
        Finished = false;
        Conclusion.Hull = {};
        Conclusion.HugeBlobs.clear();
        Conclusion.PhysicalSstRecords = 0;
        Conclusion.PhysicalMetadataBytes = 0;
        Conclusion.PhysicalInplacedBytes = 0;
        Conclusion.PhysicalSsts = {};
        Conclusion.HugeRefCountSeen = 0;
        Conclusion.HugeReferencedBytesSeen = 0;
        Conclusion.HugeRefsOverflow = false;
    }

    void TLogoBlobSpaceMerger::CheckKey(const TKeyLogoBlob& key) {
        Y_DEBUG_ABORT_UNLESS(!Key || *Key == key);
        Key = key;
    }

    void TLogoBlobSpaceMerger::MergeIndexFromFresh(
            const TMemRecLogoBlob& memRec,
            const TKeyLogoBlob& key,
            ui64 lsn)
    {
        IndexMerger.AddFromFresh(memRec, nullptr, key, lsn);
    }

    void TLogoBlobSpaceMerger::MergeIndexFromSegment(
            const TMemRecLogoBlob& memRec,
            const TDiskPart* outbound,
            const TKeyLogoBlob& key,
            ui64 circaLsn,
            const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>* sst)
    {
        IndexMerger.AddFromSegment(memRec, outbound, key, circaLsn, sst);
    }

    void TLogoBlobSpaceMerger::RecordMemParts(const TMemRecLogoBlob& memRec) {
        if (memRec.GetType() != TBlobType::MemBlob) {
            return;
        }

        for (ui8 partIdx : memRec.GetLocalParts(GType)) {
            if (partIdx < HasMemSource.size()) {
                HasMemSource[partIdx] = true;
            }
        }
    }

    void TLogoBlobSpaceMerger::RecordInplacedData(
            const TMemRecLogoBlob& memRec,
            const TDiskPart& location,
            ui64 circaLsn)
    {
        const NMatrix::TVectorType local = memRec.GetLocalParts(GType);
        if (location.Empty() || local.Empty()) {
            InplacedUnclassifiedBytes += location.Size;
            return;
        }

        ui64 payloadBytes = 0;
        std::array<ui32, MaxTotalPartCount> partBytes = {};
        for (ui8 partIdx : local) {
            if (partIdx >= partBytes.size()) {
                InplacedUnclassifiedBytes += location.Size;
                return;
            }
            const ui32 bytes = GType.PartSize(TLogoBlobID(Key->LogoBlobID(), partIdx + 1));
            partBytes[partIdx] = bytes;
            payloadBytes += bytes;
        }

        if (payloadBytes > location.Size) {
            InplacedUnclassifiedBytes += location.Size;
            return;
        }

        const ui32 recordMetadataBytes = location.Size - payloadBytes;
        InplacedPayloadBytes += payloadBytes;
        InplacedMetadataBytes += recordMetadataBytes;

        const ui64 recordId = NextInplacedRecordId++;
        for (ui8 partIdx : local) {
            TInplacedWinner& winner = InplacedWinners[partIdx];
            if (!winner.Present || winner.CircaLsn < circaLsn ||
                    (winner.CircaLsn == circaLsn && winner.Location < location)) {
                winner = {
                    .RecordId = recordId,
                    .Location = location,
                    .CircaLsn = circaLsn,
                    .PayloadBytes = partBytes[partIdx],
                    .RecordMetadataBytes = recordMetadataBytes,
                    .Present = true,
                };
            }
        }
    }

    void TLogoBlobSpaceMerger::RecordHugeRefs(
            const TMemRecLogoBlob& memRec,
            const TDiskPart* outbound,
            ui64 circaLsn)
    {
        const TBlobType::EType type = memRec.GetType();
        if (type != TBlobType::HugeBlob && type != TBlobType::ManyHugeBlobs) {
            return;
        }

        if (type == TBlobType::ManyHugeBlobs && !outbound) {
            HugeRefsOverflow = true;
            HugeRefs.clear();
            HugeRefCountSeen += memRec.GetLocalParts(GType).CountBits();
            HugeReferencedBytesSeen += memRec.DataSize();
            return;
        }

        TDiskDataExtractor extractor;
        memRec.GetDiskData(&extractor, outbound);
        const NMatrix::TVectorType local = memRec.GetLocalParts(GType);
        if (local.CountBits() != size_t(extractor.End - extractor.Begin)) {
            HugeRefsOverflow = true;
            HugeRefs.clear();
            for (const TDiskPart* part = extractor.Begin; part != extractor.End; ++part) {
                ++HugeRefCountSeen;
                HugeReferencedBytesSeen += part->Size;
            }
            return;
        }

        const TDiskPart* part = extractor.Begin;
        for (ui8 partIdx : local) {
            const TDiskPart location = *part++;
            ++HugeRefCountSeen;
            HugeReferencedBytesSeen += location.Size;

            if (HugeRefsOverflow || location.Empty()) {
                continue;
            }
            if (partIdx >= HugeWinners.size() || HugeRefs.size() >= MaxHugeRefsPerKey) {
                HugeRefsOverflow = true;
                HugeRefs.clear();
                HugeWinners.fill({});
                continue;
            }

            HugeRefs.push_back({
                .Part = location,
                .CircaLsn = circaLsn,
                .PartIdx = partIdx,
            });
            TWinner& winner = HugeWinners[partIdx];
            if (!winner.Present || winner.CircaLsn < circaLsn ||
                    (winner.CircaLsn == circaLsn && winner.Part < location)) {
                winner = {
                    .Part = location,
                    .CircaLsn = circaLsn,
                    .Present = true,
                };
            }
        }
    }

    void TLogoBlobSpaceMerger::AddFromFresh(
            const TMemRecLogoBlob& memRec,
            const TRope*,
            const TKeyLogoBlob& key,
            ui64 lsn)
    {
        CheckKey(key);
        MergeIndexFromFresh(memRec, key, lsn);
        RecordMemParts(memRec);
        RecordHugeRefs(memRec, nullptr, lsn);
    }

    void TLogoBlobSpaceMerger::AddFromSegment(
            const TMemRecLogoBlob& memRec,
            const TDiskPart* outbound,
            const TKeyLogoBlob& key,
            ui64 circaLsn,
            const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>* sst)
    {
        CheckKey(key);
        MergeIndexFromSegment(memRec, outbound, key, circaLsn, sst);
        BaseMetadataBytes += sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob);
        ++Conclusion.PhysicalSstRecords;
        Conclusion.PhysicalSsts.AddIfLastKey<TKeyLogoBlob, TMemRecLogoBlob>(key, sst);

        const TBlobType::EType type = memRec.GetType();
        if (type == TBlobType::MemBlob || !memRec.HasData()) {
            RecordMemParts(memRec);
            return;
        }

        TDiskDataExtractor extractor;
        if (type == TBlobType::ManyHugeBlobs && !outbound) {
            RecordHugeRefs(memRec, outbound, circaLsn);
            return;
        }
        memRec.GetDiskData(&extractor, outbound);
        if (type == TBlobType::DiskBlob) {
            const TDiskPart& location = extractor.SwearOne();
            RecordInplacedData(memRec, location, circaLsn);
            Conclusion.Hull.WritePaddingBytes += AlignUp<ui32>(location.Size, 4) - location.Size;
        } else {
            if (type == TBlobType::ManyHugeBlobs) {
                OutboundMetadataBytes += size_t(extractor.End - extractor.Begin) * sizeof(TDiskPart);
            }
            RecordHugeRefs(memRec, outbound, circaLsn);
        }
    }

    void TLogoBlobSpaceMerger::ClassifyHugeRefs(bool keepData) {
        if (HugeRefsOverflow) {
            Conclusion.HugeRefsOverflow = true;
            return;
        }

        std::sort(HugeRefs.begin(), HugeRefs.end(), [](const TRawHugeRef& left, const TRawHugeRef& right) {
            if (left.Part != right.Part) {
                return left.Part < right.Part;
            }
            if (left.PartIdx != right.PartIdx) {
                return left.PartIdx < right.PartIdx;
            }
            return left.CircaLsn < right.CircaLsn;
        });

        // Validate all groups before committing any of them. Different sizes
        // for one physical address make the whole key unsafe to subdivide.
        for (size_t begin = 0; begin != HugeRefs.size();) {
            size_t end = begin + 1;
            while (end != HugeRefs.size() && SameSlot(HugeRefs[begin].Part, HugeRefs[end].Part)) {
                if (HugeRefs[begin].Part.Size != HugeRefs[end].Part.Size) {
                    Conclusion.HugeRefsOverflow = true;
                    return;
                }
                ++end;
            }
            begin = end;
        }

        for (size_t begin = 0; begin != HugeRefs.size();) {
            size_t end = begin + 1;
            while (end != HugeRefs.size() && SameSlot(HugeRefs[begin].Part, HugeRefs[end].Part)) {
                ++end;
            }

            EHugeBlobClassification classification = keepData
                ? EHugeBlobClassification::MergeRedundant
                : EHugeBlobClassification::GcDead;
            ui32 payloadBytes = 0;
            for (size_t index = begin; index != end; ++index) {
                const TRawHugeRef& ref = HugeRefs[index];
                const ui32 refPayloadBytes = Min<ui32>(
                    ref.Part.Size,
                    GType.PartSize(TLogoBlobID(Key->LogoBlobID(), ref.PartIdx + 1)));

                const TWinner& winner = HugeWinners[ref.PartIdx];
                const bool useful = keepData &&
                    winner.Present &&
                    ref.Part == winner.Part &&
                    ref.CircaLsn == winner.CircaLsn &&
                    (ProducingHugeBlob ||
                        (!HasMemSource[ref.PartIdx] && !InplacedWinners[ref.PartIdx].Present));
                if (useful) {
                    classification = EHugeBlobClassification::Useful;
                    payloadBytes = Max(payloadBytes, refPayloadBytes);
                } else if (classification != EHugeBlobClassification::Useful) {
                    payloadBytes = Max(payloadBytes, refPayloadBytes);
                }
            }

            Conclusion.HugeBlobs.push_back({
                .Part = HugeRefs[begin].Part,
                .PayloadBytes = payloadBytes,
                .Classification = classification,
            });
            begin = end;
        }
    }

    void TLogoBlobSpaceMerger::ClassifyInplacedData(bool keepData) {
        Conclusion.PhysicalInplacedBytes = InplacedPayloadBytes;
        Conclusion.PhysicalInplacedBytes += InplacedMetadataBytes;
        Conclusion.PhysicalInplacedBytes += InplacedUnclassifiedBytes;
        Conclusion.Hull.UnclassifiedBytes += InplacedUnclassifiedBytes;

        if (!keepData) {
            Conclusion.Hull.GcDeadBlobDataBytes += InplacedPayloadBytes;
            Conclusion.Hull.GcDeadMetadataBytes += InplacedMetadataBytes;
            return;
        }

        ui64 usefulPayloadBytes = 0;
        ui64 liveMetadataBytes = 0;
        std::array<ui64, MaxTotalPartCount> selectedRecordIds = {};
        size_t selectedRecordCount = 0;

        for (ui8 partIdx = 0; partIdx != InplacedWinners.size(); ++partIdx) {
            const TInplacedWinner& winner = InplacedWinners[partIdx];
            if (!winner.Present || HasMemSource[partIdx] ||
                    (ProducingHugeBlob && HugeWinners[partIdx].Present)) {
                continue;
            }

            usefulPayloadBytes += winner.PayloadBytes;
            bool seenRecord = false;
            for (size_t index = 0; index != selectedRecordCount; ++index) {
                seenRecord |= selectedRecordIds[index] == winner.RecordId;
            }
            if (!seenRecord) {
                selectedRecordIds[selectedRecordCount++] = winner.RecordId;
                liveMetadataBytes += winner.RecordMetadataBytes;
            }
        }

        usefulPayloadBytes = Min(usefulPayloadBytes, InplacedPayloadBytes);
        liveMetadataBytes = Min(liveMetadataBytes, InplacedMetadataBytes);
        Conclusion.Hull.UsefulBlobDataBytes += usefulPayloadBytes;
        Conclusion.Hull.LiveMetadataBytes += liveMetadataBytes;
        Conclusion.Hull.MergeRedundantBlobDataBytes += InplacedPayloadBytes - usefulPayloadBytes;
        Conclusion.Hull.MergeRedundantMetadataBytes += InplacedMetadataBytes - liveMetadataBytes;
    }

    void TLogoBlobSpaceMerger::ClassifyIndexMetadata(bool keepIndex, bool keepData) {
        Conclusion.PhysicalMetadataBytes = BaseMetadataBytes;
        Conclusion.PhysicalMetadataBytes += OutboundMetadataBytes;

        if (!keepIndex) {
            Conclusion.Hull.GcDeadMetadataBytes += Conclusion.PhysicalMetadataBytes;
            return;
        }

        const ui64 oneRecordBytes = sizeof(TKeyLogoBlob) + sizeof(TMemRecLogoBlob);
        const ui64 liveBaseBytes = Min(BaseMetadataBytes, oneRecordBytes);
        Conclusion.Hull.LiveMetadataBytes += liveBaseBytes;
        Conclusion.Hull.MergeRedundantMetadataBytes += BaseMetadataBytes - liveBaseBytes;

        if (Conclusion.HugeRefsOverflow) {
            Conclusion.Hull.UnclassifiedBytes += OutboundMetadataBytes;
            return;
        }

        const TMemRecLogoBlob& merged = IndexMerger.GetMemRec();
        const ui64 localPartCount = merged.GetLocalParts(GType).CountBits();
        const ui64 requiredOutboundBytes = keepData && ProducingHugeBlob && localPartCount > 1
            ? localPartCount * sizeof(TDiskPart)
            : 0;
        const ui64 liveOutboundBytes = Min(OutboundMetadataBytes, requiredOutboundBytes);
        Conclusion.Hull.LiveMetadataBytes += liveOutboundBytes;
        Conclusion.Hull.MergeRedundantMetadataBytes += OutboundMetadataBytes - liveOutboundBytes;
    }

    void TLogoBlobSpaceMerger::Finish() {
        Y_DEBUG_ABORT_UNLESS(Key && !Finished);
        IndexMerger.Finish();
        const NGc::TKeepStatus keep = Barriers
            ? Barriers->Keep(
                *Key,
                IndexMerger.GetMemRec(),
                {},
                AllowKeepFlags,
                AllowGarbageCollection)
            : NGc::TKeepStatus(true);

        TargetingHugeBlob = HugeBlobCtx && MinHugeBlobInBytes &&
            HugeBlobCtx->IsHugeBlob(GType, Key->LogoBlobID(), MinHugeBlobInBytes);
        const bool hasMemData = std::ranges::any_of(HasMemSource, [](bool value) {
            return value;
        });
        const bool hasInplacedData = std::ranges::any_of(InplacedWinners, [](const TInplacedWinner& winner) {
            return winner.Present && winner.PayloadBytes;
        });
        const bool hasHugeData = std::ranges::any_of(HugeWinners, [](const TWinner& winner) {
            return winner.Present;
        });
        ProducingHugeBlob = TargetingHugeBlob || (!hasMemData && !hasInplacedData && hasHugeData);

        ClassifyHugeRefs(keep.KeepData);
        ClassifyInplacedData(keep.KeepData);
        ClassifyIndexMetadata(keep.KeepIndex, keep.KeepData);
        Conclusion.HugeRefCountSeen = HugeRefCountSeen;
        Conclusion.HugeReferencedBytesSeen = HugeReferencedBytesSeen;
        Conclusion.HugeRefsOverflow |= HugeRefsOverflow;
        Finished = true;
    }

    const TLogoBlobKeyEstimate& TLogoBlobSpaceMerger::GetConclusion() const {
        Y_DEBUG_ABORT_UNLESS(Finished);
        return Conclusion;
    }

} // namespace NKikimr::NVDiskSpaceReport
