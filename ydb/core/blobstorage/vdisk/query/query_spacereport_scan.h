#pragma once

#include "defs.h"

#include <ydb/core/blobstorage/groupinfo/blobstorage_groupinfo.h>
#include <ydb/core/blobstorage/vdisk/hulldb/barriers/barriers_essence.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_barrier.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_block.h>
#include <ydb/core/blobstorage/vdisk/hulldb/base/hullbase_logoblob.h>
#include <ydb/core/blobstorage/vdisk/hulldb/generic/blobstorage_hullrecmerger.h>

#include <array>
#include <optional>
#include <vector>

namespace NKikimr {
    class THugeBlobCtx;
}

namespace NKikimr::NVDiskSpaceReport {

    // Protobuf-independent representation of the report breakdown.
    struct TSpaceBreakdown {
        ui64 UsefulBlobDataBytes = 0;
        ui64 LiveMetadataBytes = 0;
        ui64 LiveAuxiliaryDataBytes = 0;
        ui64 GcDeadBlobDataBytes = 0;
        ui64 GcDeadMetadataBytes = 0;
        ui64 MergeRedundantBlobDataBytes = 0;
        ui64 MergeRedundantMetadataBytes = 0;
        ui64 WritePaddingBytes = 0;
        ui64 SlotInternalFragmentationBytes = 0;
        ui64 FreeSlotBytes = 0;
        ui64 ChunkTailBytes = 0;
        ui64 FreeChunkReserveBytes = 0;
        ui64 LockedOrQuarantinedBytes = 0;
        ui64 UnclassifiedBytes = 0;

        TSpaceBreakdown& operator+=(const TSpaceBreakdown& other);

        ui64 TotalBytes() const;
    };

    enum class EHugeBlobClassification : ui8 {
        Useful,
        GcDead,
        MergeRedundant,
    };

    // Physical SST ownership is sampled while the semantic scan visits the
    // SST's last key. This makes every SST contribute exactly once without a
    // separate unbounded walk or any snapshot-dependent state between quanta.
    struct TPhysicalSstEstimate {
        ui64 SstCount = 0;
        ui64 ChunkCount = 0;
        ui64 StructuralMetadataBytes = 0;

        template <class TKey, class TMemRec>
        void AddIfLastKey(const TKey& key, const TLevelSegment<TKey, TMemRec>* sst) {
            if (!sst) {
                return;
            }

            if (!(key == sst->LastKey())) {
                return;
            }

            ++SstCount;
            ChunkCount += sst->AllChunks.empty()
                ? (sst->Info.Chunks ? sst->Info.Chunks : 1)
                : sst->AllChunks.size();
            StructuralMetadataBytes += sizeof(TIdxDiskPlaceHolder);
            if (sst->Info.IndexParts > 1) {
                StructuralMetadataBytes += (sst->Info.IndexParts - 1) * sizeof(TIdxDiskLinker);
            }
        }
    };

    struct TClassifiedHugeBlob {
        TDiskPart Part;
        ui32 PayloadBytes = 0;
        EHugeBlobClassification Classification = EHugeBlobClassification::MergeRedundant;
    };

    void AddClassifiedHugeBlob(TSpaceBreakdown& breakdown, const TClassifiedHugeBlob& blob);

    struct TLogoBlobKeyEstimate {
        // Hull index and in-place bytes. Huge blob extents are returned
        // separately so the owner of Huge allocator statistics can aggregate
        // them by size class without retaining per-key state.
        TSpaceBreakdown Hull;
        std::vector<TClassifiedHugeBlob> HugeBlobs;
        ui64 PhysicalSstRecords = 0;
        ui64 PhysicalMetadataBytes = 0;
        ui64 PhysicalInplacedBytes = 0;
        TPhysicalSstEstimate PhysicalSsts;
        ui64 HugeRefCountSeen = 0;
        ui64 HugeReferencedBytesSeen = 0;
        bool HugeRefsOverflow = false;
    };

    template <class TKey, class TMemRec>
    struct TMetadataKeyEstimate;

    // Cumulative state intentionally contains only scalars. A scan actor may
    // keep it across snapshot quanta while each key conclusion (and its
    // bounded Huge vector) is consumed and released inside one quantum.
    struct TSemanticScanCounters {
        TSpaceBreakdown LogoBlobs;
        TSpaceBreakdown Blocks;
        TSpaceBreakdown Barriers;
        TSpaceBreakdown HugeBlobs;
        ui64 HugeOverflowKeys = 0;
        ui64 HugeOverflowRefCount = 0;
        ui64 HugeOverflowReferencedBytes = 0;

        void AddLogoBlobKey(const TLogoBlobKeyEstimate& estimate);

        template <class TKey, class TMemRec>
        void AddBlocksKey(const TMetadataKeyEstimate<TKey, TMemRec>& estimate);

        template <class TKey, class TMemRec>
        void AddBarriersKey(const TMetadataKeyEstimate<TKey, TMemRec>& estimate);
    };

    class TLogoBlobSpaceMerger {
    public:
        TLogoBlobSpaceMerger(
            TBlobStorageGroupType gtype,
            const NGcOpt::TBarriersEssence* barriers,
            bool allowKeepFlags,
            bool allowGarbageCollection,
            size_t maxHugeRefsPerKey,
            const THugeBlobCtx* hugeBlobCtx = nullptr,
            ui32 minHugeBlobInBytes = 0);

        void Clear();

        void AddFromFresh(
            const TMemRecLogoBlob& memRec,
            const TRope*,
            const TKeyLogoBlob& key,
            ui64 lsn);

        void AddFromSegment(
            const TMemRecLogoBlob& memRec,
            const TDiskPart* outbound,
            const TKeyLogoBlob& key,
            ui64 circaLsn,
            const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>* sst);

        static constexpr bool HaveToMergeData() {
            return false;
        }

        void Finish();

        const TLogoBlobKeyEstimate& GetConclusion() const;

    private:
        struct TRawHugeRef {
            TDiskPart Part;
            ui64 CircaLsn = 0;
            ui8 PartIdx = 0;
        };

        struct TWinner {
            TDiskPart Part;
            ui64 CircaLsn = 0;
            bool Present = false;
        };

        struct TInplacedWinner {
            ui64 RecordId = 0;
            TDiskPart Location;
            ui64 CircaLsn = 0;
            ui32 PayloadBytes = 0;
            ui32 RecordMetadataBytes = 0;
            bool Present = false;
        };

        void CheckKey(const TKeyLogoBlob& key);
        void MergeIndexFromFresh(const TMemRecLogoBlob& memRec, const TKeyLogoBlob& key, ui64 lsn);
        void MergeIndexFromSegment(
            const TMemRecLogoBlob& memRec,
            const TDiskPart* outbound,
            const TKeyLogoBlob& key,
            ui64 circaLsn,
            const TLevelSegment<TKeyLogoBlob, TMemRecLogoBlob>* sst);
        void RecordMemParts(const TMemRecLogoBlob& memRec);
        void RecordInplacedData(
            const TMemRecLogoBlob& memRec,
            const TDiskPart& location,
            ui64 circaLsn);
        void RecordHugeRefs(
            const TMemRecLogoBlob& memRec,
            const TDiskPart* outbound,
            ui64 circaLsn);
        void ClassifyHugeRefs(bool keepData);
        void ClassifyInplacedData(bool keepData);
        void ClassifyIndexMetadata(bool keepIndex, bool keepData);

    private:
        const TBlobStorageGroupType GType;
        const NGcOpt::TBarriersEssence* const Barriers;
        const bool AllowKeepFlags;
        const bool AllowGarbageCollection;
        const size_t MaxHugeRefsPerKey;
        const THugeBlobCtx* const HugeBlobCtx;
        const ui32 MinHugeBlobInBytes;

        TIndexRecordMerger<TKeyLogoBlob, TMemRecLogoBlob> IndexMerger;
        std::optional<TKeyLogoBlob> Key;
        std::array<bool, MaxTotalPartCount> HasMemSource = {};
        std::array<TInplacedWinner, MaxTotalPartCount> InplacedWinners = {};
        std::array<TWinner, MaxTotalPartCount> HugeWinners = {};
        std::vector<TRawHugeRef> HugeRefs;
        ui64 BaseMetadataBytes = 0;
        ui64 OutboundMetadataBytes = 0;
        ui64 InplacedPayloadBytes = 0;
        ui64 InplacedMetadataBytes = 0;
        ui64 InplacedUnclassifiedBytes = 0;
        ui64 NextInplacedRecordId = 1;
        ui64 HugeRefCountSeen = 0;
        ui64 HugeReferencedBytesSeen = 0;
        bool HugeRefsOverflow = false;
        bool TargetingHugeBlob = false;
        bool ProducingHugeBlob = false;
        bool Finished = false;
        TLogoBlobKeyEstimate Conclusion;
    };

    template <class TKey, class TMemRec>
    struct TMetadataKeyEstimate {
        TSpaceBreakdown Breakdown;
        ui64 PhysicalSstRecords = 0;
        TPhysicalSstEstimate PhysicalSsts;
    };

    // Blocks and Barriers contain no blob payload. A fully merged retained key
    // needs one index record; additional physical SST records are merge
    // redundancy. Fresh records influence the merged value but do not consume
    // chunk space and therefore are not included in this estimate.
    template <class TKey, class TMemRec>
    class TMetadataSpaceMerger {
    public:
        using TConclusion = TMetadataKeyEstimate<TKey, TMemRec>;

        TMetadataSpaceMerger(
                TBlobStorageGroupType gtype,
                const NGcOpt::TBarriersEssence* barriers,
                bool allowKeepFlags,
                bool allowGarbageCollection)
            : Barriers(barriers)
            , AllowKeepFlags(allowKeepFlags)
            , AllowGarbageCollection(allowGarbageCollection)
            , IndexMerger(gtype)
        {
            Clear();
        }

        void Clear() {
            IndexMerger.Clear();
            Key.reset();
            PhysicalSstRecords = 0;
            PhysicalSsts = {};
            Finished = false;
            Conclusion = {};
        }

        void AddFromFresh(const TMemRec& memRec, const TRope* data, const TKey& key, ui64 lsn) {
            CheckKey(key);
            IndexMerger.AddFromFresh(memRec, data, key, lsn);
        }

        void AddFromSegment(
                const TMemRec& memRec,
                const TDiskPart* outbound,
                const TKey& key,
                ui64 circaLsn,
                const TLevelSegment<TKey, TMemRec>* sst)
        {
            CheckKey(key);
            IndexMerger.AddFromSegment(memRec, outbound, key, circaLsn, sst);
            ++PhysicalSstRecords;
            PhysicalSsts.template AddIfLastKey<TKey, TMemRec>(key, sst);
        }

        static constexpr bool HaveToMergeData() {
            return false;
        }

        void Finish() {
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

            Conclusion.PhysicalSstRecords = PhysicalSstRecords;
            Conclusion.PhysicalSsts = PhysicalSsts;
            const ui64 recordBytes = sizeof(TKey) + sizeof(TMemRec);
            const ui64 totalBytes = PhysicalSstRecords * recordBytes;
            if (!keep.KeepIndex) {
                Conclusion.Breakdown.GcDeadMetadataBytes = totalBytes;
            } else {
                Conclusion.Breakdown.LiveMetadataBytes = Min(totalBytes, recordBytes);
                Conclusion.Breakdown.MergeRedundantMetadataBytes =
                    totalBytes - Conclusion.Breakdown.LiveMetadataBytes;
            }
            Finished = true;
        }

        const TConclusion& GetConclusion() const {
            Y_DEBUG_ABORT_UNLESS(Finished);
            return Conclusion;
        }

    private:
        void CheckKey(const TKey& key) {
            Y_DEBUG_ABORT_UNLESS(!Key || *Key == key);
            Key = key;
        }

    private:
        const NGcOpt::TBarriersEssence* const Barriers;
        const bool AllowKeepFlags;
        const bool AllowGarbageCollection;
        TIndexRecordMerger<TKey, TMemRec> IndexMerger;
        std::optional<TKey> Key;
        ui64 PhysicalSstRecords = 0;
        TPhysicalSstEstimate PhysicalSsts;
        bool Finished = false;
        TConclusion Conclusion;
    };

    using TBlocksSpaceMerger = TMetadataSpaceMerger<TKeyBlock, TMemRecBlock>;
    using TBarriersSpaceMerger = TMetadataSpaceMerger<TKeyBarrier, TMemRecBarrier>;

    template <class TKey, class TMemRec>
    void TSemanticScanCounters::AddBlocksKey(const TMetadataKeyEstimate<TKey, TMemRec>& estimate) {
        Blocks += estimate.Breakdown;
    }

    template <class TKey, class TMemRec>
    void TSemanticScanCounters::AddBarriersKey(const TMetadataKeyEstimate<TKey, TMemRec>& estimate) {
        Barriers += estimate.Breakdown;
    }

} // namespace NKikimr::NVDiskSpaceReport
