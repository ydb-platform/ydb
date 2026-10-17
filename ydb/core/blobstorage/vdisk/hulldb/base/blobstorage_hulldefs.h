#pragma once

#include "defs.h"
#include "blobstorage_hullstorageratio.h"
#include "hullbase_data_parts.h"
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_dbtype.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <util/generic/vector.h>
#include <util/generic/buffer.h>
#include <util/stream/output.h>
#include <util/string/printf.h>
#include <util/ysaveload.h>
#include <array>

// FIXME: only for TIngressCache (put it to vdisk/common)
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

    class TVDiskContext;
    using TVDiskContextPtr = TIntrusivePtr<TVDiskContext>;

    ///////////////////////////////////////////////////////////////////////////////////////
    // THullCtx
    ///////////////////////////////////////////////////////////////////////////////////////
    struct THullCtx : public TThrRefBase {
        TVDiskContextPtr VCtx;
        const TIntrusivePtr<TVDiskConfig> VCfg;
        const TIntrusivePtr<TIngressCache> IngressCache;
        const ui32 ChunkSize;
        // Granularity PDisk appends in; the SST writer pads to it (see TFreshOutputGeometry).
        const ui32 AppendBlockSize;
        const ui32 CompWorthReadSize;
        const bool FreshCompaction;
        const bool GCOnlySynced;
        const bool AllowKeepFlags;
        const bool BarrierValidation;
        const ui32 HullSstSizeInChunksFresh;
        const ui32 HullSstSizeInChunksLevel;
        const double HullCompReadBatchEfficiencyThreshold;
        const TDuration HullCompStorageRatioCalcPeriod;
        const TDuration HullCompStorageRatioMaxCalcDuration;
        // Reserve chunks for compacting Fresh before accepting the writes that fill it
        // (EnableVDiskFreshSpaceProjection). See TFreshData and TFreshAdmissionGate.
        const bool FreshChunkReservation;
        // Max<ui32>() generation block alone lets us drop all data of the tablet, see IsCompleteTabletDeletionBlock
        const bool CollectByCompleteDeletionBlock;

        ui32 HullCompLevel0MaxSstsAtOnce;
        ui32 HullCompSortedPartsNum;

        NMonGroup::TCompactionStrategyGroup CompactionStrategyGroup;
        NMonGroup::TLsmHullGroup LsmHullGroup;
        std::array<NMonGroup::TLsmCompactionRankGroup, ui32(EHullDbType::Max)> LsmCompactionRankGroups;
        NMonGroup::TLsmHullSpaceGroup LsmHullSpaceGroup;

        THullCtx(
                TVDiskContextPtr vctx,
                const TIntrusivePtr<TVDiskConfig> vcfg,
                ui32 chunkSize,
                ui32 compWorthReadSize,
                bool freshCompaction,
                bool gcOnlySynced,
                bool allowKeepFlags,
                bool barrierValidation,
                ui32 hullSstSizeInChunksFresh,
                ui32 hullSstSizeInChunksLevel,
                double hullCompReadBatchEfficiencyThreshold,
                TDuration hullCompStorageRatioCalcPeriod,
                TDuration hullCompStorageRatioMaxCalcDuration,
                ui32 hullCompLevel0MaxSstsAtOnce,
                ui32 hullCompSortedPartsNum,
                bool freshChunkReservation = false,
                ui32 appendBlockSize = 4096,
                bool collectByCompleteDeletionBlock = false
        );

        void UpdateSpaceCounters(const NHullComp::TSstRatio& prev, const NHullComp::TSstRatio& current);
    };

    using THullCtxPtr = TIntrusivePtr<THullCtx>;

    ////////////////////////////////////////////////////////////////////////////
    // TPutRecoveryLogRecOpt
    ////////////////////////////////////////////////////////////////////////////
    struct TPutRecoveryLogRecOpt {
        TLogoBlobID Id;
        TString Data;
        bool IssueKeepFlag;

        static TString Serialize(const TBlobStorageGroupType &gtype, const TLogoBlobID &id, const TRope &rope,
            bool issueKeepFlag);
        // Will serialize inplace if container has enough headroom and right (single) underlying type
        static TRcBuf SerializeZeroCopy(const TBlobStorageGroupType &gtype, const TLogoBlobID &id, TRope &&rope,
            bool issueKeepFlag);
        // Will serialize inplace if container has enough headroom
        static TRcBuf SerializeZeroCopy(const TBlobStorageGroupType &gtype, const TLogoBlobID &id, TRcBuf &&data,
            bool issueKeepFlag);
        bool ParseFromString(const TBlobStorageGroupType &gtype, const TString &data);
        bool ParseFromArray(const TBlobStorageGroupType &gtype, const char* data, size_t size);
        TString ToString() const;
        void Output(IOutputStream &str) const;
    };

    // prepared data to insert to Hull Database
    struct THullDbInsert {
        TLogoBlobID Id;
        TIngress Ingress;
    };

} // NKikimr
