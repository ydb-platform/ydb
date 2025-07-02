#pragma once

#include "defs.h"
#include "blobstorage_hullstorageratio.h"
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>
#include <ydb/core/blobstorage/vdisk/common/disk_part.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_context.h>
#include <ydb/core/blobstorage/vdisk/common/vdisk_mongroups.h>
#include <util/generic/vector.h>
#include <util/generic/buffer.h>
#include <util/stream/output.h>
#include <util/string/printf.h>
#include <util/ysaveload.h>

// FIXME: only for TIngressCache (put it to vdisk/common)
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/protos/blobstorage_vdisk_internal.pb.h>

namespace NKikimr {

    template <class TKey>
    TLogSignature PDiskSignatureForHullDbKey();

    class TVDiskContext;
    using TVDiskContextPtr = TIntrusivePtr<TVDiskContext>;

    ///////////////////////////////////////////////////////////////////////////////////////
    // TDiskDataExtractor
    ///////////////////////////////////////////////////////////////////////////////////////
    struct TDiskDataExtractor {
        TBlobType::EType BlobType = TBlobType::DiskBlob;
        const TDiskPart *Begin = nullptr;
        const TDiskPart *End = nullptr;
        TDiskPart Store;

        // set up one part (DiskBlob or HugeBlob)
        void Set(TBlobType::EType t, const TDiskPart &part) {
            BlobType = t;
            Store = part;
            Begin = &Store;
            End = Begin + 1;
        }

        // set up many parts (ManyHugeBlobs)
        void Set(TBlobType::EType t, const TDiskPart *begin, const TDiskPart *end) {
            BlobType = t;
            Begin = begin;
            End = end;
            Store.Clear();
        }

        void Clear() {
            BlobType = TBlobType::DiskBlob;
            Begin = End = nullptr;
            Store.Clear();
        }

        const TDiskPart &SwearOne() const {
            Y_DEBUG_ABORT_UNLESS(Begin + 1 == End);
            return *Begin;
        }

        TString ToString() const {
            if (Begin == End)
                return "empty";
            else {
                TStringStream str;
                bool first = true;
                for (const TDiskPart *it = Begin; it != End; ++it) {
                    if (first)
                        first = false;
                    else
                        str << " ";
                    str << it->ToString();
                }
                return str.Str();
            }
        }


        ui32 GetInplacedDataSize() const {
            if (BlobType == TBlobType::DiskBlob && Begin) {
                return Begin->Size;
            } else {
                return 0;
            }
        }

        ui32 GetHugeDataSize() const {
            if (BlobType == TBlobType::HugeBlob || BlobType == TBlobType::ManyHugeBlobs) {
                ui32 size = 0;
                for (const TDiskPart *cur = Begin; cur != End; cur++) {
                    size += cur->Size;
                }
                if (End - Begin > 1) {
                    // add outbound expenses
                    size += (End - Begin) * sizeof(TDiskPart);
                }
                return size;
            } else {
                return 0;
            }
        }
    };

    ///////////////////////////////////////////////////////////////////////////////////////
    // TMemPart
    ///////////////////////////////////////////////////////////////////////////////////////
    struct TMemPart {
        ui64 BufferId;
        ui32 Size;

        TMemPart(ui64 bufferId, ui32 size)
            : BufferId(bufferId)
            , Size(size)
        {}
    };

    ///////////////////////////////////////////////////////////////////////////////////////
    // THullCtx
    ///////////////////////////////////////////////////////////////////////////////////////
    struct THullCtx : public TThrRefBase {
        TVDiskContextPtr VCtx;
        const TIntrusivePtr<TVDiskConfig> VCfg;
        const TIntrusivePtr<TIngressCache> IngressCache;
        const ui32 ChunkSize;
        const ui32 CompWorthReadSize;
        const bool FreshCompaction;
        const bool GCOnlySynced;
        const bool AllowKeepFlags;
        const bool BarrierValidation;
        const ui32 HullSstSizeInChunksFresh;
        const ui32 HullSstSizeInChunksLevel;
        const double HullCompFreeSpaceThreshold;
        const double HullCompReadBatchEfficiencyThreshold;
        const TDuration HullCompStorageRatioCalcPeriod;
        const TDuration HullCompStorageRatioMaxCalcDuration;
        const bool AddHeader;

        NMonGroup::TLsmHullGroup LsmHullGroup;
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
                double hullCompFreeSpaceThreshold,
                double hullCompReadBatchEfficiencyThreshold,
                TDuration hullCompStorageRatioCalcPeriod,
                TDuration hullCompStorageRatioMaxCalcDuration,
                bool addHeader);

        void UpdateSpaceCounters(const NHullComp::TSstRatio& prev, const NHullComp::TSstRatio& current);
    };

    using THullCtxPtr = TIntrusivePtr<THullCtx>;

    ////////////////////////////////////////////////////////////////////////////
    // TPutRecoveryLogRecOpt
    ////////////////////////////////////////////////////////////////////////////
    struct TPutRecoveryLogRecOpt {
        TLogoBlobID Id;
        TString Data;

        static TString Serialize(const TBlobStorageGroupType &gtype, const TLogoBlobID &id, const TRope &rope);
        // Will serialize inplace if container has enough headroom and right (single) underlying type
        static TRcBuf SerializeZeroCopy(const TBlobStorageGroupType &gtype, const TLogoBlobID &id, TRope &&rope);
        // Will serialize inplace if container has enough headroom
        static TRcBuf SerializeZeroCopy(const TBlobStorageGroupType &gtype, const TLogoBlobID &id, TRcBuf &&data);
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

