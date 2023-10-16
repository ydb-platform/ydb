#pragma once

#include "defs.h"
#include "blobstorage_hulldefs.h"
#include <ydb/core/blobstorage/vdisk/common/vdisk_dbtype.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>

#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    // Data types for block database

    /////////////////////////////////////////////////////////////////////////
    // TKeyBlock
    /////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
    struct TKeyBlock {
        ui64 TabletId;

        static const char *Name() {
            return "Blocks";
        }

        TKeyBlock()
            : TabletId(0)
        {}

        TKeyBlock(ui64 tabletId)
            : TabletId(tabletId)
        {}

        TString ToString() const {
            return Sprintf("[%16" PRIu64 "]", TabletId);
        }

        TLogoBlobID LogoBlobID() const {
            return TLogoBlobID();
        }

        static TKeyBlock First() {
            return TKeyBlock();
        }

        bool IsSameAs(const TKeyBlock& other) const {
            return TabletId == other.TabletId;
        }
    };
#pragma pack(pop)

    inline bool operator <(const TKeyBlock &x, const TKeyBlock &y) {
        return x.TabletId < y.TabletId;
    }

    inline bool operator >(const TKeyBlock &x, const TKeyBlock &y) {
        return x.TabletId > y.TabletId;
    }

    inline bool operator ==(const TKeyBlock &x, const TKeyBlock &y) {
        return x.TabletId == y.TabletId;
    }

    inline bool operator <=(const TKeyBlock &x, const TKeyBlock &y) {
        return x.TabletId <= y.TabletId;
    }

    inline bool operator >=(const TKeyBlock &x, const TKeyBlock &y) {
        return x.TabletId >= y.TabletId;
    }

    /////////////////////////////////////////////////////////////////////////
    // PDiskSignatureForHullDbKey
    /////////////////////////////////////////////////////////////////////////
    template <>
    inline TLogSignature PDiskSignatureForHullDbKey<TKeyBlock>() {
        return TLogSignature::SignatureHullBlocksDB;
    }

    /////////////////////////////////////////////////////////////////////////
    // TMemRecBlock
    /////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
    struct TMemRecBlock {
        ui32 BlockedGeneration;

        TMemRecBlock()
            : BlockedGeneration(ui32(-1))
        {}

        TMemRecBlock(ui32 blockGen)
            : BlockedGeneration(blockGen)
        {}

        void Merge(const TMemRecBlock& rec, const TKeyBlock& /*key*/) {
            BlockedGeneration = Max(BlockedGeneration, rec.BlockedGeneration);
        }

        ui32 DataSize() const {
            return 0;
        }

        bool HasData() const {
            return false;
        }

        void SetDiskBlob(const TDiskPart &dataAddr) {
            Y_UNUSED(dataAddr);
            // nothing to do
        }

        void SetHugeBlob(const TDiskPart &) {
            Y_ABORT("Must not be called");
        }

        void SetManyHugeBlobs(ui32, ui32, ui32) {
            Y_ABORT("Must not be called");
        }

        void SetMemBlob(ui64, ui32) {
            Y_ABORT("Must not be called");
        }

        void SetNoBlob() {
        }

        void SetType(TBlobType::EType t) {
            Y_DEBUG_ABORT_UNLESS(t == TBlobType::DiskBlob);
            Y_UNUSED(t);
        }

        TDiskDataExtractor *GetDiskData(TDiskDataExtractor *extr, const TDiskPart *) const {
            extr->Clear();
            return extr;
        }

        TMemPart GetMemData() const {
            Y_ABORT("Must not be called");
        }

        NMatrix::TVectorType GetLocalParts(TBlobStorageGroupType) const {
            return NMatrix::TVectorType();
        }

        void ClearLocalParts(TBlobStorageGroupType)
        {}

        TBlobType::EType GetType() const {
            return TBlobType::DiskBlob;
        }

        TString ToString(const TIngressCache *cache, const TDiskPart *outbound = nullptr) const {
            Y_UNUSED(cache);
            Y_UNUSED(outbound);
            return Sprintf("{BlockedGen: %" PRIu32 "}", BlockedGeneration);
        }
    };
#pragma pack(pop)

    inline bool operator==(const TMemRecBlock &x, const TMemRecBlock &y) {
        return x.BlockedGeneration == y.BlockedGeneration;
    }

    template <>
    inline EHullDbType TKeyToEHullDbType<TKeyBlock>() {
        return EHullDbType::Blocks;
    }

} // NKikimr

Y_DECLARE_PODTYPE(NKikimr::TKeyBlock);
Y_DECLARE_PODTYPE(NKikimr::TMemRecBlock);
