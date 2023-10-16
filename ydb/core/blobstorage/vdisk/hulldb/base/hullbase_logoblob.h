#pragma once

#include "defs.h"
#include "blobstorage_hulldefs.h"

#include <ydb/core/blobstorage/vdisk/common/vdisk_dbtype.h>
#include <ydb/core/blobstorage/vdisk/ingress/blobstorage_ingress.h>
#include <ydb/core/protos/blobstorage.pb.h>

namespace NKikimr {

    // Data types for logoblob database

    /////////////////////////////////////////////////////////////////////////
    // TKeyLogoBlob
    /////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
    struct TKeyLogoBlob {
        ui64 Raw[3];

        static const char *Name() {
            return "LogoBlobs";
        }

        TKeyLogoBlob() {
            Raw[0] = Raw[1] = Raw[2] = 0;
        }

        TKeyLogoBlob(const TLogoBlobID &id) {
            const ui64 *raw = id.GetRaw();
            Raw[0] = raw[0];
            Raw[1] = raw[1];
            Raw[2] = raw[2];
        }

        TLogoBlobID LogoBlobID() const {
            return TLogoBlobID(Raw);
        }

        TString ToString() const {
            return LogoBlobID().ToString();
        }

        static TKeyLogoBlob First() {
            return TKeyLogoBlob();
        }

        bool IsSameAs(const TKeyLogoBlob& other) const {
            TLogoBlobID x(LogoBlobID());
            TLogoBlobID y(other.LogoBlobID());
            return x.TabletID()   == y.TabletID()
                && x.Channel()    == y.Channel()
                && x.Generation() == y.Generation()
                && x.Step()       == y.Step()
                && x.Cookie()     == y.Cookie();
        }
    };
#pragma pack(pop)

    inline bool operator <(const TKeyLogoBlob &x, const TKeyLogoBlob &y) {
        return x.LogoBlobID() < y.LogoBlobID();
    }

    inline bool operator >(const TKeyLogoBlob &x, const TKeyLogoBlob &y) {
        return x.LogoBlobID() > y.LogoBlobID();
    }

    inline bool operator ==(const TKeyLogoBlob &x, const TKeyLogoBlob &y) {
        return x.LogoBlobID() == y.LogoBlobID();
    }

    inline bool operator !=(const TKeyLogoBlob &x, const TKeyLogoBlob &y) {
        return x.LogoBlobID() != y.LogoBlobID();
    }

    inline bool operator <=(const TKeyLogoBlob &x, const TKeyLogoBlob &y) {
        return x.LogoBlobID() <= y.LogoBlobID();
    }

    inline bool operator >=(const TKeyLogoBlob &x, const TKeyLogoBlob &y) {
        return x.LogoBlobID() >= y.LogoBlobID();
    }

    /////////////////////////////////////////////////////////////////////////
    // PDiskSignatureForHullDbKey
    /////////////////////////////////////////////////////////////////////////
    template <>
    inline TLogSignature PDiskSignatureForHullDbKey<TKeyLogoBlob>() {
        return TLogSignature::SignatureHullLogoBlobsDB;
    }

    /////////////////////////////////////////////////////////////////////////
    // TMemRecLogoBlob
    /////////////////////////////////////////////////////////////////////////
#pragma pack(push, 4)
    class TMemRecLogoBlob {
        TIngress Ingress;
        ui32 Type : 2;
        ui32 Id : 30;
        ui32 Offset;
        ui32 Size;

        void ClearData() {
            Type = TBlobType::DiskBlob;
            Id = 0;
            Offset = 0;
            Size = 0;
        }

    public:
        TMemRecLogoBlob()
            : Ingress()
        {
            ClearData();
        }

        TMemRecLogoBlob(const TIngress &ingress)
            : Ingress(ingress)
        {
            ClearData();
        }

        void Merge(const TMemRecLogoBlob& rec, const TKeyLogoBlob& /*key*/) {
            TIngress::Merge(Ingress, rec.Ingress);
        }

        ui32 DataSize() const {
            return Size;
        }

        bool HasData() const {
            TBlobType::EType t = GetType();
            return t == TBlobType::HugeBlob || t == TBlobType::ManyHugeBlobs || Size;
        }

        void SetDiskBlob(const TDiskPart &dataAddr) {
            Type = TBlobType::DiskBlob;
            Id = dataAddr.ChunkIdx;
            Offset = dataAddr.Offset;
            Size = dataAddr.Size;
        }

        void SetHugeBlob(const TDiskPart &dataAddr) {
            Type = TBlobType::HugeBlob;
            Id = dataAddr.ChunkIdx;
            Offset = dataAddr.Offset;
            Size = dataAddr.Size;
        }

        void SetManyHugeBlobs(ui32 idx, ui32 num, ui32 size) {
            Type = TBlobType::ManyHugeBlobs;
            Id = idx;
            Offset = num;
            Size = size;
        }

        void SetMemBlob(ui64 id, ui32 size) {
            Type = TBlobType::MemBlob;
            Y_ABORT_UNLESS(id < (ui64(1) << 62));
            Id = id >> 32;
            Offset = id;
            Size = size;
        }

        void SetNoBlob() {
            ClearData();
        }

        void SetType(TBlobType::EType t) {
            Y_DEBUG_ABORT_UNLESS(t == TBlobType::DiskBlob || t == TBlobType::HugeBlob || t == TBlobType::ManyHugeBlobs);
            Type = t;
        }

        TDiskDataExtractor *GetDiskData(TDiskDataExtractor *extr, const TDiskPart *outbound) const {
            TBlobType::EType t = GetType();
            if (t == TBlobType::DiskBlob || t == TBlobType::HugeBlob) {
                extr->Set(t, TDiskPart(Id, Offset, Size));
            } else {
                Y_ABORT_UNLESS(t == TBlobType::ManyHugeBlobs && outbound);
                const TDiskPart *begin = &(outbound[Id]);
                extr->Set(TBlobType::ManyHugeBlobs, begin, begin + Offset);
            }
            return extr;
        }

        TMemPart GetMemData() const {
            Y_DEBUG_ABORT_UNLESS(GetType() == TBlobType::MemBlob);
            return TMemPart(ui64(Id) << 32 | Offset, Size);
        }

        const TIngress &GetIngress() const {
            return Ingress;
        }

        TBlobType::EType GetType() const {
            return TBlobType::EType(Type);
        }

        NMatrix::TVectorType GetLocalParts(const TBlobStorageGroupType &gtype) const {
            return Ingress.LocalParts(gtype);
        }

        void ClearLocalParts(const TBlobStorageGroupType &gtype) {
            Ingress = Ingress.CopyWithoutLocal(gtype);
        }

        TString ToString(const TIngressCache *cache, const TDiskPart *outbound) const {
            Y_UNUSED(cache);
            TStringStream str;
            TBlobType::EType t = GetType();
            str << "{" << TBlobType::TypeToStr(t) << " " << CollectMode2String(Ingress.GetCollectMode(
                TIngress::IngressMode(cache->Topology->GType)));
            if (t == TBlobType::MemBlob) {
                // nothing
            } else {
                TDiskDataExtractor extr;
                GetDiskData(&extr, outbound);
                str << " " << extr.ToString();
            }
            str << "}";
            return str.Str();
        }
    };
#pragma pack(pop)

    template <>
    inline EHullDbType TKeyToEHullDbType<TKeyLogoBlob>() {
        return EHullDbType::LogoBlobs;
    }

} // NKikimr

Y_DECLARE_PODTYPE(NKikimr::TKeyLogoBlob);
Y_DECLARE_PODTYPE(NKikimr::TMemRecLogoBlob);
