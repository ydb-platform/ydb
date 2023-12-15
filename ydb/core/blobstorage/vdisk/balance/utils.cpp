#include "utils.h"


namespace NKikimr {

    TVector<ui8> GetRequiredParts(NMatrix::TVectorType vec, NMatrix::TVectorType mask) {
        auto parts = vec & mask;
        TVector<ui8> res(Reserve(parts.CountBits()));
        for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
            res.push_back(i + 1);
        }
        return res;
    }

    TVector<ui8> PartsToSendOnMain(
        const TBlobStorageGroupInfo::TTopology& top,
        const TVDiskIdShort &vdisk,
        const TLogoBlobID &key,
        const TIngress& ingress
    ) {
        auto [moveMask, _] = ingress.HandoffParts(&top, vdisk, key);
        return GetRequiredParts(ingress.LocalParts(top.GType), moveMask);
    }

    TVector<ui8> PartsToDelete(
        const TBlobStorageGroupInfo::TTopology& top,
        const TVDiskIdShort &vdisk,
        const TLogoBlobID &key,
        const TIngress& ingress
    ) {
        auto [_, delMask] = ingress.HandoffParts(&top, vdisk, key);
        return GetRequiredParts(ingress.LocalParts(top.GType), delMask);
    }

    TVDiskID GetVDiskId(const TBlobStorageGroupInfo& gInfo, const TLogoBlobID& key) {
        TBlobStorageGroupInfo::TOrderNums orderNums;
        TBlobStorageGroupInfo::TVDiskIds vdisks;
        gInfo.GetTopology().PickSubgroup(key.Hash(), orderNums);
        for (const auto &x : orderNums) {
            vdisks.push_back(gInfo.GetVDiskId(x));
        }
        return vdisks[key.PartId() - 1];
    }



    ///////////////////////////// TPartsCollectorMerger /////////////////////////////

    TPartsCollectorMerger::TPartsCollectorMerger(const TBlobStorageGroupType gType)
        : GType(gType)
    {
        Parts.resize(GType.TotalPartCount());
    }

    void TPartsCollectorMerger::AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& /*key*/, ui64 /*lsn*/) {
        Ingress.Merge(memRec.GetIngress());

        TDiskDataExtractor extr;
        memRec.GetDiskData(&extr, outbound);
        const NMatrix::TVectorType local = memRec.GetLocalParts(GType);
        if (local.Empty()) {
            return;
        }

        ui8 partIdx = local.FirstPosition();
        for (const TDiskPart *part = extr.Begin; part != extr.End; ++part, partIdx = local.NextPosition(partIdx)) {
            if (part->ChunkIdx && part->Size) {
                Parts[partIdx] = *part;
            }
        }
    }

    void TPartsCollectorMerger::AddFromFresh(const TMemRecLogoBlob& memRec, const TRope* data, const TKeyLogoBlob& key, ui64 /*lsn*/) {
        if (!memRec.HasData()) {
            return;
        }
        Ingress.Merge(memRec.GetIngress());

        const NMatrix::TVectorType local = memRec.GetLocalParts(GType);

        if (data) {
            auto diskBlob = TDiskBlob(data, local, GType, key.LogoBlobID());
            TDiskBlob::TPartIterator it(&diskBlob, local.FirstPosition());
            for (ui32 i = 0; i < local.CountBits(); ++i, ++it) {
                Parts[it.GetPartId() - 1] = it.GetPart();
            }
        } else {
            TDiskDataExtractor extr;
            memRec.GetDiskData(&extr, nullptr);
            Parts[local.FirstPosition()] = extr.SwearOne();
        }
    }

    void TPartsCollectorMerger::Clear() {
        Parts.clear();
        Parts.resize(GType.TotalPartCount());
    }

} // NKikimr
