#include "utils.h"


namespace NKikimr {

    TVector<ui8> ApplyMask(NMatrix::TVectorType vec, NMatrix::TVectorType mask) {
        auto parts = vec & mask;
        TVector<ui8> res(Reserve(parts.CountBits()));
        for (ui8 i = parts.FirstPosition(); i != parts.GetSize(); i = parts.NextPosition(i)) {
            res.push_back(i);
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
        return ApplyMask(ingress.LocalParts(top.GType), moveMask);
    }

    TVector<ui8> PartsToDelete(
        const TBlobStorageGroupInfo::TTopology& top,
        const TVDiskIdShort &vdisk,
        const TLogoBlobID &key,
        const TIngress& ingress
    ) {
        auto [_, delMask] = ingress.HandoffParts(&top, vdisk, key);
        return ApplyMask(ingress.LocalParts(top.GType), delMask);
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

} // NKikimr
