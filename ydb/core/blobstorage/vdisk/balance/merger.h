#pragma once

#include "defs.h"


namespace NKikimr {
    struct TPartsCollectorMerger {
        const TBlobStorageGroupType GType;

        TIngress Ingress;
        TVector<std::optional<std::variant<TDiskPart, TRope>>> Parts;

        TPartsCollectorMerger(const TBlobStorageGroupType gType)
            : GType(gType)
        {
            Parts.resize(GType.TotalPartCount());
        }

        static bool HaveToMergeData() { return true; }

        void AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob&, ui64) {
            Ingress.Merge(memRec.GetIngress());

            TDiskDataExtractor extr;
            memRec.GetDiskData(&extr, outbound);
            const NMatrix::TVectorType local = memRec.GetLocalParts(GType);

            ui8 partIdx = local.FirstPosition();
            for (const TDiskPart *part = extr.Begin; part != extr.End; ++part, partIdx = local.NextPosition(partIdx)) {
                if (part->ChunkIdx && part->Size) {
                    Parts[partIdx] = *part;
                }
            }
        }

        void AddFromFresh(const TMemRecLogoBlob& memRec, const TRope* data, const TKeyLogoBlob& key, ui64 /*lsn*/) {
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

        void Clear() {
            Parts.clear();
            Parts.resize(GType.TotalPartCount());
        }
    };
}
