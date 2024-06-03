#include "utils.h"


namespace NKikimr {
namespace NBalancing {

    TVDiskID GetMainReplicaVDiskId(const TBlobStorageGroupInfo& gInfo, const TLogoBlobID& key) {
        TBlobStorageGroupInfo::TOrderNums orderNums;
        gInfo.GetTopology().PickSubgroup(key.Hash(), orderNums);
        return gInfo.GetVDiskId(orderNums[key.PartId() - 1]);
    }


    ///////////////////////////// TPartsCollectorMerger /////////////////////////////

    TPartsCollectorMerger::TPartsCollectorMerger(const TBlobStorageGroupType gType)
        : GType(gType)
    {
    }

    void TPartsCollectorMerger::AddFromSegment(const TMemRecLogoBlob& memRec, const TDiskPart *outbound, const TKeyLogoBlob& /*key*/, ui64 /*lsn*/) {
        Ingress.Merge(memRec.GetIngress());

        const NMatrix::TVectorType local = memRec.GetLocalParts(GType);
        if (local.Empty()) {
            return;
        }

        TDiskDataExtractor extr;
        memRec.GetDiskData(&extr, outbound);

        switch (memRec.GetType()) {
            case TBlobType::DiskBlob: {
                const TDiskPart &data = extr.SwearOne();
                if (data.ChunkIdx && data.Size) {
                    Parts.emplace_back(local, data);
                }
                break;
            }
            case TBlobType::HugeBlob:
            case TBlobType::ManyHugeBlobs: {
                ui8 partIdx = local.FirstPosition();
                for (const TDiskPart *part = extr.Begin; part != extr.End; ++part, partIdx = local.NextPosition(partIdx)) {
                    if (part->ChunkIdx && part->Size) {
                        Parts.emplace_back(NMatrix::TVectorType::MakeOneHot(partIdx, GType.TotalPartCount()), *part);
                    }
                }
                break;
            }
            default:
                Y_ABORT("Impossible case");
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
                Parts.emplace_back(NMatrix::TVectorType::MakeOneHot(it.GetPartId() - 1, GType.TotalPartCount()), it.GetPart());
            }
        } else {
            Y_DEBUG_ABORT_UNLESS(local.CountBits() == 1, "Only one part can be on disk");
            TDiskDataExtractor extr;
            memRec.GetDiskData(&extr, nullptr);
            Parts.emplace_back(NMatrix::TVectorType::MakeOneHot(local.FirstPosition(), GType.TotalPartCount()), extr.SwearOne());
        }
    }

    void TPartsCollectorMerger::Clear() {
        Parts.clear();
        Parts.resize(GType.TotalPartCount());
    }

    void TWaiter::Init() {
        Y_VERIFY(State == EInit || State == ECompleteJob, "Invalid state");
        State = EInit;
    }

    ui64 TWaiter::StartJob(TInstant now, ui32 count, ui32 partsLeft) {
        Y_VERIFY(State == EInit, "Invalid state");
        State = EStartJob;
        SendPartsStart = now;
        ToSendPartsCount = count;
        PartsLeft = partsLeft;
        return Epoch;
    }

    void TWaiter::PartJobDone(ui64 epoch, ui32 cnt) {
        if (epoch != Epoch) {
            return;
        }
        Y_VERIFY(State == EStartJob, "Invalid state");
        SentPartsCount += cnt;
    }

    NActors::TEvents::TEvCompleted* TWaiter::IsJobDone(ui32 epoch, TInstant now) {
        if (epoch != Epoch) {
            return nullptr;
        }
        Y_VERIFY(State == EStartJob, "Invalid state");
        if (SentPartsCount == ToSendPartsCount || now > SendPartsStart + SendTimeout) {
            State = PartsLeft == 0 ? EPassAway : ECompleteJob;
            ToSendPartsCount = 0;
            SentPartsCount = 0;
            ++Epoch;
            return new NActors::TEvents::TEvCompleted(SENDER_ID, PartsLeft);
        }
        return nullptr;
    }

    bool TWaiter::IsPassAway() {
        Y_VERIFY(State == ECompleteJob || State == EPassAway, "Invalid state");
        return State == EPassAway;
    }

} // NBalancing
} // NKikimr
