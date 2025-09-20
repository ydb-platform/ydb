#include "metadata_actor.h"

#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {

class TMetadataActor : public TActor<TMetadataActor> {
    TIntrusivePtr<TMetadataContext> MetadataCtx;
    NKikimrVDiskData::TMetadataEntryPoint MetadataEntryPoint;

    ui64 CurEntryPointLsn = 0;
    TActorId CommitNotifyId;
    bool CommitInFlight = false;

    void WriteEntryPoint() {
        if (CommitInFlight) {
            return;
        }

        NPDisk::TCommitRecord commitRecord;
        commitRecord.IsStartingPoint = true;

        TRcBuf data(TRcBuf::Uninitialized(MetadataEntryPoint.ByteSizeLong()));
        const bool success = MetadataEntryPoint.SerializeToArray(
                reinterpret_cast<uint8_t*>(data.UnsafeGetDataMut()), data.GetSize());
        Y_VERIFY_S(success, MetadataCtx->VCtx->VDiskLogPrefix);

        TLsnSeg seg = MetadataCtx->LsnMngr->AllocLsnForLocalUse();

        auto msg = std::make_unique<NPDisk::TEvLog>(MetadataCtx->PDiskCtx->Dsk->Owner,
            MetadataCtx->PDiskCtx->Dsk->OwnerRound, TLogSignature::SignatureMetadata,
            commitRecord, data, seg, nullptr);

        Send(MetadataCtx->LoggerId, msg.release());

        CommitInFlight = true;
    }

    void Handle(TEvCommitVDiskMetadata::TPtr& ev) {
        if (!AppData()->FeatureFlags.GetEnableTinyDisks()) {
            Send(ev->Sender, new TEvCommitVDiskMetadataDone);
            return;
        }

        CommitNotifyId = ev->Sender;
        WriteEntryPoint();
    }

    void Handle(NPDisk::TEvLogResult::TPtr& ev) {
        CHECK_PDISK_RESPONSE(MetadataCtx->VCtx, ev, TActivationContext::AsActorContext());

        CommitInFlight = false;

        if (ev->Get()->Status == NKikimrProto::OK) {
            CurEntryPointLsn = ev->Get()->Results.back().Lsn;
            if (CommitNotifyId) {
                Send(CommitNotifyId, new TEvCommitVDiskMetadataDone);
                CommitNotifyId = {};
            }
            Send(MetadataCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Metadata, CurEntryPointLsn));
        }
    }

    void Handle(NPDisk::TEvCutLog::TPtr& ev) {
        if (!AppData()->FeatureFlags.GetEnableTinyDisks()) {
            return;
        }

        if (CurEntryPointLsn < ev->Get()->FreeUpToLsn) {
            WriteEntryPoint();
        }
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr& /*ev*/, const TActorContext& /*ctx*/) {
        PassAway();
    }

    STRICT_STFUNC(StateFunc,
        hFunc(TEvCommitVDiskMetadata, Handle)
        hFunc(NPDisk::TEvLogResult, Handle)
        hFunc(NPDisk::TEvCutLog, Handle)
        HFunc(TEvents::TEvPoisonPill, HandlePoison)
    )

    PDISK_TERMINATE_STATE_FUNC_DEF;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_VDISK_METADATA_ACTOR;
    }

    TMetadataActor(TIntrusivePtr<TMetadataContext> metadataCtx,
            NKikimrVDiskData::TMetadataEntryPoint metadataEntryPoint)
        : TActor(&TThis::StateFunc)
        , MetadataCtx(std::move(metadataCtx))
        , MetadataEntryPoint(std::move(metadataEntryPoint))
    {}
};

IActor *CreateMetadataActor(TIntrusivePtr<TMetadataContext>& metadataCtx,
        NKikimrVDiskData::TMetadataEntryPoint metadataEntryPoint)
{
    return new TMetadataActor(metadataCtx, std::move(metadataEntryPoint));
}

} // NKikimr
