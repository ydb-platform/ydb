#include "metadata_actor.h"

#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {

class TMetadataActor : public TActor<TMetadataActor> {
    TIntrusivePtr<TVDiskLogContext> LogCtx;
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
        Y_VERIFY_S(success, LogCtx->VCtx->VDiskLogPrefix);

        TLsnSeg seg = LogCtx->LsnMngr->AllocLsnForLocalUse();

        auto msg = std::make_unique<NPDisk::TEvLog>(LogCtx->PDiskCtx->Dsk->Owner,
            LogCtx->PDiskCtx->Dsk->OwnerRound, TLogSignature::SignatureMetadata,
            commitRecord, data, seg, nullptr);

        Send(LogCtx->LoggerId, msg.release());

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
        CHECK_PDISK_RESPONSE(LogCtx->VCtx, ev, TActivationContext::AsActorContext());

        CommitInFlight = false;

        if (ev->Get()->Status == NKikimrProto::OK) {
            CurEntryPointLsn = ev->Get()->Results.back().Lsn;
            if (CommitNotifyId) {
                Send(CommitNotifyId, new TEvCommitVDiskMetadataDone);
                CommitNotifyId = {};
            }
            Send(LogCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Metadata, CurEntryPointLsn));
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

    TMetadataActor(const TIntrusivePtr<TVDiskLogContext>& logCtx,
            NKikimrVDiskData::TMetadataEntryPoint metadataEntryPoint)
        : TActor(&TThis::StateFunc)
        , LogCtx(logCtx)
        , MetadataEntryPoint(metadataEntryPoint)
    {}
};

IActor *CreateMetadataActor(const TIntrusivePtr<TVDiskLogContext>& logCtx,
        NKikimrVDiskData::TMetadataEntryPoint metadataEntryPoint)
{
    return new TMetadataActor(logCtx, metadataEntryPoint);
}

} // NKikimr
