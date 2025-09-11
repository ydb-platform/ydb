#include "metadata_actor.h"

#include <ydb/core/blobstorage/vdisk/common/blobstorage_dblogcutter.h>
#include <ydb/core/blobstorage/pdisk/blobstorage_pdisk.h>

namespace NKikimr {

class TMetadataActor : public TActorBootstrapped<TMetadataActor> {
    TIntrusivePtr<TMetadataContext> MetadataCtx;
    NKikimrVDiskData::TMetadataEntryPoint MetadataEntryPoint;

    ui64 CurEntryPointLsn = 0;
    TActorId CommitNotifyId;

    friend class TActorBootstrapped<TMetadataActor>;

    void Bootstrap(const TActorContext& ctx) {
        Y_UNUSED(ctx);
        TThis::Become(&TThis::StateFunc);
    }

    void WriteEntryPoint(const TActorContext& ctx) {
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

        ctx.Send(MetadataCtx->LoggerId, msg.release());
    }

    void Handle(TEvCommitMetadata::TPtr& ev, const TActorContext& ctx) {
        CommitNotifyId = ev->Sender;
        WriteEntryPoint(ctx);
    }

    void Handle(NPDisk::TEvLogResult::TPtr& ev, const TActorContext& ctx) {
        CHECK_PDISK_RESPONSE(MetadataCtx->VCtx, ev, ctx);

        if (ev->Get()->Status == NKikimrProto::OK) {
            CurEntryPointLsn = ev->Get()->Results.back().Lsn;
            if (CommitNotifyId) {
                ctx.Send(CommitNotifyId, new TEvCommitMetadataDone);
                CommitNotifyId = {};
            }
            ctx.Send(MetadataCtx->LogCutterId, new TEvVDiskCutLog(TEvVDiskCutLog::Metadata, CurEntryPointLsn));
        }
    }

    void Handle(NPDisk::TEvCutLog::TPtr& ev, const TActorContext& ctx) {
        if (CurEntryPointLsn < ev->Get()->FreeUpToLsn) {
            WriteEntryPoint(ctx);
        }
    }

    void HandlePoison(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Die(ctx);
    }

    STRICT_STFUNC(StateFunc,
        HFunc(TEvCommitMetadata, Handle)
        HFunc(NPDisk::TEvLogResult, Handle)
        HFunc(NPDisk::TEvCutLog, Handle)
        HFunc(TEvents::TEvPoisonPill, HandlePoison)
    )

    PDISK_TERMINATE_STATE_FUNC_DEF;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::BS_VDISK_METADATA_ACTOR;
    }

    TMetadataActor(TIntrusivePtr<TMetadataContext> metadataCtx,
            NKikimrVDiskData::TMetadataEntryPoint metadataEntryPoint)
        : TActorBootstrapped<TMetadataActor>()
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
