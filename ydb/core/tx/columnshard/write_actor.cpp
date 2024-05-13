#include "columnshard_impl.h"
#include "columnshard_private_events.h"

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NColumnShard {

namespace {

class TWriteActor: public TActorBootstrapped<TWriteActor>, public TMonitoringObjectsCounter<TWriteActor> {
    ui64 TabletId;
    IWriteController::TPtr WriteController;

    THashSet<ui32> YellowMoveChannels;
    THashSet<ui32> YellowStopChannels;
    TInstant Deadline;

public:
    TWriteActor(ui64 tabletId, IWriteController::TPtr writeController, const TInstant deadline)
        : TabletId(tabletId)
        , WriteController(writeController)
        , Deadline(deadline) {
    }

    void Handle(TEvBlobStorage::TEvPutResult::TPtr& ev, const TActorContext& ctx) {
        TEvBlobStorage::TEvPutResult* msg = ev->Get();
        auto status = msg->Status;

        if (msg->StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceLightYellowMove)) {
            YellowMoveChannels.insert(msg->Id.Channel());
        }
        if (msg->StatusFlags.Check(NKikimrBlobStorage::StatusDiskSpaceYellowStop)) {
            YellowStopChannels.insert(msg->Id.Channel());
        }

        if (status != NKikimrProto::OK) {
            ACFL_ERROR("event", "TEvPutResult")("blob_id", msg->Id.ToString())("status", status)("error", msg->ErrorReason);
            WriteController->Abort("cannot write blob " + msg->Id.ToString() + ", status: " + ::ToString(status) + ". reason: " + msg->ErrorReason);
            return SendResultAndDie(ctx, status);
        }

        WriteController->OnBlobWriteResult(*msg);
        if (WriteController->IsReady()) {
            return SendResultAndDie(ctx, NKikimrProto::OK);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& /*ev*/, const TActorContext& ctx) {
        ACFL_WARN("event", "wakeup");
        SendResultAndDie(ctx, NKikimrProto::TIMEOUT);
    }

    void SendResultAndDie(const TActorContext& ctx, const NKikimrProto::EReplyStatus status) {
        NKikimrProto::EReplyStatus putStatus = status;
        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                putStatus = NKikimrProto::TIMEOUT;
            }
        }

        auto putResult = std::make_shared<TBlobPutResult>(putStatus,
            std::move(YellowMoveChannels),
            std::move(YellowStopChannels));

        WriteController->OnReadyResult(ctx, putResult);
        Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        WriteController->OnStartSending();
        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                return SendResultAndDie(ctx, NKikimrProto::TIMEOUT);
            }

            const TDuration timeout = Deadline - now;
            ctx.Schedule(timeout, new TEvents::TEvWakeup());
        }

        while (auto writeInfo = WriteController->Next()) {
            writeInfo->GetWriteOperator()->SendWriteBlobRequest(writeInfo->GetData(), writeInfo->GetBlobId());
        }

        if (WriteController->IsReady()) {
            return SendResultAndDie(ctx, NKikimrProto::OK);
        }
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        NActors::TLogContextGuard logGuard = NActors::TLogContextBuilder::Build(NKikimrServices::TX_COLUMNSHARD)("tablet_id", TabletId);
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvPutResult, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            default:
                break;
        }
    }
};

} // namespace

IActor* CreateWriteActor(ui64 tabletId, IWriteController::TPtr writeController, const TInstant deadline) {
    return new TWriteActor(tabletId, writeController, deadline);
}

}
