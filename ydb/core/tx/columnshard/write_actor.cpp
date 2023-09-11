#include "columnshard_impl.h"
#include "columnshard_private_events.h"

#include <library/cpp/actors/core/actor_bootstrapped.h>
#include <ydb/core/util/backoff.h>

namespace NKikimr::NColumnShard {

namespace {

class TWriteActor : public TActorBootstrapped<TWriteActor>, public TMonitoringObjectsCounter<TWriteActor> {
    ui64 TabletId;
    TUsage ResourceUsage;

    IWriteController::TPtr WriteController;

    THashSet<ui32> YellowMoveChannels;
    THashSet<ui32> YellowStopChannels;
    TInstant Deadline;

public:
    TWriteActor(ui64 tabletId, IWriteController::TPtr writeController, const TInstant& deadline)
        : TabletId(tabletId)
        , WriteController(writeController)
        , Deadline(deadline)
    {}

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
            LOG_S_ERROR("Unsuccessful TEvPutResult for blob " << msg->Id.ToString() << " status: " << status << " reason: " << msg->ErrorReason);
            return SendResultAndDie(ctx, status);
        }

        LOG_S_TRACE("TEvPutResult for blob " << msg->Id.ToString());
        WriteController->OnBlobWriteResult(*msg);
        if (WriteController->IsBlobActionsReady()) {
            return SendResultAndDie(ctx, NKikimrProto::OK);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        LOG_S_WARN("TEvWakeup: write timeout at tablet " << TabletId << " (write)");
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
                std::move(YellowStopChannels),
                ResourceUsage);

        WriteController->OnReadyResult(ctx, putResult);
        Die(ctx);
    }

    void Bootstrap(const TActorContext& ctx) {
        if (Deadline != TInstant::Max()) {
            TInstant now = TAppData::TimeProvider->Now();
            if (Deadline <= now) {
                return SendResultAndDie(ctx, NKikimrProto::TIMEOUT);
            }

            const TDuration timeout = Deadline - now;
            ctx.Schedule(timeout, new TEvents::TEvWakeup());
        }

        while (auto writeInfo = WriteController->Next()) {
            ResourceUsage.Network += writeInfo->GetData().size();
            writeInfo->GetWriteOperator()->SendWriteBlobRequest(writeInfo->GetData(), writeInfo->GetBlobId());
        }

        if (WriteController->IsBlobActionsReady()) {
            return SendResultAndDie(ctx, NKikimrProto::OK);
        }
        Become(&TThis::StateWait);
    }

    STFUNC(StateWait) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvPutResult, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            default:
                break;
        }
    }
};

} // namespace

IActor* CreateWriteActor(ui64 tabletId, IWriteController::TPtr writeController, const TInstant& deadline) {
    return new TWriteActor(tabletId, writeController, deadline);
}

}
