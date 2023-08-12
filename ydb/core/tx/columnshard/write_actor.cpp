#include "columnshard_impl.h"
#include "columnshard_private_events.h"

#include <ydb/core/blobstorage/dsproxy/blobstorage_backoff.h>
#include <library/cpp/actors/core/actor_bootstrapped.h>


namespace NKikimr::NColumnShard {

namespace {

class TWriteActor : public TActorBootstrapped<TWriteActor> {
    ui64 TabletId;
    TUsage ResourceUsage;

    TBlobBatch BlobBatch;
    IWriteController::TPtr WriteController;

    THashSet<ui32> YellowMoveChannels;
    THashSet<ui32> YellowStopChannels;
    TInstant Deadline;
    std::optional<ui64> MaxSmallBlobSize;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::TX_COLUMNSHARD_WRITE_ACTOR;
    }

    TWriteActor(ui64 tabletId, TBlobBatch&& blobBatch, IWriteController::TPtr writeController, const TInstant& deadline, std::optional<ui64> maxSmallBlobSize = {})
        : TabletId(tabletId)
        , BlobBatch(std::move(blobBatch))
        , WriteController(writeController)
        , Deadline(deadline)
        , MaxSmallBlobSize(maxSmallBlobSize)
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

        BlobBatch.OnBlobWriteResult(ev);
        if (BlobBatch.AllBlobWritesCompleted()) {
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

        auto putResult = std::make_shared<TBlobPutResult>(putStatus, std::move(BlobBatch),
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

        auto blobsConstructor = WriteController->GetBlobConstructor();
        if (!blobsConstructor) {
            return SendResultAndDie(ctx, NKikimrProto::CORRUPTED);
        }
        auto status = NOlap::IBlobConstructor::EStatus::Finished;
        while (true) {
            status = blobsConstructor->BuildNext();
            if (status != NOlap::IBlobConstructor::EStatus::Ok) {
                break;
            }
            auto blobId = SendWriteBlobRequest(blobsConstructor->GetBlob(), ctx);
            blobsConstructor->RegisterBlobId(blobId);

        }
        if (status != NOlap::IBlobConstructor::EStatus::Finished) {
            return SendResultAndDie(ctx, NKikimrProto::CORRUPTED);
        }

        if (BlobBatch.AllBlobWritesCompleted()) {
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

private:
    TUnifiedBlobId SendWriteBlobRequest(const TString& data, const TActorContext& ctx) {
        ResourceUsage.Network += data.size();
        if (MaxSmallBlobSize && data.size() <= *MaxSmallBlobSize) {
            TUnifiedBlobId smallBlobId = BlobBatch.AddSmallBlob(data);
            Y_VERIFY(smallBlobId.IsSmallBlob());
            return smallBlobId;
        }
        return BlobBatch.SendWriteBlobRequest(data, Deadline, ctx);
    }
};

} // namespace

IActor* CreateWriteActor(ui64 tabletId, IWriteController::TPtr writeController, TBlobBatch&& blobBatch, const TInstant& deadline, const ui64 maxSmallBlobSize) {
    return new TWriteActor(tabletId, std::move(blobBatch), writeController, deadline, maxSmallBlobSize);
}

}
