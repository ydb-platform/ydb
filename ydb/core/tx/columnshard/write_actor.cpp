#include "columnshard_impl.h"
#include "columnshard_private_events.h"

#include <ydb/core/util/backoff.h>

#include <ydb/library/actors/core/actor_bootstrapped.h>
#include <ydb/library/actors/struct_log/log_stack.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TX_COLUMNSHARD_WRITE

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
        , Deadline(deadline)
    {
        YDB_LOG_DEBUG("",
            {"event", "actor_created"},
            {"tabletId", tabletId},
            {"debug", writeController->DebugString()});
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

        status = NYDBTest::TControllers::GetColumnShardController()->OverrideBlobPutResultOnWrite(status);

        if (status != NKikimrProto::OK) {
            YDB_LOG_ERROR_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "",
                {"event", "TEvPutResult"},
                {"blobId", msg->Id},
                {"status", status},
                {"error", msg->ErrorReason});
            WriteController->Abort(
                "cannot write blob " + msg->Id.ToString() + ", status: " + ::ToString(status) + ". reason: " + msg->ErrorReason);
            return SendResultAndDie(ctx, status);
        }

        WriteController->OnBlobWriteResult(*msg);
        if (WriteController->IsReady()) {
            return SendResultAndDie(ctx, NKikimrProto::OK);
        }
    }

    void Handle(TEvents::TEvWakeup::TPtr& /*ev*/, const TActorContext& ctx) {
        YDB_LOG_WARN_COMP(NActors::NStructuredLog::TLogStack::GetComponent(), "",
            {"event", "wakeup"});
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

        auto putResult = std::make_shared<TBlobPutResult>(putStatus, std::move(YellowMoveChannels), std::move(YellowStopChannels));

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
        YDB_LOG_CREATE_CONTEXT_COMP(NKikimrServices::TX_COLUMNSHARD,
            {"tabletId", TabletId});
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvBlobStorage::TEvPutResult, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            default:
                break;
        }
    }
};

}   // namespace

IActor* CreateWriteActor(ui64 tabletId, IWriteController::TPtr writeController, const TInstant deadline) {
    return new TWriteActor(tabletId, writeController, deadline);
}

}   // namespace NKikimr::NColumnShard
