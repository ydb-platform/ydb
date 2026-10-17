#include "controller_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxPrepareAttachDst: public TTxBase {
    TEvPrivate::TEvPrepareAttachDst::TPtr Ev;
    bool Saved = false;

public:
    TTxPrepareAttachDst(TController* self, TEvPrivate::TEvPrepareAttachDst::TPtr& ev)
        : TTxBase("TxPrepareAttachDst", self)
        , Ev(ev)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_PREPARE_ATTACH_DST;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", Ev->Get()->ToString()});

        const auto* event = Ev->Get();
        const auto rid = event->ReplicationId;
        const auto tid = event->TargetId;
        auto replication = Self->Find(rid);
        if (!replication) {
            YDB_LOG_WARN_CTX(ctx, "Unknown replication",
                {"rid", rid});
            return true;
        }

        auto* target = replication->FindTarget(tid);
        if (!target) {
            YDB_LOG_WARN_CTX(ctx, "Unknown target",
                {"rid", rid},
                {"tid", tid});
            return true;
        }

        const bool attachmentNotStarted = target->GetDstState() != TReplication::EDstState::Attaching;
        const bool replicationDone = replication->GetDesiredState() == TReplication::EState::Done;
        if (replicationDone && attachmentNotStarted) {
            YDB_LOG_DEBUG_CTX(ctx, "Attachment skipped after DONE",
                {"rid", rid},
                {"tid", tid});
            return true;
        }

        switch (target->GetDstState()) {
        case TReplication::EDstState::Creating:
        case TReplication::EDstState::Alter:
        case TReplication::EDstState::Attaching:
            break;
        default:
            YDB_LOG_WARN_CTX(ctx, "Dst state mismatch",
                {"rid", rid},
                {"tid", tid},
                {"state", target->GetDstState()});
            return true;
        }

        YDB_LOG_NOTICE_CTX(ctx, "Prepare dst attachment",
            {"rid", rid},
            {"tid", tid},
            {"pathId", event->DstPathId});
        target->SetPendingDstPathId(event->DstPathId);
        target->SetDstState(TReplication::EDstState::Attaching);
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Targets>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::Targets::PendingDstPathOwnerId>(event->DstPathId.OwnerId),
            NIceDb::TUpdate<Schema::Targets::PendingDstPathLocalId>(event->DstPathId.LocalPathId),
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState())
        );
        Saved = true;
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        if (Saved) {
            ctx.Send(Ev->Sender, new TEvPrivate::TEvPrepareAttachDstResult());
        }
    }
};

class TController::TTxCreateDstResult: public TTxBase {
    TEvPrivate::TEvCreateDstResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxCreateDstResult(TController* self, TEvPrivate::TEvCreateDstResult::TPtr& ev)
        : TTxBase("TxCreateDstResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_DST_RESULT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Execute",
            {"ev", Ev->Get()->ToString()});

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        Replication = Self->Find(rid);
        if (!Replication) {
            YDB_LOG_WARN_CTX(ctx, "Unknown replication",
                {"rid", rid});
            return true;
        }

        auto* target = Replication->FindTarget(tid);
        if (!target) {
            YDB_LOG_WARN_CTX(ctx, "Unknown target",
                {"rid", rid},
                {"tid", tid});
            return true;
        }

        const auto dstState = target->GetDstState();
        const bool expectsResult = dstState == TReplication::EDstState::Creating
            || dstState == TReplication::EDstState::Alter
            || dstState == TReplication::EDstState::Attaching;
        if (!expectsResult) {
            YDB_LOG_WARN_CTX(ctx, "Dst state mismatch",
                {"rid", rid},
                {"tid", tid},
                {"state", dstState});
            return true;
        }

        const bool droppingAttachment = Replication->GetState() == TReplication::EState::Removing
            && dstState == TReplication::EDstState::Attaching;
        if (droppingAttachment) {
            target->SetDstState(TReplication::EDstState::Removing);
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Targets>().Key(rid, tid).Update<Schema::Targets::DstState>(target->GetDstState());
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            const bool wasAltering = dstState == TReplication::EDstState::Alter
                || dstState == TReplication::EDstState::Attaching;
            target->SetDstPathId(Ev->Get()->DstPathId);
            target->SetPendingDstPathId({});
            if (Replication->GetDesiredState() == TReplication::EState::Done) {
                target->SetDstState(TReplication::EDstState::Alter);
            } else {
                target->SetDstState(TReplication::EDstState::Ready);
                if (wasAltering && Replication->CheckAlterDone()) {
                    Replication->SetState(Replication->GetDesiredState());
                }
            }

            YDB_LOG_NOTICE_CTX(ctx, "Target dst created",
                {"rid", rid},
                {"tid", tid},
                {"pathId", Ev->Get()->DstPathId});
        } else {
            target->SetDstState(TReplication::EDstState::Error);
            target->SetIssue(TStringBuilder() << "Create dst error"
                << ": " << NKikimrScheme::EStatus_Name(Ev->Get()->Status)
                << ", " << Ev->Get()->Error);

            Replication->SetState(TReplication::EState::Error, TStringBuilder() << "Error in target #" << target->GetId()
                << ": " << target->GetIssue());

            YDB_LOG_ERROR_CTX(ctx, "Create dst error",
                {"rid", rid},
                {"tid", tid},
                {"status", NKikimrScheme::EStatus_Name(Ev->Get()->Status)},
                {"error", Ev->Get()->Error});
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue())
        );
        db.Table<Schema::Targets>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::Targets::DstPathOwnerId>(target->GetDstPathId().OwnerId),
            NIceDb::TUpdate<Schema::Targets::DstPathLocalId>(target->GetDstPathId().LocalPathId),
            NIceDb::TUpdate<Schema::Targets::PendingDstPathOwnerId>(target->GetPendingDstPathId().OwnerId),
            NIceDb::TUpdate<Schema::Targets::PendingDstPathLocalId>(target->GetPendingDstPathId().LocalPathId),
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue())
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        YDB_LOG_DEBUG_CTX(ctx, "Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxCreateDstResult

void TController::RunTxCreateDstResult(TEvPrivate::TEvCreateDstResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCreateDstResult(this, ev), ctx);
}

void TController::RunTxPrepareAttachDst(TEvPrivate::TEvPrepareAttachDst::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxPrepareAttachDst(this, ev), ctx);
}

}
