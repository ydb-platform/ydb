#include "controller_impl.h"
#include "target_cdc.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

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

        // Ignore results from a creator replaced by ALTER/DONE/DROP.
        if (!target->IsExpectedDstCreator(Ev->Sender)) {
            YDB_LOG_DEBUG_CTX(ctx, "Ignore obsolete destination creator",
                {"rid", rid},
                {"tid", tid},
                {"sender", Ev->Sender});
            Replication.Reset();
            return true;
        }

        const auto dstState = target->GetDstState();
        const bool expectsResult = dstState == TReplication::EDstState::Creating
            || dstState == TReplication::EDstState::Alter;
        if (!expectsResult) {
            YDB_LOG_WARN_CTX(ctx, "Dst state mismatch",
                {"rid", rid},
                {"tid", tid},
                {"state", dstState});
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        auto* cdc = GetCdcTarget(target);
        if (Ev->Get()->IsSuccess() && cdc) {
            if (dstState != TReplication::EDstState::Creating) {
                Replication.Reset();
                return true;
            }

            Y_ABORT_UNLESS(Ev->Get()->CdcState);
            const auto& created = *Ev->Get()->CdcState;
            auto& state = cdc->GetCdc();
            if (Ev->Get()->DstPathId) {
                target->SetDstPathId(Ev->Get()->DstPathId);
                target->SetDstState(TReplication::EDstState::Alter);
                *state.MutableStreamPathId() = created.GetStreamPathId();
                state.SetTrimRequestId(created.GetTrimRequestId());
                state.ClearInitialCreateTxId();
                state.ClearSourceStream();
                state.ClearDefinition();
                state.ClearTopicSetup();

                if (Replication->GetState() == TReplication::EState::Removing) {
                    state.SetPhase(NKikimrReplicationController::TCdcTarget::TRIMMING);
                } else if (Replication->GetDesiredState() == TReplication::EState::Done
                    && state.GetPhase() == NKikimrReplicationController::TCdcTarget::STOPPING)
                {
                    // This Disabled topic has never mirrored any data.
                    state.SetPhase(NKikimrReplicationController::TCdcTarget::PREPARED);
                }
            } else {
                // Persist the reserved CREATE before starting destination DDL.
                *state.MutableTablePathId() = created.GetTablePathId();
                state.SetActivateOnDetach(created.GetActivateOnDetach());
                state.SetInitialCreateTxId(created.GetInitialCreateTxId());
                *state.MutableDefinition() = created.GetDefinition();
                *state.MutableTopicSetup() = created.GetTopicSetup();
            }

            cdc->Shutdown(ctx);
            db.Table<Schema::Targets>().Key(rid, tid).Update(
                NIceDb::TUpdate<Schema::Targets::Cdc>(state.SerializeAsString()));
        } else if (Ev->Get()->IsSuccess()) {
            const bool wasAltering = dstState == TReplication::EDstState::Alter;
            target->SetDstPathId(Ev->Get()->DstPathId);
            if (target->GetKind() == TReplication::ETargetKind::Topic) {
                // Topics always go through ALTER after creation.
                target->SetDstState(TReplication::EDstState::Alter);
            } else if (Replication->GetDesiredState() == TReplication::EState::Done) {
                target->SetDstState(TReplication::EDstState::Alter);
            } else if (Ev->Get()->PreparingCdc) {
                target->SetDstState(TReplication::EDstState::PreparingCdc);
            } else if (Ev->Get()->WaitingImport) {
                if (Replication->GetDesiredState() == TReplication::EState::Paused) {
                    target->SetDstState(TReplication::EDstState::WaitingImport);
                } else {
                    // STANDBY may arrive before placeholder creation completes.
                    target->SetDstPathId({});
                    target->SetDstState(TReplication::EDstState::Alter);
                }
            } else {
                // Other attachments can keep the replication Ready while PAUSE
                // is pending. Do not let this newly attached target start workers.
                target->SetDstState(Replication->GetDesiredState() == TReplication::EState::Paused
                    ? TReplication::EDstState::Paused
                    : TReplication::EDstState::Ready);
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

            Self->SetReplicationError(*Replication, *target);

            YDB_LOG_ERROR_CTX(ctx, "Create dst error",
                {"rid", rid},
                {"tid", tid},
                {"status", NKikimrScheme::EStatus_Name(Ev->Get()->Status)},
                {"error", Ev->Get()->Error});
        }

        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue())
        );
        db.Table<Schema::Targets>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::Targets::DstPathOwnerId>(target->GetDstPathId().OwnerId),
            NIceDb::TUpdate<Schema::Targets::DstPathLocalId>(target->GetDstPathId().LocalPathId),
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

}
