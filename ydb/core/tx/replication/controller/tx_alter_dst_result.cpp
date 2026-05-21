#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxAlterDstResult: public TTxBase {
    TEvPrivate::TEvAlterDstResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxAlterDstResult(TController* self, TEvPrivate::TEvAlterDstResult::TPtr& ev)
        : TTxBase("TxAlterDstResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_ALTER_DST_RESULT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Execute: " << Ev->Get()->ToString());

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        Replication = Self->Find(rid);
        if (!Replication) {
            LOG_WARN_S  (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Unknown replication"
                << ": rid# " << rid);
            return true;
        }

        auto* target = Replication->FindTarget(tid);
        if (!target) {
            LOG_WARN_S  (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Unknown target"
                << ": rid# " << rid
                << ", tid# " << tid);
            return true;
        }

        if (target->GetDstState() != TReplication::EDstState::Alter) {
            LOG_WARN_S  (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Dst state mismatch"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", state# " << target->GetDstState());
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            target->SetDstState(NextState(Replication->GetDesiredState()));
            target->UpdateConfig(Replication->GetConfig());

            LOG_NOTICE_S(ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Target dst altered"
                << ": rid# " << rid
                << ", tid# " << tid);

            if (Replication->CheckAlterDone()) {
                LOG_NOTICE_S(ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Replication altered"
                    << ": rid# " << rid
                    << ", state# " << Replication->GetDesiredState());
                Replication->SetState(Replication->GetDesiredState());
            }
        } else {
            target->SetDstState(TReplication::EDstState::Error);
            target->SetIssue(TStringBuilder() << "Alter dst error"
                << ": " << NKikimrScheme::EStatus_Name(Ev->Get()->Status)
                << ", " << Ev->Get()->Error);

            Replication->SetState(TReplication::EState::Error, TStringBuilder() << "Error in target #" << target->GetId()
                << ": " << target->GetIssue());

            LOG_ERROR_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Alter dst error"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", " << NKikimrScheme::EStatus_Name(Ev->Get()->Status)
                << ", " << Ev->Get()->Error);
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue())
        );
        db.Table<Schema::Targets>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue())
        );

        if (Replication->GetState() != TReplication::EState::Ready) {
            Replication.Reset();
        }

        return true;
    }

    TReplication::EDstState NextState(TReplication::EState state) {
        switch (state) {
        case TReplication::EState::Done:
            return TReplication::EDstState::Done;
        case TReplication::EState::Ready:
            return TReplication::EDstState::Ready;
        case TReplication::EState::Error:
            return TReplication::EDstState::Error;
        case TReplication::EState::Removing:
            return TReplication::EDstState::Removing;
        case TReplication::EState::Paused:
            return TReplication::EDstState::Paused;
        }
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxAlterDstResult

void TController::RunTxAlterDstResult(TEvPrivate::TEvAlterDstResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxAlterDstResult(this, ev), ctx);
}

}

