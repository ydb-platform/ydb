#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxPauseTargetResult: public TTxBase {
    TEvPrivate::TEvPauseTargetResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxPauseTargetResult(TController* self, TEvPrivate::TEvPauseTargetResult::TPtr& ev)
        : TTxBase("TxPauseTargetResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_PAUSE_TARGET_RESULT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute: " << Ev->Get()->ToString());

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        Replication = Self->Find(rid);
        if (!Replication) {
            CLOG_W(ctx, "Unknown replication"
                << ": rid# " << rid);
            return true;
        }

        auto* target = Replication->FindTarget(tid);
        if (!target) {
            CLOG_W(ctx, "Unknown target"
                << ": rid# " << rid
                << ", tid# " << tid);
            return true;
        }

        if (target->GetDstState() != TReplication::EDstState::Pausing) {
            CLOG_W(ctx, "Dst state mismatch"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", state# " << target->GetDstState());
            return true;
        }
        target->SetDstState(TReplication::EDstState::Paused);

        if (Replication->CheckPauseDone()) {
            CLOG_N(ctx, "Replication paused"
                << ": rid# " << rid);
            Replication->SetState(TReplication::EState::Paused);
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState())
        );
        db.Table<Schema::Targets>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue())
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxTargetResult

void TController::RunTxPauseTargetResult(TEvPrivate::TEvPauseTargetResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxPauseTargetResult(this, ev), ctx);
}

}
