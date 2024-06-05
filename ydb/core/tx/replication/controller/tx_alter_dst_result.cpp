#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxAlterDstResult: public TTxBase {
    TEvPrivate::TEvAlterDstResult::TPtr Ev;

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
        CLOG_D(ctx, "Execute: " << Ev->Get()->ToString());

        const auto rid = Ev->Get()->ReplicationId;
        const auto tid = Ev->Get()->TargetId;

        auto replication = Self->Find(rid);
        if (!replication) {
            CLOG_W(ctx, "Unknown replication"
                << ": rid# " << rid);
            return true;
        }

        auto* target = replication->FindTarget(tid);
        if (!target) {
            CLOG_W(ctx, "Unknown target"
                << ": rid# " << rid
                << ", tid# " << tid);
            return true;
        }

        if (target->GetDstState() != TReplication::EDstState::Alter) {
            CLOG_W(ctx, "Dst state mismatch"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", state# " << target->GetDstState());
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            target->SetDstState(TReplication::EDstState::Done);

            CLOG_N(ctx, "Target dst altered"
                << ": rid# " << rid
                << ", tid# " << tid);

            if (replication->CheckAlterDone()) {
                CLOG_N(ctx, "Replication altered"
                    << ": rid# " << rid);
                replication->SetState(TReplication::EState::Done);
            }
        } else {
            target->SetDstState(TReplication::EDstState::Error);
            target->SetIssue(TStringBuilder() << "Alter dst error"
                << ": " << NKikimrScheme::EStatus_Name(Ev->Get()->Status)
                << ", " << Ev->Get()->Error);

            replication->SetState(TReplication::EState::Error, TStringBuilder() << "Error in target #" << target->GetId()
                << ": " << target->GetIssue());

            CLOG_E(ctx, "Alter dst error"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", " << NKikimrScheme::EStatus_Name(Ev->Get()->Status)
                << ", " << Ev->Get()->Error);
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::State>(replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(replication->GetIssue())
        );
        db.Table<Schema::Targets>().Key(rid, tid).Update(
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue())
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");
    }

}; // TTxAlterDstResult

void TController::RunTxAlterDstResult(TEvPrivate::TEvAlterDstResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxAlterDstResult(this, ev), ctx);
}

}

