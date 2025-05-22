#include "controller_impl.h"

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

        if (target->GetDstState() != TReplication::EDstState::Creating) {
            CLOG_W(ctx, "Dst state mismatch"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", state# " << target->GetDstState());
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            target->SetDstPathId(Ev->Get()->DstPathId);
            target->SetDstState(TReplication::EDstState::Ready);

            CLOG_N(ctx, "Target dst created"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", pathId# " << Ev->Get()->DstPathId);
        } else {
            target->SetDstState(TReplication::EDstState::Error);
            target->SetIssue(TStringBuilder() << "Create dst error"
                << ": " << NKikimrScheme::EStatus_Name(Ev->Get()->Status)
                << ", " << Ev->Get()->Error);

            Replication->SetState(TReplication::EState::Error, TStringBuilder() << "Error in target #" << target->GetId()
                << ": " << target->GetIssue());

            CLOG_E(ctx, "Create dst error"
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
            NIceDb::TUpdate<Schema::Targets::DstPathOwnerId>(target->GetDstPathId().OwnerId),
            NIceDb::TUpdate<Schema::Targets::DstPathLocalId>(target->GetDstPathId().LocalPathId),
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

}; // TTxCreateDstResult

void TController::RunTxCreateDstResult(TEvPrivate::TEvCreateDstResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCreateDstResult(this, ev), ctx);
}

}
