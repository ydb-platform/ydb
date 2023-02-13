#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxDropDstResult: public TTxBase {
    TEvPrivate::TEvDropDstResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxDropDstResult(TController* self, TEvPrivate::TEvDropDstResult::TPtr& ev)
        : TTxBase("TxDropDstResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DROP_DST_RESULT;
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

        if (target->GetDstState() != TReplication::EDstState::Removing) {
            CLOG_W(ctx, "Dst state mismatch"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", state# " << target->GetDstState());
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            CLOG_N(ctx, "Target dst dropped"
                << ": rid# " << rid
                << ", tid# " << tid);
        } else {
            CLOG_E(ctx, "Drop dst error"
                << ": rid# " << rid
                << ", tid# " << tid
                << ", " << NKikimrScheme::EStatus_Name(Ev->Get()->Status)
                << ", " << Ev->Get()->Error);
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Targets>().Key(rid, tid).Delete();
        db.Table<Schema::SrcStreams>().Key(rid, tid).Delete();
        Replication->RemoveTarget(tid);

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxDropDstResult

void TController::RunTxDropDstResult(TEvPrivate::TEvDropDstResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDropDstResult(this, ev), ctx);
}

}
