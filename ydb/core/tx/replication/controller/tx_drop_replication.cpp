#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxDropReplication: public TTxBase {
    TEvController::TEvDropReplication::TPtr Ev;
    THolder<TEvController::TEvDropReplicationResult> Result;
    TReplication::TPtr Replication;

public:
    explicit TTxDropReplication(TController* self, TEvController::TEvDropReplication::TPtr& ev)
        : TTxBase("TxDropReplication", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DROP_REPLICATION;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute: " << Ev->Get()->ToString());

        const auto& record = Ev->Get()->Record;
        Result = MakeHolder<TEvController::TEvDropReplicationResult>();
        Result->Record.MutableOperationId()->CopyFrom(record.GetOperationId());
        Result->Record.SetOrigin(Self->TabletID());

        const auto pathId = PathIdFromPathId(record.GetPathId());
        Replication = Self->Find(pathId);

        if (!Replication) {
            CLOG_W(ctx, "Cannot drop unknown replication"
                << ": pathId# " << pathId);

            Result->Record.SetStatus(NKikimrReplication::TEvDropReplicationResult::NOT_FOUND);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        Replication->SetState(TReplication::EState::Removing);
        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState())
        );

        CLOG_N(ctx, "Drop replication"
            << ": rid# " << Replication->GetId()
            << ", pathId# " << pathId);

        // TODO: delay response
        Result->Record.SetStatus(NKikimrReplication::TEvDropReplicationResult::SUCCESS);
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

        if (Result) {
            ctx.Send(Ev->Sender, Result.Release(), 0, Ev->Cookie);
        }

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxDropReplication

void TController::RunTxDropReplication(TEvController::TEvDropReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDropReplication(this, ev), ctx);
}

}
