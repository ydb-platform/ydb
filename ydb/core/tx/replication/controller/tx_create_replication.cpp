#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxCreateReplication: public TTxBase {
    TEvController::TEvCreateReplication::TPtr Ev;
    THolder<TEvController::TEvCreateReplicationResult> Result;
    TReplication::TPtr Replication;

public:
    explicit TTxCreateReplication(TController* self, TEvController::TEvCreateReplication::TPtr& ev)
        : TTxBase("TxCreateReplication", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_CREATE_REPLICATION;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute: " << Ev->Get()->ToString());

        auto& record = Ev->Get()->Record;
        Result = MakeHolder<TEvController::TEvCreateReplicationResult>();
        Result->Record.MutableOperationId()->CopyFrom(record.GetOperationId());
        Result->Record.SetOrigin(Self->TabletID());

        const auto pathId = PathIdFromPathId(record.GetPathId());
        if (Self->Find(pathId)) {
            CLOG_W(ctx, "Replication already exists"
                << ": pathId# " << pathId);

            Result->Record.SetStatus(NKikimrReplication::TEvCreateReplicationResult::ALREADY_EXISTS);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        const auto rid = Self->SysParams.AllocateReplicationId(db);
        CLOG_N(ctx, "Add replication"
            << ": rid# " << rid
            << ", pathId# " << pathId);

        db.Table<Schema::Replications>().Key(rid).Update(
            NIceDb::TUpdate<Schema::Replications::PathOwnerId>(pathId.OwnerId),
            NIceDb::TUpdate<Schema::Replications::PathLocalId>(pathId.LocalPathId),
            NIceDb::TUpdate<Schema::Replications::Config>(record.GetConfig().SerializeAsString())
        );
        Replication = Self->Add(rid, pathId, std::move(*record.MutableConfig()));

        Result->Record.SetStatus(NKikimrReplication::TEvCreateReplicationResult::SUCCESS);
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

}; // TTxCreateReplication

void TController::RunTxCreateReplication(TEvController::TEvCreateReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxCreateReplication(this, ev), ctx);
}

}
