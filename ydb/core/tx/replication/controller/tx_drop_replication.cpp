#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxDropReplication: public TTxBase {
    TEvController::TEvDropReplication::TPtr PubEv;
    TEvPrivate::TEvDropReplication::TPtr PrivEv;
    THolder<IEventHandle> Result; // TEvController::TEvDropReplicationResult
    TReplication::TPtr Replication;

public:
    explicit TTxDropReplication(TController* self, TEvController::TEvDropReplication::TPtr& ev)
        : TTxBase("TxDropReplication", self)
        , PubEv(ev)
    {
    }

    explicit TTxDropReplication(TController* self, TEvPrivate::TEvDropReplication::TPtr& ev)
        : TTxBase("TxDropReplication", self)
        , PrivEv(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_DROP_REPLICATION;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        if (PubEv) {
            return ExecutePub(txc, ctx);
        } else if (PrivEv) {
            return ExecutePriv(txc, ctx);
        } else {
            Y_ABORT("unreachable");
        }
    }

    bool ExecutePub(TTransactionContext& txc, const TActorContext& ctx) {
        CLOG_D(ctx, "Execute: " << PubEv->Get()->ToString());

        const auto& record = PubEv->Get()->Record;
        const auto pathId = PathIdFromPathId(record.GetPathId());
        const auto& opId = record.GetOperationId();
        Replication = Self->Find(pathId);

        if (!Replication) {
            CLOG_W(ctx, "Cannot drop unknown replication"
                << ": pathId# " << pathId);

            auto ev = MakeHolder<TEvController::TEvDropReplicationResult>();
            ev->Record.MutableOperationId()->CopyFrom(record.GetOperationId());
            ev->Record.SetOrigin(Self->TabletID());
            ev->Record.SetStatus(NKikimrReplication::TEvDropReplicationResult::NOT_FOUND);
            Result = MakeHolder<IEventHandle>(PubEv->Sender, ctx.SelfID, ev.Release());

            return true;
        }

        if (Replication->GetState() == TReplication::EState::Removing) {
            Replication->SetDropOp(PubEv->Sender, std::make_pair(opId.GetTxId(), opId.GetPartId()));
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);

        Replication->SetState(TReplication::EState::Removing);
        db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState())
        );

        for (ui64 tid = 0; tid < Replication->GetNextTargetId(); ++tid) {
            auto* target = Replication->FindTarget(tid);
            if (!target) {
                continue;
            }

            target->Shutdown(ctx);

            target->SetStreamState(TReplication::EStreamState::Removing);
            db.Table<Schema::SrcStreams>().Key(Replication->GetId(), tid).Update(
                NIceDb::TUpdate<Schema::SrcStreams::State>(target->GetStreamState())
            );

            if (record.GetCascade()) {
                target->SetDstState(TReplication::EDstState::Removing);
                db.Table<Schema::Targets>().Key(Replication->GetId(), tid).Update(
                    NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState())
                );
            }
        }

        CLOG_N(ctx, "Drop replication"
            << ": rid# " << Replication->GetId()
            << ", pathId# " << pathId);

        Replication->SetDropOp(PubEv->Sender, std::make_pair(opId.GetTxId(), opId.GetPartId()));
        return true;
    }

    bool ExecutePriv(TTransactionContext& txc, const TActorContext& ctx) {
        CLOG_D(ctx, "Execute: " << PrivEv->Get()->ToString());

        const auto rid = PrivEv->Get()->ReplicationId;
        Replication = Self->Find(rid);

        if (!Replication) {
            CLOG_W(ctx, "Cannot drop unknown replication"
                << ": rid# " << rid);
            return true;
        }

        if (Replication->GetState() != TReplication::EState::Removing) {
            CLOG_W(ctx, "Replication state mismatch"
                << ": rid# " << rid
                << ", state# " << Replication->GetState());
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(rid).Delete();

        if (const auto& op = Replication->GetDropOp()) {
            auto ev = MakeHolder<TEvController::TEvDropReplicationResult>();
            ev->Record.MutableOperationId()->SetTxId(op->OperationId.first);
            ev->Record.MutableOperationId()->SetPartId(op->OperationId.second);
            ev->Record.SetOrigin(Self->TabletID());
            ev->Record.SetStatus(NKikimrReplication::TEvDropReplicationResult::SUCCESS);
            Result = MakeHolder<IEventHandle>(op->Sender, ctx.SelfID, ev.Release());
        }

        Self->Remove(rid);
        Replication.Reset();

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");

        if (Result) {
            ctx.Send(Result.Release());
        }

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxDropReplication

void TController::RunTxDropReplication(TEvController::TEvDropReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDropReplication(this, ev), ctx);
}

void TController::RunTxDropReplication(TEvPrivate::TEvDropReplication::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxDropReplication(this, ev), ctx);
}

}
