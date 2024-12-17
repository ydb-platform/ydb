#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxWorkerError: public TTxBase {
    const TWorkerId WorkerId;
    const TString Error;

public:
    explicit TTxWorkerError(TController* self, const TWorkerId& id, const TString& error)
        : TTxBase("TxWorkerError", self)
        , WorkerId(id)
        , Error(error)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_WORKER_ERROR;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        CLOG_D(ctx, "Execute"
            << ": workerId# " << WorkerId
            << ", error# " << Error);

        auto replication = Self->Find(WorkerId.ReplicationId());
        if (!replication) {
            CLOG_W(ctx, "Unknown replication"
                << ": rid# " << WorkerId.ReplicationId());
            return true;
        }

        auto* target = replication->FindTarget(WorkerId.TargetId());
        if (!target) {
            CLOG_W(ctx, "Unknown target"
                << ": rid# " << WorkerId.ReplicationId()
                << ", tid# " << WorkerId.TargetId());
            return true;
        }

        CLOG_E(ctx, "Worker error"
            << ": rid# " << WorkerId.ReplicationId()
            << ", tid# " << WorkerId.TargetId()
            << ", error# " << Error);

        target->SetDstState(TReplication::EDstState::Error);
        target->SetIssue(Error);

        replication->SetState(TReplication::EState::Error, TStringBuilder() << "Error in target #" << target->GetId()
            << ": " << target->GetIssue());

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(WorkerId.ReplicationId()).Update(
            NIceDb::TUpdate<Schema::Replications::State>(replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(replication->GetIssue())
        );
        db.Table<Schema::Targets>().Key(WorkerId.ReplicationId(), WorkerId.TargetId()).Update(
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue())
        );

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        CLOG_D(ctx, "Complete");
    }

}; // TTxWorkerError

void TController::RunTxWorkerError(const TWorkerId& id, const TString& error, const TActorContext& ctx) {
    Execute(new TTxWorkerError(this, id, error), ctx);
}

}
