#include "controller_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxWorkerRegistrationError: public TTxBase {
    TEvPrivate::TEvWorkerRegistrationError::TPtr Ev;
    TReplication::TPtr FailedReplication;

public:
    TTxWorkerRegistrationError(TController* self, TEvPrivate::TEvWorkerRegistrationError::TPtr& ev)
        : TTxBase("TxWorkerRegistrationError", self)
        , Ev(ev)
    {}

    TTxType GetTxType() const override {
        return TXTYPE_WORKER_ERROR;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        YDB_LOG_CREATE_CONTEXT(TxLogPrefix);
        const auto& result = *Ev->Get();
        auto replication = Self->Find(result.ReplicationId);
        if (!replication || replication->GetState() != TReplication::EState::Ready
            || replication->GetDesiredState() != TReplication::EState::Ready)
        {
            return true;
        }
        auto* target = replication->FindTarget(result.TargetId);
        // Validate in Execute: a newer ALTER/PAUSE/DROP may run while this
        // transaction is queued, replacing the registrar or its desired state.
        if (!target || target->GetDstState() != TReplication::EDstState::Ready
            || !target->IsExpectedWorkerRegistar(Ev->Sender))
        {
            return true;
        }

        FailedReplication = replication;
        target->SetDstState(TReplication::EDstState::Error);
        target->SetIssue(result.GetIssue());
        Self->SetReplicationError(*replication, *target);
        YDB_LOG_ERROR_CTX(ctx, "Worker registration failed", {"ev", result.ToString()});

        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(result.ReplicationId).Update(
            NIceDb::TUpdate<Schema::Replications::State>(replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(replication->GetIssue()));
        db.Table<Schema::Targets>().Key(result.ReplicationId, result.TargetId).Update(
            NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
            NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue()));
        return true;
    }

    void Complete(const TActorContext&) override {
        if (!FailedReplication || !FailedReplication->GetConfig().HasTransferSpecific()) {
            return;
        }
        // A restarted controller can adopt old transfer workers while its new
        // registrar authenticates. Stop them only after Error is durable, as
        // for a terminal error reported by a transfer worker.
        for (const auto& [id, worker] : Self->Workers) {
            if (id.ReplicationId() != FailedReplication->GetId()) {
                continue;
            }
            Self->BootQueue.erase(id);
            if (worker.HasSession()) {
                Self->StopQueue.emplace(id, worker.GetSession());
            }
        }
        Self->ScheduleProcessQueues();
    }
};

void TController::RunTxWorkerRegistrationError(TEvPrivate::TEvWorkerRegistrationError::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxWorkerRegistrationError(this, ev), ctx);
}

}
