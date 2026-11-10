#include "controller_impl.h"
#include "secret_resolver.h"

namespace NKikimr::NReplication::NController {

class TController::TTxRefreshCredentials: public TTxBase {
    TEvPrivate::TEvRefreshCredentials::TPtr Event;
    TReplication::TPtr Replication;

public:
    TTxRefreshCredentials(TController* self, TEvPrivate::TEvRefreshCredentials::TPtr& ev)
        : TTxBase("TxRefreshCredentials", self), Event(ev) {}

    TTxType GetTxType() const override { return TXTYPE_RESOLVE_SECRET_RESULT; }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        const auto& result = *Event->Get();
        auto replication = Self->Find(result.ReplicationId);
        if (!replication || !replication->IsExpectedSecretRefresh(Event->Sender)
            || !IsNewSecretRevision(replication->GetAuthenticationErrorRevision(), result.Revision)
            || Self->ReplicationsWithSourceOffsetMismatch.contains(result.ReplicationId))
        {
            return true;
        }
        // The refresh actor checked access to every source topic with a fresh
        // provider. Only auth-only Error retains the marker; generic state writes
        // and explicit lifecycle/config changes atomically clear it.
        Replication = replication;
        Replication->ResetCredentials(ctx);
        Replication->UpdateSecret(result.Value, result.Revision, ToString(Event->Sender));
        Replication->SetState(TReplication::EState::Ready);
        NIceDb::TNiceDb db(txc.DB);
        db.Table<Schema::Replications>().Key(result.ReplicationId).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(TString()),
            NIceDb::TUpdate<Schema::Replications::AuthenticationErrorRevision>(TString()));
        for (auto* target : Replication->GetTargets()) {
            target->Shutdown(ctx);
            if (target->GetDstState() == TReplication::EDstState::Error) {
                target->SetDstState(TReplication::EDstState::Ready);
                target->SetIssue({});
                db.Table<Schema::Targets>().Key(result.ReplicationId, target->GetId()).Update(
                    NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
                    NIceDb::TUpdate<Schema::Targets::Issue>(TString()));
            }
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (!Replication) {
            return;
        }
        // No checkpoint, schema barrier or GLOBAL commit state is reset. Stop
        // old credential incarnations before the usual boot queue replaces them.
        for (const auto& [id, worker] : Self->Workers) {
            if (id.ReplicationId() != Replication->GetId()) {
                continue;
            }
            if (worker.HasSession()) {
                Self->StopQueue.emplace(id, worker.GetSession());
            } else {
                Self->BootQueue.insert(id);
            }
        }
        Replication->Progress(ctx);
        Self->ScheduleProcessQueues();
    }
};

void TController::Handle(TEvPrivate::TEvRefreshCredentials::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxRefreshCredentials(this, ev), ctx);
}

}
