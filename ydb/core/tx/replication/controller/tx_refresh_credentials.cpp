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
            || !CanRefreshSecret(replication->GetAuthenticationErrorRevision(), result.Revision)
            || Self->ReplicationsWithSourceOffsetMismatch.contains(result.ReplicationId))
        {
            return true;
        }
        // The refresh actor verified the required source streams with a fresh
        // provider. Only auth-only Error retains the marker; generic state writes
        // and explicit lifecycle/config changes atomically clear it.
        Replication = replication;
        Replication->ResetCredentials(ctx);
        Replication->UpdateSecret(result.Value, result.Revision, ToString(Event->Sender));
        NIceDb::TNiceDb db(txc.DB);
        for (auto* target : Replication->GetTargets()) {
            target->Shutdown(ctx);
            if (result.MissingIndexStreams.contains(target->GetId())
                && target->GetDstState() != TReplication::EDstState::Removing)
            {
                // The source confirmed the GUID is gone, but may have retained
                // the original index. Keep this target in the GLOBAL quorum
                // until an authoritative base-table schema record retires it.
                target->SetStreamState(TReplication::EStreamState::Removed);
                target->SetDstState(TReplication::EDstState::Error);
                target->SetIssue("Source index changefeed is missing");
                db.Table<Schema::SrcStreams>().Key(result.ReplicationId, target->GetId()).Update(
                    NIceDb::TUpdate<Schema::SrcStreams::State>(target->GetStreamState()));
                db.Table<Schema::Targets>().Key(result.ReplicationId, target->GetId()).Update(
                    NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
                    NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue()));
            } else if (target->GetDstState() == TReplication::EDstState::Error && !target->HasMissingIndexStream()) {
                target->SetDstState(TReplication::EDstState::Ready);
                target->SetIssue({});
                db.Table<Schema::Targets>().Key(result.ReplicationId, target->GetId()).Update(
                    NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
                    NIceDb::TUpdate<Schema::Targets::Issue>(TString()));
            }
        }
        Replication->SetState(TReplication::EState::Ready);
        db.Table<Schema::Replications>().Key(result.ReplicationId).Update(
            NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
            NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue()),
            NIceDb::TUpdate<Schema::Replications::AuthenticationErrorRevision>(Replication->GetAuthenticationErrorRevision()));
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
