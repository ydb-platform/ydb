#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxSecretsCopied: public TTxBase {
    TEvPrivate::TEvSecretsCopied::TPtr Ev;
    TReplication::TPtr Replication;

public:
    TTxSecretsCopied(TController* self, TEvPrivate::TEvSecretsCopied::TPtr ev)
        : TTxBase("TxSecretsCopied", self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_SECRETS_COPIED; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Replication = Self->Find(Ev->Get()->ReplicationId);
        if (!Replication || !Replication->IsExpectedSecretCopier(Ev->Sender)) {
            Replication.Reset();
            return true;
        }
        const bool dropping = Replication->GetState() == TReplication::EState::Removing;
        NIceDb::TNiceDb db(txc.DB);
        for (auto* target : Replication->GetTargets()) {
            if (target->GetKind() != TReplication::ETargetKind::Secret) {
                continue;
            }
            const auto created = Ev->Get()->Created.find(target->GetId());
            if (created != Ev->Get()->Created.end()) {
                target->SetDstPathId(created->second);
                if (target->GetDstState() != TReplication::EDstState::Removing) {
                    target->SetDstState(TReplication::EDstState::Done);
                }
            }
            if (!Ev->Get()->Status.IsSuccess() && !dropping
                && target->GetDstState() != TReplication::EDstState::Done)
            {
                target->SetDstState(TReplication::EDstState::Error);
                target->SetIssue(Ev->Get()->Status.GetIssues().ToOneLineString());
            }
            db.Table<Schema::Targets>().Key(Replication->GetId(), target->GetId()).Update(
                NIceDb::TUpdate<Schema::Targets::DstState>(target->GetDstState()),
                NIceDb::TUpdate<Schema::Targets::DstPathOwnerId>(target->GetDstPathId().OwnerId),
                NIceDb::TUpdate<Schema::Targets::DstPathLocalId>(target->GetDstPathId().LocalPathId),
                NIceDb::TUpdate<Schema::Targets::Issue>(target->GetIssue()));
            db.Table<Schema::SrcStreams>().Key(Replication->GetId(), target->GetId()).Update(
                NIceDb::TUpdate<Schema::SrcStreams::State>(target->GetStreamState()));
        }
        if (!Ev->Get()->Status.IsSuccess() && !dropping) {
            Replication->SetState(TReplication::EState::Error, Ev->Get()->Status.GetIssues().ToOneLineString());
            db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
                NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
                NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue()));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Replication && Replication->IsExpectedSecretCopier(Ev->Sender)) {
            if (Ev->Get()->Finished) {
                Replication->FinishSecretCopy(ctx);
                Replication->Progress(ctx);
            } else {
                ctx.Send(Ev->Sender, new TEvPrivate::TEvSecretsCopiedSaved(), 0, Ev->Cookie);
            }
        }
    }
};

void TController::Handle(TEvPrivate::TEvSecretsCopied::TPtr& ev, const TActorContext& ctx) {
    RunTxSecretsCopied(ev, ctx);
}

void TController::RunTxSecretsCopied(TEvPrivate::TEvSecretsCopied::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSecretsCopied(this, ev), ctx);
}

}
