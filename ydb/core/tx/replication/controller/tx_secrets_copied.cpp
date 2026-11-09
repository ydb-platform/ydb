#include "controller_impl.h"
#include "target_secret.h"

namespace NKikimr::NReplication::NController {

class TController::TTxPrepareSecretCreate: public TTxBase {
    TEvPrivate::TEvPrepareSecretCreate::TPtr Ev;
    TReplication::TPtr Replication;
    ui64 TxId = 0;

public:
    TTxPrepareSecretCreate(TController* self, TEvPrivate::TEvPrepareSecretCreate::TPtr ev)
        : TTxBase("TxPrepareSecretCreate", self)
        , Ev(std::move(ev))
    {}

    TTxType GetTxType() const override { return TXTYPE_PREPARE_SECRET_CREATE; }

    bool Execute(TTransactionContext& txc, const TActorContext&) override {
        Replication = Self->Find(Ev->Get()->ReplicationId);
        if (!Replication || !Replication->IsExpectedSecretCopier(Ev->Sender)) {
            Replication.Reset();
            return true;
        }
        auto* target = Replication->FindTarget(Ev->Get()->TargetId);
        if (!target || target->GetKind() != TReplication::ETargetKind::Secret) {
            Replication.Reset();
            return true;
        }
        auto* secret = static_cast<TTargetSecret*>(target);
        TxId = secret->GetCreateTxId();
        if (!TxId) {
            TxId = Ev->Get()->TxId;
            Y_ABORT_UNLESS(TxId);
            secret->SetCreateTxId(TxId);
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Targets>().Key(Replication->GetId(), target->GetId()).Update(
                NIceDb::TUpdate<Schema::Targets::SecretCreateTxId>(TxId));
        }
        return true;
    }

    void Complete(const TActorContext& ctx) override {
        if (Replication && Replication->IsExpectedSecretCopier(Ev->Sender)) {
            ctx.Send(Ev->Sender, new TEvPrivate::TEvSecretCreatePrepared(TxId), 0, Ev->Get()->TargetId);
        }
    }
};

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
            auto* secret = static_cast<TTargetSecret*>(target);
            if (created != Ev->Get()->Created.end()) {
                target->SetDstPathId(created->second);
                target->SetIssue({});
                if (target->GetDstState() != TReplication::EDstState::Removing) {
                    target->SetDstState(TReplication::EDstState::Done);
                }
            }
            if (created != Ev->Get()->Created.end() || Ev->Get()->Rejected.contains(target->GetId())) {
                secret->SetCreateTxId(0);
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
                NIceDb::TUpdate<Schema::Targets::SecretCreateTxId>(secret->GetCreateTxId()),
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

void TController::Handle(TEvPrivate::TEvPrepareSecretCreate::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxPrepareSecretCreate(this, ev), ctx);
}

void TController::RunTxSecretsCopied(TEvPrivate::TEvSecretsCopied::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxSecretsCopied(this, ev), ctx);
}

}
