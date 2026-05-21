#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxResolveSecretResult: public TTxBase {
    TEvPrivate::TEvResolveSecretResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxResolveSecretResult(TController* self, TEvPrivate::TEvResolveSecretResult::TPtr& ev)
        : TTxBase("TxResolveSecretResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_RESOLVE_SECRET_RESULT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Execute: " << Ev->Get()->ToString());

        const auto rid = Ev->Get()->ReplicationId;

        Replication = Self->Find(rid);
        if (!Replication) {
            LOG_WARN_S  (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Unknown replication"
                << ": rid# " << rid);
            return true;
        }

        if (Ev->Cookie != Replication->GetExpectedSecretResolverCookie()) {
            LOG_ERROR_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Unexpected cookie"
                << ": cookie# " << Ev->Cookie);
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            LOG_NOTICE_S(ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Secret resolved"
                << ": rid# " << rid);
            Replication->UpdateSecret(Ev->Get()->Value);
        } else {
            LOG_ERROR_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Resolve secret error"
                << ": rid# " << rid
                << ", error# " << Ev->Get()->Error);
            Replication->SetState(TReplication::EState::Error, Ev->Get()->Error);

            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
                NIceDb::TUpdate<Schema::Replications::State>(Replication->GetState()),
                NIceDb::TUpdate<Schema::Replications::Issue>(Replication->GetIssue())
            );
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Complete");

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxResolveSecretResult

void TController::RunTxResolveSecretResult(TEvPrivate::TEvResolveSecretResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxResolveSecretResult(this, ev), ctx);
}

}
