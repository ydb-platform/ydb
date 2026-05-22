#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

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
        YDB_LOG_CTX_DEBUG(ctx, "",
            {"LogPrefix", LogPrefix},
            {"Execute", Ev->Get()->ToString()});

        const auto rid = Ev->Get()->ReplicationId;

        Replication = Self->Find(rid);
        if (!Replication) {
            YDB_LOG_CTX_WARN(ctx, "Unknown replication",
                {"LogPrefix", LogPrefix},
                {"rid", rid});
            return true;
        }

        if (Ev->Cookie != Replication->GetExpectedSecretResolverCookie()) {
            YDB_LOG_CTX_ERROR(ctx, "Unexpected cookie",
                {"LogPrefix", LogPrefix},
                {"cookie", Ev->Cookie});
            return true;
        }

        if (Ev->Get()->IsSuccess()) {
            YDB_LOG_CTX_NOTICE(ctx, "Secret resolved",
                {"LogPrefix", LogPrefix},
                {"rid", rid});
            Replication->UpdateSecret(Ev->Get()->Value);
        } else {
            YDB_LOG_CTX_ERROR(ctx, "Resolve secret error",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"error", Ev->Get()->Error});
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
        YDB_LOG_CTX_DEBUG(ctx, "Complete",
            {"LogPrefix", LogPrefix});

        if (Replication) {
            Replication->Progress(ctx);
        }
    }

}; // TTxResolveSecretResult

void TController::RunTxResolveSecretResult(TEvPrivate::TEvResolveSecretResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxResolveSecretResult(this, ev), ctx);
}

}
