#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

namespace NKikimr::NReplication::NController {

class TController::TTxResolveResourceIdResult: public TTxBase {
    TEvPrivate::TEvResolveResourceIdResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxResolveResourceIdResult(TController* self, TEvPrivate::TEvResolveResourceIdResult::TPtr& ev)
        : TTxBase("TxResolveResourceIdResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_RESOLVE_RESOURCE_ID_RESULT;
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

        NIceDb::TNiceDb db(txc.DB);
        if (Ev->Get()->IsSuccess()) {
            YDB_LOG_CTX_NOTICE(ctx, "Resource id resolved",
                {"LogPrefix", LogPrefix},
                {"rid", rid});
            Replication->UpdateResourceId(Ev->Get()->Value);

            db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
                NIceDb::TUpdate<Schema::Replications::Config>(Replication->GetConfig().SerializeAsString())
            );
        } else {
            YDB_LOG_CTX_ERROR(ctx, "Resolve resource id error",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"error", Ev->Get()->Error});
            Replication->SetState(TReplication::EState::Error, Ev->Get()->Error);

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

}; // TTxResolveResourceIdResult

void TController::RunTxResolveResourceIdResult(TEvPrivate::TEvResolveResourceIdResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxResolveResourceIdResult(this, ev), ctx);
}

}
