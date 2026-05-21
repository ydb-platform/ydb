#include "controller_impl.h"

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
        LOG_DEBUG_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Execute: " << Ev->Get()->ToString());

        const auto rid = Ev->Get()->ReplicationId;

        Replication = Self->Find(rid);
        if (!Replication) {
            LOG_WARN_S  (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Unknown replication"
                << ": rid# " << rid);
            return true;
        }

        NIceDb::TNiceDb db(txc.DB);
        if (Ev->Get()->IsSuccess()) {
            LOG_NOTICE_S(ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Resource id resolved"
                << ": rid# " << rid);
            Replication->UpdateResourceId(Ev->Get()->Value);

            db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
                NIceDb::TUpdate<Schema::Replications::Config>(Replication->GetConfig().SerializeAsString())
            );
        } else {
            LOG_ERROR_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Resolve resource id error"
                << ": rid# " << rid
                << ", error# " << Ev->Get()->Error);
            Replication->SetState(TReplication::EState::Error, Ev->Get()->Error);

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

}; // TTxResolveResourceIdResult

void TController::RunTxResolveResourceIdResult(TEvPrivate::TEvResolveResourceIdResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxResolveResourceIdResult(this, ev), ctx);
}

}
