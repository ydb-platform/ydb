#include "controller_impl.h"

namespace NKikimr::NReplication::NController {

class TController::TTxResolveDatabaseResult : public TTxBase {
    const TEvPrivate::TEvResolveTenantResult::TPtr Ev;
    TReplication::TPtr Replication;

public:
    explicit TTxResolveDatabaseResult(TController* self, TEvPrivate::TEvResolveTenantResult::TPtr& ev)
        : TTxBase("TxResolveDatabaseResult", self)
        , Ev(ev)
    {
    }

    TTxType GetTxType() const override {
        return TXTYPE_RESOLVE_DATABASE_RESULT;
    }

    bool Execute(TTransactionContext& txc, const TActorContext& ctx) override {
        LOG_DEBUG_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Execute: " << Ev->Get()->ToString());

        const auto rid = Ev->Get()->ReplicationId;
        const auto& tenant = Ev->Get()->Tenant;

        Replication = Self->Find(rid);
        if (!Replication) {
            LOG_WARN_S  (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Cannot resolve database of unknown replication"
                << ": rid# " << rid);
            return true;
        }

        Replication->SetDatabase(tenant);

        if (Ev->Get()->IsSuccess()) {
            LOG_NOTICE_S(ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Database resolved"
                << ": rid# " << rid
                << ", database# " << tenant);

            Self->UnresolvedDatabaseReplications.erase(Replication->GetId());
        } else {
            LOG_ERROR_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Resolve database error"
                << ": rid# " << rid);
            Y_ABORT_UNLESS(!tenant);

            auto& resolveAttempts = Self->UnresolvedDatabaseReplications[rid];
            if (resolveAttempts > 0) {
                Replication->ResolveDatabase(ctx);
                --resolveAttempts;
            } else {
                Self->UnresolvedDatabaseReplications.erase(Replication->GetId());
            }
        }

        if (tenant) {
            NIceDb::TNiceDb db(txc.DB);
            db.Table<Schema::Replications>().Key(Replication->GetId()).Update(
                NIceDb::TUpdate<Schema::Replications::Database>(tenant)
            );
        }

        return true;
    }

    void Complete(const TActorContext& ctx) override {
        LOG_DEBUG_S (ctx, NKikimrServices::REPLICATION_CONTROLLER, LogPrefix <<"Complete");

        if (Self->UnresolvedDatabaseReplications.empty()) {
            Self->SwitchToWork(ctx);
        }
    }

}; // TTxResolveDatabaseResult

void TController::RunTxResolveDatabaseResult(TEvPrivate::TEvResolveTenantResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxResolveDatabaseResult(this, ev), ctx);
}

}
