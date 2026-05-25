#include "controller_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::REPLICATION_CONTROLLER

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
        YDB_LOG_CTX_DEBUG(ctx, "",
            {"LogPrefix", LogPrefix},
            {"Execute", Ev->Get()->ToString()});

        const auto rid = Ev->Get()->ReplicationId;
        const auto& tenant = Ev->Get()->Tenant;

        Replication = Self->Find(rid);
        if (!Replication) {
            YDB_LOG_CTX_WARN(ctx, "Cannot resolve database of unknown replication",
                {"LogPrefix", LogPrefix},
                {"rid", rid});
            return true;
        }

        Replication->SetDatabase(tenant);

        if (Ev->Get()->IsSuccess()) {
            YDB_LOG_CTX_NOTICE(ctx, "Database resolved",
                {"LogPrefix", LogPrefix},
                {"rid", rid},
                {"database", tenant});

            Self->UnresolvedDatabaseReplications.erase(Replication->GetId());
        } else {
            YDB_LOG_CTX_ERROR(ctx, "Resolve database error",
                {"LogPrefix", LogPrefix},
                {"rid", rid});
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
        YDB_LOG_CTX_DEBUG(ctx, "Complete",
            {"LogPrefix", LogPrefix});

        if (Self->UnresolvedDatabaseReplications.empty()) {
            Self->SwitchToWork(ctx);
        }
    }

}; // TTxResolveDatabaseResult

void TController::RunTxResolveDatabaseResult(TEvPrivate::TEvResolveTenantResult::TPtr& ev, const TActorContext& ctx) {
    Execute(new TTxResolveDatabaseResult(this, ev), ctx);
}

}
