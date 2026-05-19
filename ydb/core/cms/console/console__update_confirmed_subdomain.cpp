#include "console_tenants_manager.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_TENANTS

namespace NKikimr::NConsole {

class TTenantsManager::TTxUpdateConfirmedSubdomain : public TTransactionBase<TTenantsManager> {
public:
    TTxUpdateConfirmedSubdomain(TTenantsManager *self,
                                const TString &path,
                                ui64 version,
                                TActorId worker)
        : TBase(self)
        , Path(path)
        , Version(version)
        , Worker(worker)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateConfirmedSubdomain for tenant to",
            {"Path", Path},
            {"Version", Version});

        Tenant = Self->GetTenant(Path);
        if (!Tenant) {
            YDB_LOG_CTX_ERROR(ctx, "TTxUpdateConfirmedSubdomain cannot find tenant",
                {"Path", Path});
            return true;
        }

        if (Tenant->State == TTenant::CONFIGURING_SUBDOMAIN)
            Self->DbUpdateTenantState(Tenant, TTenant::RUNNING, txc, ctx);
        Self->DbUpdateConfirmedSubdomain(Tenant, Version, txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateConfirmedSubdomain complete for",
            {"Path", Path});

        if (Tenant) {
            if (Tenant->Worker == Worker)
                Tenant->Worker = TActorId();
            Tenant->ConfirmedSubdomain = Version;
            if (Tenant->State == TTenant::CONFIGURING_SUBDOMAIN) {
                Tenant->State = TTenant::RUNNING;
                Self->SendTenantNotifications(Tenant, TTenant::CREATE,
                                              Ydb::StatusIds::SUCCESS, ctx);
            }
            Self->ProcessTenantActions(Tenant, ctx);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TTenant::TPtr Tenant;
    TString Path;
    ui64 Version;
    TActorId Worker;
};

ITransaction *TTenantsManager::CreateTxUpdateConfirmedSubdomain(const TString &path,
                                                                ui64 version,
                                                                TActorId worker)
{
    return new TTxUpdateConfirmedSubdomain(this, path, version, worker);
}

} // namespace NKikimr::NConsole
