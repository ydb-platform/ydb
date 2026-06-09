#include "console_tenants_manager.h"

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
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxUpdateConfirmedSubdomain for tenant " << Path << " to " << Version);

        Tenant = Self->GetTenant(Path);
        if (!Tenant) {
            LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                        "TTxUpdateConfirmedSubdomain cannot find tenant " << Path);
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
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxUpdateConfirmedSubdomain complete for " << Path);

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
