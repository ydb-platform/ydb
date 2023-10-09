#include "console_tenants_manager.h"

namespace NKikimr::NConsole {

using namespace NOperationId;

class TTenantsManager::TTxRemoveTenantFailed : public TTransactionBase<TTenantsManager> {
public:
    TTxRemoveTenantFailed(TTenant::TPtr tenant,
                          Ydb::StatusIds::StatusCode code,
                          TTenantsManager *self)
        : TBase(self)
        , Tenant(tenant)
        , Code(code)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        // We can cancel tenant removal only if the first stage
        // (subdomain removal) fails.
        Y_ABORT_UNLESS(Tenant->State == TTenant::REMOVING_SUBDOMAIN);

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxRemoveTenantFailed for tenant " << Tenant->Path
                    << " txid=" << Tenant->TxId);

        Self->DbUpdateTenantState(Tenant, TTenant::RUNNING, txc, ctx);
        Self->DbUpdateRemovedTenant(Tenant, Code, txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_TENANTS, "TTxRemoveTenantFailed Complete");

        Self->SendTenantNotifications(Tenant, TTenant::REMOVE, Code, ctx);
        Self->Counters.Inc(Ydb::StatusIds::SUCCESS, COUNTER_REMOVE_RESPONSES);

        Tenant->State = TTenant::RUNNING;

        Self->RemoveTenantFailed(Tenant, Code);
        Self->ProcessTenantActions(Tenant, ctx);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TTenant::TPtr Tenant;
    Ydb::StatusIds::StatusCode Code;
};

ITransaction *TTenantsManager::CreateTxRemoveTenantFailed(TTenant::TPtr tenant,
                                                          Ydb::StatusIds::StatusCode code)
{
    return new TTxRemoveTenantFailed(tenant, code, this);
}

} // namespace NKikimr::NConsole
