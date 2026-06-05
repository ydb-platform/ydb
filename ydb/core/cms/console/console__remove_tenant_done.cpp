#include "console_tenants_manager.h"

namespace NKikimr::NConsole {

using namespace NOperationId;

class TTenantsManager::TTxRemoveTenantDone : public TTransactionBase<TTenantsManager> {
public:
    TTxRemoveTenantDone(TTenant::TPtr tenant, TTenantsManager *self)
        : TBase(self)
        , Tenant(tenant)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        Y_ABORT_UNLESS(Tenant->State == TTenant::REMOVING_POOLS);

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxRemoveTenantDone for tenant " << Tenant->Path
                    << " txid=" << Tenant->TxId);

        Self->DbRemoveComputationalUnits(Tenant, txc, ctx);
        Self->DbRemoveTenantAndPools(Tenant, txc, ctx);
        Self->DbUpdateRemovedTenant(Tenant, Ydb::StatusIds::SUCCESS, txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG(ctx, NKikimrServices::CMS_TENANTS, "TTxRemoveTenantDone Complete");

        Self->SendTenantNotifications(Tenant, TTenant::REMOVE, Ydb::StatusIds::SUCCESS, ctx);
        Self->Counters.Inc(Ydb::StatusIds::SUCCESS, COUNTER_REMOVE_RESPONSES);

        Self->RemoveTenant(Tenant);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TTenant::TPtr Tenant;
};

ITransaction *TTenantsManager::CreateTxRemoveTenantDone(TTenant::TPtr tenant)
{
    return new TTxRemoveTenantDone(tenant, this);
}

} // namespace NKikimr::NConsole
