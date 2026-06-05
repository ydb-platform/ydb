#include "console_tenants_manager.h"

namespace NKikimr::NConsole {

class TTenantsManager::TTxRevertPoolState : public TTransactionBase<TTenantsManager> {
public:
    TTxRevertPoolState(TTenantsManager *self,
                       TTenant::TPtr tenant,
                       TStoragePool::TPtr pool,
                       TActorId worker)
        : TBase(self)
        , Tenant(tenant)
        , Pool(pool)
        , Worker(worker)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxRevertPoolState for pool " << Pool->Config.GetName() << " of " << Tenant->Path);

        if (Tenant != Self->GetTenant(Tenant->Path)) {
            LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                        "TTxRevertPoolState tenant " << Tenant->Path << " mismatch");
            return true;
        }

        if (!Tenant->StoragePools.contains(Pool->Kind)
            || Pool != Tenant->StoragePools.at(Pool->Kind)) {
            LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                        "TTxRevertPoolState pool " << Pool->Config.GetName() << " mismatch");
            return true;
        }

        if (Pool->Worker != Worker) {
            LOG_NOTICE_S(ctx, NKikimrServices::CMS_TENANTS,
                         "TTxRevertPoolState pool " << Pool->Config.GetName() << " worker mismatch");
            return true;
        }

        Pool->RevertRequiredGroups();
        Self->DbUpdatePool(Tenant, Pool, txc, ctx);
        Self->DbUpdateTenantGeneration(Tenant, Tenant->Generation + 1, txc, ctx);
        Update = true;
        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxRevertPoolState complete for " << Pool->Config.GetName());

        if (Update) {
            Tenant->Generation++;
            Pool->Worker = TActorId();

            if (Tenant->State == TTenant::RUNNING)
                Self->ProcessTenantActions(Tenant, ctx);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TTenant::TPtr Tenant;
    TStoragePool::TPtr Pool;
    TActorId Worker;
    bool Update = false;
};


ITransaction *TTenantsManager::CreateTxRevertPoolState(TTenant::TPtr tenant,
                                                       TStoragePool::TPtr pool,
                                                       TActorId worker)
{
    return new TTxRevertPoolState(this, tenant, pool, worker);
}

} // namespace NKikimr::NConsole
