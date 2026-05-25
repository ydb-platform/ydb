#include "console_tenants_manager.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_TENANTS

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
        YDB_LOG_CTX_DEBUG(ctx, "TTxRevertPoolState for pool of",
            {"GetName", Pool->Config.GetName()},
            {"Path", Tenant->Path});

        if (Tenant != Self->GetTenant(Tenant->Path)) {
            YDB_LOG_CTX_ERROR(ctx, "TTxRevertPoolState tenant mismatch",
                {"Path", Tenant->Path});
            return true;
        }

        if (!Tenant->StoragePools.contains(Pool->Kind)
            || Pool != Tenant->StoragePools.at(Pool->Kind)) {
            YDB_LOG_CTX_ERROR(ctx, "TTxRevertPoolState pool mismatch",
                {"GetName", Pool->Config.GetName()});
            return true;
        }

        if (Pool->Worker != Worker) {
            YDB_LOG_CTX_NOTICE(ctx, "TTxRevertPoolState pool worker mismatch",
                {"GetName", Pool->Config.GetName()});
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
        YDB_LOG_CTX_DEBUG(ctx, "TTxRevertPoolState complete for",
            {"GetName", Pool->Config.GetName()});

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
