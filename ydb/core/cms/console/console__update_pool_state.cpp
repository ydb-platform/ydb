#include "console_tenants_manager.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::CMS_TENANTS

namespace NKikimr::NConsole {

class TTenantsManager::TTxUpdatePoolState : public TTransactionBase<TTenantsManager> {
public:
    TTxUpdatePoolState(TTenantsManager *self,
                       TTenant::TPtr tenant,
                       TStoragePool::TPtr pool,
                       TActorId worker,
                       TStoragePool::EState state)
        : TBase(self)
        , Tenant(tenant)
        , Pool(pool)
        , Worker(worker)
        , State(state)
        , SubdomainVersion(Tenant->SubdomainVersion)
        , AllocatedNumGroups(pool->AllocatedNumGroups)
        , Update(false)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdatePoolState for pool of",
            {"GetName", Pool->Config.GetName()},
            {"Path", Tenant->Path},
            {"state", State});

        if (Tenant != Self->GetTenant(Tenant->Path)) {
            YDB_LOG_CTX_ERROR(ctx, "TTxUpdatePoolState tenant mismatch",
                {"Path", Tenant->Path});
            return true;
        }

        if (!Tenant->StoragePools.contains(Pool->Kind)
            || Pool != Tenant->StoragePools.at(Pool->Kind)) {
            YDB_LOG_CTX_ERROR(ctx, "TTxUpdatePoolState pool mismatch",
                {"GetName", Pool->Config.GetName()});
            return true;
        }

        if (Pool->Worker != Worker) {
            YDB_LOG_CTX_NOTICE(ctx, "TTxUpdatePoolState pool worker mismatch",
                {"GetName", Pool->Config.GetName()});
            return true;
        }

        if (State == TStoragePool::ALLOCATED)
            AllocatedNumGroups = Pool->Config.GetNumGroups();
        else if (State == TStoragePool::DELETED)
            AllocatedNumGroups = 0;

        Self->DbUpdatePoolState(Tenant, Pool, State, AllocatedNumGroups, txc, ctx);

        // If new pool was added then subdomain version should be incremented.
        if (Pool->State == TStoragePool::NOT_ALLOCATED && AllocatedNumGroups > 0 ||
            Pool->State == TStoragePool::NOT_UPDATED && AllocatedNumGroups > 0 && Pool->AllocatedNumGroups == 0)
        {
            ++SubdomainVersion;
            Self->DbUpdateSubdomainVersion(Tenant, SubdomainVersion, txc, ctx);
        }

        Update = true;

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdatePoolState complete for",
            {"GetName", Pool->Config.GetName()});

        if (Update) {
            Self->Counters.Inc(Pool->Kind, COUNTER_ALLOCATED_STORAGE_UNITS,
                               AllocatedNumGroups - Pool->AllocatedNumGroups);

            Pool->Worker = TActorId();
            Pool->State = State;
            Pool->AllocatedNumGroups = AllocatedNumGroups;
            Tenant->SubdomainVersion = SubdomainVersion;

            if (Tenant->State == TTenant::CREATING_POOLS && !Tenant->HasPoolsToCreate())
                Self->TxProcessor->ProcessTx(Self->CreateTxUpdateTenantState(Tenant->Path, TTenant::CREATING_SUBDOMAIN), ctx);
            else if (Tenant->State == TTenant::REMOVING_POOLS && !Tenant->HasPoolsToDelete())
                Self->TxProcessor->ProcessTx(Self->CreateTxRemoveTenantDone(Tenant), ctx);
            else if (Tenant->State == TTenant::RUNNING)
                Self->ProcessTenantActions(Tenant, ctx);
        }

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TTenant::TPtr Tenant;
    TStoragePool::TPtr Pool;
    TActorId Worker;
    TStoragePool::EState State;
    ui64 SubdomainVersion;
    ui64 AllocatedNumGroups;
    bool Update;
};

ITransaction *TTenantsManager::CreateTxUpdatePoolState(TTenant::TPtr tenant,
                                                       TStoragePool::TPtr pool,
                                                       TActorId worker,
                                                       TStoragePool::EState state)
{
    return new TTxUpdatePoolState(this, tenant, pool, worker, state);
}

} // namespace NKikimr::NConsole
