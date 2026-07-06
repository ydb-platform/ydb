#include "console_tenants_manager.h"

namespace NKikimr::NConsole {

class TTenantsManager::TTxDecommitGroups : public TTransactionBase<TTenantsManager> {
public:
    TTxDecommitGroups(TTenantsManager *self,
                      TTenant::TPtr tenant,
                      TStoragePool::TPtr pool,
                      TActorId worker,
                      TVector<ui32> groups)
        : TBase(self)
        , Tenant(tenant)
        , Pool(pool)
        , Worker(worker)
        , Groups(std::move(groups))
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());

        if (Tenant != Self->GetTenant(Tenant->Path)) {
            LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                        "TTxDecommitGroups tenant " << Tenant->Path << " mismatch");
            return true;
        }

        if (!Tenant->StoragePools.contains(Pool->Kind)
            || Pool != Tenant->StoragePools.at(Pool->Kind)) {
            LOG_ERROR_S(ctx, NKikimrServices::CMS_TENANTS,
                        "TTxDecommitGroups pool " << Pool->Config.GetName() << " mismatch");
            return true;
        }

        if (Pool->Worker != Worker) {
            LOG_NOTICE_S(ctx, NKikimrServices::CMS_TENANTS,
                         "TTxDecommitGroups pool " << Pool->Config.GetName() << " worker mismatch");
            return true;
        }

        Update = true;
        NIceDb::TNiceDb db(txc.DB);
        auto now = ctx.Now();
        size_t decommitNumGroups = 0;
        for (auto group : Groups) {
            if (Self->DecommittedGroups.insert(group).second) {
                db.Table<Schema::DecommittedGroups>().Key(group).Update<Schema::DecommittedGroups::DecommitTime>(now.MilliSeconds());
                ++decommitNumGroups;
            }
        }

        AllocatedNumGroups = (Pool->AllocatedNumGroups > decommitNumGroups)
                             ? Pool->AllocatedNumGroups - decommitNumGroups
                             : 0;
        State = Pool->State;
        if (State == TStoragePool::SHRINKING && AllocatedNumGroups == Pool->Config.GetNumGroups()) {
            State = TStoragePool::ALLOCATED;
        }
        Self->DbUpdatePoolState(Tenant, Pool, State, AllocatedNumGroups, txc, ctx);
        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxDecommitGroups complete for " << Pool->Config.GetName());

        if (Update) {
            Self->Counters.Dec(Pool->Kind, COUNTER_ALLOCATED_STORAGE_UNITS,
                               Pool->AllocatedNumGroups - AllocatedNumGroups);

            Pool->Worker = TActorId();
            Pool->State = State;
            Pool->AllocatedNumGroups = AllocatedNumGroups;
        }

        Self->ProcessTenantActions(Tenant, ctx);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TTenant::TPtr Tenant;
    TStoragePool::TPtr Pool;
    TActorId Worker;
    TVector<ui32> Groups;
    TStoragePool::EState State;
    ui64 AllocatedNumGroups;
    bool Update = false;
};

ITransaction *TTenantsManager::CreateTxDecommitGroups(TTenant::TPtr tenant,
                                                      TStoragePool::TPtr pool,
                                                      TActorId worker,
                                                      TVector<ui32> groups)
{
    return new TTxDecommitGroups(this, tenant, pool, worker, std::move(groups));
}

} // namespace NKikimr::NConsole
