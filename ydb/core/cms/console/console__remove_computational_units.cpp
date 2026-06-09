#include "console_tenants_manager.h"

namespace NKikimr::NConsole {

using namespace NOperationId;

class TTenantsManager::TTxRemoveComputationalUnits : public TTransactionBase<TTenantsManager> {
public:
    TTxRemoveComputationalUnits(TTenant::TPtr tenant, TTenantsManager *self)
        : TBase(self)
        , Tenant(tenant)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());

        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxRemoveComputationalUnits Execute " << Tenant->Path);

        Y_ABORT_UNLESS(Tenant->State == TTenant::REMOVING_SUBDOMAIN);

        Self->DbUpdateTenantState(Tenant, TTenant::REMOVING_UNITS, txc, ctx);
        Self->DbRemoveComputationalUnits(Tenant, txc, ctx);

        return true;
    }

    void Complete(const TActorContext &executorCtx) override
    {
        auto ctx = executorCtx.MakeFor(Self->SelfId());
        LOG_DEBUG_S(ctx, NKikimrServices::CMS_TENANTS,
                    "TTxRemoveComputationalUnits Complete " << Tenant->Path);

        Self->SlotStats.DeallocateSlots(Tenant->Slots);
        Self->Counters.RemoveUnits(Tenant->ComputationalUnits);
        for (auto &pr : Tenant->RegisteredComputationalUnits)
            Self->Counters.Dec(pr.second.Kind, COUNTER_REGISTERED_UNITS);

        Tenant->State = TTenant::REMOVING_UNITS;
        Tenant->RemoveComputationalUnits();

        Self->ProcessTenantActions(Tenant, ctx);

        Self->TxProcessor->TxCompleted(this, ctx);
    }

private:
    TTenant::TPtr Tenant;
};

ITransaction *TTenantsManager::CreateTxRemoveComputationalUnits(TTenant::TPtr tenant)
{
    return new TTxRemoveComputationalUnits(tenant, this);
}

} // namespace NKikimr::NConsole
