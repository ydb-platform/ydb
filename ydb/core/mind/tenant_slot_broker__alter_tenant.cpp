#include "tenant_slot_broker_impl.h"

namespace NKikimr {
namespace NTenantSlotBroker {

class TTenantSlotBroker::TTxAlterTenant : public TTransactionBase<TTenantSlotBroker> {
public:
    TTxAlterTenant(TTenantSlotBroker *self, TEvTenantSlotBroker::TEvAlterTenant::TPtr ev)
        : TBase(self)
        , Event(std::move(ev))
        , Modified(false)
    {
    }

    void DetachSlots(TTenant::TPtr tenant,
                     THashMap<TSlotDescription, ui64> &detach,
                     TSlotsAllocation::TPtr allocation,
                     bool onlySplit,
                     bool onlyMisplaced,
                     TTransactionContext &txc,
                     const TActorContext &ctx)
    {
        auto it = detach.find(allocation->Description);
        if (it == detach.end())
            return;

        for (auto slotIt = allocation->AssignedSlots.begin();
             slotIt != allocation->AssignedSlots.end();
             ) {
            auto slot = *slotIt;
            // Avoid iterator invalidation by slot detach.
            ++slotIt;

            if (onlySplit) {
                Y_ABORT_UNLESS(!onlyMisplaced);
                Y_ABORT_UNLESS(allocation->Group);
                if (!allocation->SplitCount)
                    return;
                if (slot->DataCenter == allocation->Group->GetPreferredDataCenter())
                    continue;
            } else if (onlyMisplaced) {
                Y_ABORT_UNLESS(!onlySplit);
                Y_ABORT_UNLESS(allocation->Description.DataCenter != ANY_DATA_CENTER);
                if (!allocation->MisplacedCount)
                    return;
                if (slot->DataCenter == allocation->Description.DataCenter)
                    continue;
            }

            // Check if slot is still OK to use by tenant.
            bool reattached = false;
            TSlotDescription key;

            // Check if slot may actually be left assigned
            // by another allocation.
            for (auto allocation : tenant->GetMissing()) {
                if (detach.contains(allocation->Description))
                    continue;
                if (allocation->IsSlotOk(slot->Type, slot->DataCenter)) {
                    reattached = true;
                    key = allocation->Description;
                    break;
                }
            }

            if (reattached) {
                Y_ABORT_UNLESS(slot->UsedAs != key);
                Self->DetachSlotNoConfigureNoDb(slot, false);
                Self->AttachSlotNoConfigure(slot, tenant, key, slot->Label, txc);
            } else {
                Self->DetachSlot(slot, txc, ctx, false);
                Modified = true;
            }

            if (it->second == 1) {
                detach.erase(it);
                return;
            } else {
                --it->second;
            }
        }
    }

    void DetachSlots(TTenant::TPtr tenant,
                     THashMap<TSlotDescription, ui64> &detach,
                     TSlotsAllocation::TPtr allocation,
                     TTransactionContext &txc,
                     const TActorContext &ctx)
    {
        if (allocation->SplitCount)
            DetachSlots(tenant, detach, allocation, true, false, txc, ctx);
        if (allocation->MisplacedCount)
            DetachSlots(tenant, detach, allocation, false, true, txc, ctx);
        DetachSlots(tenant, detach, allocation, false, false, txc, ctx);
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto &rec = Event->Get()->Record;

        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxAlterTenant "
                    << rec.ShortDebugString());

        THashMap<TSlotDescription, ui64> newSlots;
        for (auto &slot : rec.GetRequiredSlots()) {
            if (slot.GetCount() == 0)
                continue;
            TSlotDescription key(slot);
            if (newSlots.contains(key))
                newSlots[key] += slot.GetCount();
            else
                newSlots[key] = slot.GetCount();
        }

        NIceDb::TNiceDb db(txc.DB);
        auto &name = rec.GetTenantName();
        TTenant::TPtr tenant;
        auto it = Self->Tenants.find(name);
        if (it == Self->Tenants.end()) {
            // Don't create tenant with no slots.
            if (!newSlots.empty()) {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                            "Create new tenant " << name);

                tenant = Self->AddTenant(name);

                for (auto &pr : newSlots) {
                    tenant->AddSlotsAllocation(pr.first,  pr.second);
                    tenant->DbUpdateAllocation(pr.first, txc);
                }

                tenant->AddSlotLabels(tenant->GetTotalRequired(), txc);
            }

        } else {
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                        "Alter existing tenant " << name);

            tenant = it->second;
            Self->RemoveUnhappyTenant(tenant);

            auto total = tenant->GetTotalRequired();
            THashSet<TSlotDescription> oldSlots;
            for (auto &pr : tenant->GetAllocations())
                oldSlots.insert(pr.first);

            // Update required slots for tenant. Fill map
            // with slot counts to detach.
            THashMap<TSlotDescription, ui64> detach;
            for (auto &pr : newSlots) {
                auto &key = pr.first;
                ui64 newRequired = pr.second;
                auto allocation = tenant->GetAllocation(key);

                if (allocation) {
                    auto required = allocation->RequiredCount;
                    auto missing = allocation->MissingCount;

                    if (required > newRequired && missing < required - newRequired)
                        detach[key] = required - newRequired - missing;

                    allocation->SetRequired(newRequired);
                    // It's OK to have negative value here. It will be back
                    // to normal after extra slots are detached.
                    allocation->IncMissing(newRequired - required);
                } else {
                    tenant->AddSlotsAllocation(key, newRequired);
                }

                tenant->DbUpdateAllocation(key, txc);
            }

            // Handle required slots not mentioned in new tenant config.
            for (auto &key : oldSlots) {
                if (newSlots.contains(key))
                    continue;

                auto allocation = tenant->GetAllocation(key);
                ui64 connected = allocation->RequiredCount - allocation->MissingCount;
                if (connected)
                    detach[key] = connected;

                allocation->SetRequired(0);
                allocation->SetMissing(-connected);
                tenant->DbUpdateAllocation(key, txc);
            }

            for (auto &pr : tenant->GetAllocations())
                if (detach.contains(pr.first))
                    DetachSlots(tenant, detach, pr.second, txc, ctx);

            Y_ABORT_UNLESS(detach.empty());

            // Add/remove sot labels for tenant.
            if (tenant->GetTotalRequired() > total)
                tenant->AddSlotLabels(tenant->GetTotalRequired() - total, txc);
            else
                tenant->RemoveSlotLabels(total - tenant->GetTotalRequired(), txc);

            tenant->ClearEmptyAllocations();
            Self->MaybeRemoveTenant(tenant);
        }

        if (tenant) {
            if (tenant->GetTotalMissing())
                Self->AssignFreeSlots(tenant, false, txc, ctx);
            Self->AddUnhappyTenant(tenant);
        }

        TenantState = MakeHolder<TEvTenantSlotBroker::TEvTenantState>();
        Self->FillTenantState(name, TenantState->Record);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        auto &name = Event->Get()->Record.GetTenantName();
        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxAlterTenant "
                    << name << " complete");

        ctx.Send(Event->Sender, TenantState.Release());

        if (Modified && Self->HasUnhappyTenant())
            Self->ScheduleTxAssignFreeSlots(ctx);

        Self->TxCompleted(this, ctx);
    }

private:
    TEvTenantSlotBroker::TEvAlterTenant::TPtr Event;
    THolder<TEvTenantSlotBroker::TEvTenantState> TenantState;
    bool Modified;
};

ITransaction *TTenantSlotBroker::CreateTxAlterTenant(TEvTenantSlotBroker::TEvAlterTenant::TPtr &ev)
{
    return new TTxAlterTenant(this, std::move(ev));
}

} // NTenantSlotBroker
} // NKikimr
