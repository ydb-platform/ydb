#include "tenant_slot_broker_impl.h"

namespace NKikimr {
namespace NTenantSlotBroker {

class TTenantSlotBroker::TTxAssignFreeSlots : public TTransactionBase<TTenantSlotBroker> {
public:
    TTxAssignFreeSlots(TTenantSlotBroker *self)
        : TBase(self)
    {
    }

    void AssignMissingSlots(TTransactionContext &txc,
                            const TActorContext &ctx)
    {
        auto prev = Self->UnhappyTenants.end();
        while (!Self->UnhappyTenants.empty()
               && !Self->FreeSlots.FreeSlotsByDataCenter.empty()) {
            auto cur = prev;
            if (cur == Self->UnhappyTenants.end())
                cur = Self->UnhappyTenants.begin();
            else
                ++cur;

            if (cur == Self->UnhappyTenants.end())
                break;

            auto tenant = *cur;

            Self->RemoveUnhappyTenant(tenant);
            auto assigned = Self->AssignFreeSlots(tenant, true, txc, ctx);
            Self->AddUnhappyTenant(tenant);

            if (!assigned) {
                if (prev == Self->UnhappyTenants.end()) {
                    prev = Self->UnhappyTenants.begin();
                    Y_ABORT_UNLESS(*prev == tenant);
                } else {
                    ++prev;
                }
            }
        }
    }

    void AssignSplitSlots(THashSet<TCollocationGroup::TPtr> &visitedGroups,
                          TTransactionContext &txc,
                          const TActorContext &ctx)
    {
        auto prev = Self->SplitTenants.end();
        while (!Self->SplitTenants.empty()
               && !Self->FreeSlots.FreeSlotsByDataCenter.empty()) {
            auto cur = prev;
            if (cur == Self->SplitTenants.end())
                cur = Self->SplitTenants.begin();
            else
                ++cur;

            if (cur == Self->SplitTenants.end())
                break;

            auto tenant = *cur;

            bool assigned = false;
            for (auto it = tenant->GetSplit().begin();
                 !assigned && it != tenant->GetSplit().end();
                 ) {
                auto allocation = *(it++);

                Y_ABORT_UNLESS(allocation->Group);
                if (visitedGroups.contains(allocation->Group))
                    continue;

                visitedGroups.insert(allocation->Group);

                Self->RemoveUnhappyTenant(tenant);
                assigned = Self->AssignFreeSlotsForGroup(tenant, allocation->Group, txc, ctx);
                Self->AddUnhappyTenant(tenant);
            }

            if (!assigned) {
                if (prev == Self->SplitTenants.end()) {
                    prev = Self->SplitTenants.begin();
                    Y_ABORT_UNLESS(*prev == tenant);
                } else {
                    ++prev;
                }
            }
        }
    }

    void AssignMisplacedSlots(THashSet<TCollocationGroup::TPtr> &visitedGroups,
                              TTransactionContext &txc,
                              const TActorContext &ctx)
    {
        auto prev = Self->MisplacedTenants.end();
        while (!Self->MisplacedTenants.empty()
               && !Self->FreeSlots.FreeSlotsByDataCenter.empty()) {
            auto cur = prev;
            if (cur == Self->MisplacedTenants.end())
                cur = Self->MisplacedTenants.begin();
            else
                ++cur;

            if (cur == Self->MisplacedTenants.end())
                break;

            auto tenant = *cur;

            bool assigned = false;
            for (auto it = tenant->GetMisplaced().begin();
                 !assigned && it != tenant->GetMisplaced().end();
                 ) {
                auto allocation = *(it++);

                if (allocation->Group && visitedGroups.contains(allocation->Group))
                    continue;

                Self->RemoveUnhappyTenant(tenant);
                if (allocation->Group) {
                    assigned = Self->AssignFreeSlotsForGroup(tenant, allocation->Group, txc, ctx);
                    visitedGroups.insert(allocation->Group);
                } else {
                    assigned = Self->MoveMisplacedSlots(tenant, allocation, false, txc, ctx);
                }
                Self->AddUnhappyTenant(tenant);
            }

            if (!assigned) {
                if (prev == Self->MisplacedTenants.end()) {
                    prev = Self->MisplacedTenants.begin();
                    Y_ABORT_UNLESS(*prev == tenant);
                } else {
                    ++prev;
                }
            }
        }
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxAssignFreeSlots Execute");

        // Remember which collocation groups are going to be visited during
        // missing slots processing. Don't revisit them later for split and
        // misplaced slots.
        THashSet<TCollocationGroup::TPtr> visitedGroups;
        for (auto tenant : Self->UnhappyTenants) {
            for (auto allocation : tenant->GetMissing()) {
                if (allocation->Group)
                    visitedGroups.insert(allocation->Group);
            }
        }

        // First process tenants with missing slots.
        AssignMissingSlots(txc, ctx);

        // Now try to find better layout for collocation groups which don't have
        // missing slot but have split ones.
        AssignSplitSlots(visitedGroups, txc, ctx);

        // Finally try to move some misplaced slots to better positions.
        AssignMisplacedSlots(visitedGroups, txc, ctx);

        Y_ABORT_UNLESS(Self->PendingAssignFreeSlots);
        Self->PendingAssignFreeSlots = false;

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxAssignFreeSlots Complete");
        Self->TxCompleted(this, ctx);
    }

private:
};

ITransaction *TTenantSlotBroker::CreateTxAssignFreeSlots()
{
    return new TTxAssignFreeSlots(this);
}

} // NTenantSlotBroker
} // NKikimr
