#include "tenant_slot_broker_impl.h"

namespace NKikimr {
namespace NTenantSlotBroker {

class TTenantSlotBroker::TTxCheckSlotStatus : public TTransactionBase<TTenantSlotBroker> {
public:
    TTxCheckSlotStatus(TTenantSlotBroker *self, ui64 requestId, TSlot::TPtr slot)
        : TBase(self)
        , RequestId(requestId)
        , Slot(slot)
        , ReassignSlots(false)
    {
    }

    void CheckSlot(TSlot::TPtr slot, const TActorContext &ctx) {
        if (slot->LastRequestId != RequestId)
            return;

        if (!slot->IsConnected) {
            SlotsToRemove.push_back(slot);
        } else {
            LOG_WARN_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                       "Repeat timeouted request " << RequestId << " for " << slot->IdString(true));
            Self->SendConfigureSlot(slot, ctx);
        }
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                    "TTxCheckSlotStatus Execute "
                    << (Slot ? Slot->IdString() : TString("all slots")));

        if (Slot) {
            // Check slot wasn't re-created.
            auto curSlot = Self->GetSlot(Slot->Id);
            if (curSlot != Slot)
                return true;

            CheckSlot(Slot, ctx);
        } else {
            for (auto &pr : Self->Slots)
                CheckSlot(pr.second, ctx);
        }

        for (auto &slot : SlotsToRemove) {
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                        "Removing slot " << slot->IdString() << " due to connection timeout");
            if (slot->AssignedTenant)
                ReassignSlots = true;
            Self->RemoveSlot(slot, txc, ctx);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxCheckSlotStatus Complete");
        if (ReassignSlots)
            Self->ScheduleTxAssignFreeSlots(ctx);
        Self->TxCompleted(this, ctx);
    }

private:
    ui64 RequestId;
    TSlot::TPtr Slot;
    TVector<TSlot::TPtr> SlotsToRemove;
    bool ReassignSlots;
};

ITransaction *TTenantSlotBroker::CreateTxCheckSlotStatus(ui64 requestId, TSlot::TPtr slot)
{
    return new TTxCheckSlotStatus(this, requestId, slot);
}

} // NTenantSlotBroker
} // NKikimr
