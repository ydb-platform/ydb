#include "tenant_slot_broker_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TENANT_SLOT_BROKER

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
            YDB_LOG_WARN_CTX(ctx, "Repeat timeouted request",
                {"requestId", RequestId},
                {"slotId", slot->IdString(true)});
            Self->SendConfigureSlot(slot, ctx);
        }
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TTxCheckSlotStatus Execute",
            {"slotFilter", (Slot ? Slot->IdString() : TString("all slots"))});

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
            YDB_LOG_DEBUG_CTX(ctx, "Removing slot due to connection timeout",
                {"slotIdString", slot->IdString()});
            if (slot->AssignedTenant)
                ReassignSlots = true;
            Self->RemoveSlot(slot, txc, ctx);
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TTxCheckSlotStatus Complete");
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
