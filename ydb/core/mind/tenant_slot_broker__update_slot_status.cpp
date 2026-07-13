#include "tenant_slot_broker_impl.h"

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TENANT_SLOT_BROKER

namespace NKikimr {
namespace NTenantSlotBroker {

class TTenantSlotBroker::TTxUpdateSlotStatus : public TTransactionBase<TTenantSlotBroker> {
public:
    TTxUpdateSlotStatus(TTenantSlotBroker *self, TEvTenantPool::TEvConfigureSlotResult::TPtr ev)
        : TBase(self)
        , Event(std::move(ev))
        , Modified(false)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto nodeId = Event->Sender.NodeId();
        auto &rec = Event->Get()->Record;

        YDB_LOG_DEBUG_CTX(ctx, "TTxUpdateSlotStatus Execute: update for node",
            {"nodeId", nodeId},
            {"record", rec});

        auto slot = Self->GetSlot(nodeId, rec.GetSlotStatus().GetId());
        if (!slot) {
            YDB_LOG_WARN_CTX(ctx, "TTxUpdateSlotStatus: update for unknown slot",
                {"nodeId", nodeId},
                {"slotId", rec.GetSlotStatus().GetId()});
            return true;
        }

        if (!slot->IsConnected) {
            YDB_LOG_WARN_CTX(ctx, "TTxUpdateSlotStatus: update for disconnected slot",
                {"slotId", slot->IdString()});
            return true;
        }

        if (Event->Cookie != slot->LastRequestId) {
            YDB_LOG_DEBUG_CTX(ctx, "TTxUpdateSlotStatus: late configure response",
                {"slotId", slot->IdString(true)},
                {"cookie", Event->Cookie},
                {"lastRequestId", slot->LastRequestId});
            return true;
        }
        slot->LastRequestId = 0;

        if (rec.GetStatus() == NKikimrTenantPool::ERROR) {
            YDB_LOG_ERROR_CTX(ctx, "TTxUpdateSlotStatus: configure error",
                {"slotId", slot->IdString(true)},
                {"error", rec.GetError()});

            Modified = slot->AssignedTenant != nullptr;
            Self->RemoveSlot(slot, txc, ctx);
            return true;
        }

        if (rec.GetStatus() == NKikimrTenantPool::NOT_OWNER) {
            YDB_LOG_ERROR_CTX(ctx, "TTxUpdateSlotStatus: cannot configure due to lack of ownership",
                {"slotId", slot->IdString(true)});

            Self->DisconnectNodeSlots(nodeId, ctx);

            YDB_LOG_DEBUG_CTX(ctx, "TTxUpdateSlotStatus: taking ownership of tenant pool on node",
                {"nodeId", nodeId});

            ctx.Send(MakeTenantPoolID(nodeId), new TEvTenantPool::TEvTakeOwnership(Self->Generation()));
            return true;
        }

        if (rec.GetStatus() == NKikimrTenantPool::UNKNOWN_SLOT) {
            YDB_LOG_ERROR_CTX(ctx, "TTxUpdateSlotStatus: slot reported as unknown",
                {"slotId", slot->IdString(true)});

            Modified = slot->AssignedTenant != nullptr;
            Self->RemoveSlot(slot, txc, ctx);
            return true;
        }

        if (rec.GetStatus() == NKikimrTenantPool::UNKNOWN_TENANT) {
            YDB_LOG_ERROR_CTX(ctx, "TTxUpdateSlotStatus: slot cannot be assigned",
                {"slotId", slot->IdString(true)},
                {"error", rec.GetError()});

            Modified = false; // prevent infinite loop (attach-detach-attach...)
            if (slot->AssignedTenant) {
                slot->AssignedTenant->GetAllocation(slot->UsedAs)->DecPending();
            }
            Self->DetachSlotNoConfigure(slot, txc);
            return true;
        }

        if (rec.GetStatus() != NKikimrTenantPool::SUCCESS) {
            YDB_LOG_ERROR_CTX(ctx, "TTxUpdateSlotStatus: unknown slot status",
                {"slotId", slot->IdString(true)},
                {"slotStatus", NKikimrTenantPool::EStatus_Name(rec.GetStatus())},
                {"error", rec.GetError()});

            Modified = slot->AssignedTenant != nullptr;
            Self->RemoveSlot(slot, txc, ctx);
            return true;
        }

        if (slot->AssignedTenant) {
            if (slot->AssignedTenant->Name != rec.GetSlotStatus().GetAssignedTenant()) {
                YDB_LOG_ERROR_CTX(ctx, "TTxUpdateSlotStatus: configure result has wrong tenant",
                    {"slotId", slot->IdString(true)});

                Self->SendConfigureSlot(slot, ctx);
            } else {
                YDB_LOG_DEBUG_CTX(ctx, "TTxUpdateSlotStatus: confirmed resource assignment",
                    {"slotId", slot->IdString(true)});

                slot->AssignedTenant->GetAllocation(slot->UsedAs)->DecPending();
            }
        } else {
            if (rec.GetSlotStatus().GetAssignedTenant()) {
                YDB_LOG_ERROR_CTX(ctx, "TTxUpdateSlotStatus: slot has wrongly assigned tenant",
                    {"slotId", slot->IdString(true)});

                Self->SendConfigureSlot(slot, ctx);
            } else if (slot->IsFree()) {
                YDB_LOG_DEBUG_CTX(ctx, "TTxUpdateSlotStatus: confirmed free slot",
                    {"slotId", slot->IdString()});

                Self->FreeSlots.Add(slot);
                Modified = true;
            } else {
                YDB_LOG_DEBUG_CTX(ctx, "TTxUpdateSlotStatus: confirmed detached slot",
                    {"slotId", slot->IdString()});
            }
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        YDB_LOG_DEBUG_CTX(ctx, "TTxUpdateSlotStatus Complete");

        if (Modified && Self->HasUnhappyTenant())
            Self->ScheduleTxAssignFreeSlots(ctx);

        Self->TxCompleted(this, ctx);
    }

private:
    TEvTenantPool::TEvConfigureSlotResult::TPtr Event;
    bool Modified;
};

ITransaction *TTenantSlotBroker::CreateTxUpdateSlotStatus(TEvTenantPool::TEvConfigureSlotResult::TPtr &ev)
{
    return new TTxUpdateSlotStatus(this, std::move(ev));
}

} // NTenantSlotBroker
} // NKikimr
