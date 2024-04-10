#include "tenant_slot_broker_impl.h"

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

        LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxUpdateSlotStatus for node "
                    << nodeId << ": " << rec.ShortDebugString());

        auto slot = Self->GetSlot(nodeId, rec.GetSlotStatus().GetId());
        if (!slot) {
            LOG_WARN_S(ctx, NKikimrServices::TENANT_SLOT_BROKER, "update for unknown slot <"
                       << nodeId << ", " << rec.GetSlotStatus().GetId() << ">");
            return true;
        }

        if (!slot->IsConnected) {
            LOG_WARN_S(ctx, NKikimrServices::TENANT_SLOT_BROKER, "update for disconnected slot "
                       << slot->IdString());
            return true;
        }

        if (Event->Cookie != slot->LastRequestId) {
            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER, "late response for "
                        << slot->IdString(true) << " " << Event->Cookie << ":" << slot->LastRequestId);
            return true;
        }
        slot->LastRequestId = 0;

        if (rec.GetStatus() == NKikimrTenantPool::ERROR) {
            LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER, "configure error for "
                        << slot->IdString(true) << ": " << rec.GetError());

            Modified = slot->AssignedTenant != nullptr;
            Self->RemoveSlot(slot, txc, ctx);
            return true;
        }

        if (rec.GetStatus() == NKikimrTenantPool::NOT_OWNER) {
            LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                        "cannot configure " << slot->IdString(true) << " because of lack of ownership");

            Self->DisconnectNodeSlots(nodeId, ctx);

            LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                        "Taking ownership of tenant pool on node " << nodeId);

            ctx.Send(MakeTenantPoolID(nodeId), new TEvTenantPool::TEvTakeOwnership(Self->Generation()));
            return true;
        }

        if (rec.GetStatus() == NKikimrTenantPool::UNKNOWN_SLOT) {
            LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER, slot->IdString(true)
                        << " is reported to be unknown");

            Modified = slot->AssignedTenant != nullptr;
            Self->RemoveSlot(slot, txc, ctx);
            return true;
        }

        if (rec.GetStatus() == NKikimrTenantPool::UNKNOWN_TENANT) {
            LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER, slot->IdString(true)
                        << " cannot be assigned to tenant: " << rec.GetError());

            Modified = false; // prevent infinite loop (attach-detach-attach...)
            if (slot->AssignedTenant) {
                slot->AssignedTenant->GetAllocation(slot->UsedAs)->DecPending();
            }
            Self->DetachSlotNoConfigure(slot, txc);
            return true;
        }

        if (rec.GetStatus() != NKikimrTenantPool::SUCCESS) {
            LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                        "Unknown status for " << slot->IdString(true) << ": "
                        << NKikimrTenantPool::EStatus_Name(rec.GetStatus()) << " " << rec.GetError());

            Modified = slot->AssignedTenant != nullptr;
            Self->RemoveSlot(slot, txc, ctx);
            return true;
        }

        if (slot->AssignedTenant) {
            if (slot->AssignedTenant->Name != rec.GetSlotStatus().GetAssignedTenant()) {
                LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                            "configure result for " << slot->IdString(true) << " has wrong tenant");

                Self->SendConfigureSlot(slot, ctx);
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                            "confirmed resource assignment for " << slot->IdString(true));

                slot->AssignedTenant->GetAllocation(slot->UsedAs)->DecPending();
            }
        } else {
            if (rec.GetSlotStatus().GetAssignedTenant()) {
                LOG_ERROR_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                            slot->IdString(true) << " has wrongly assigned tenant");

                Self->SendConfigureSlot(slot, ctx);
            } else if (slot->IsFree()) {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                            "confirmed free slot " << slot->IdString());

                Self->FreeSlots.Add(slot);
                Modified = true;
            } else {
                LOG_DEBUG_S(ctx, NKikimrServices::TENANT_SLOT_BROKER,
                            "confirmed detached slot " << slot->IdString());
            }
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxUpdateSlotStatus Complete");

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
