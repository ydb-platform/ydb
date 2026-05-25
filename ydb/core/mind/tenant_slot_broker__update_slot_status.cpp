#include "tenant_slot_broker_impl.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>

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

        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdateSlotStatus for node",
            {"nodeId", nodeId},
            {"ShortDebugString", rec.ShortDebugString()});

        auto slot = Self->GetSlot(nodeId, rec.GetSlotStatus().GetId());
        if (!slot) {
            YDB_LOG_CTX_WARN(ctx, "update for unknown slot < >",
                {"nodeId", nodeId},
                {"GetId", rec.GetSlotStatus().GetId()});
            return true;
        }

        if (!slot->IsConnected) {
            YDB_LOG_CTX_WARN(ctx, "update for disconnected slot",
                {"IdString", slot->IdString()});
            return true;
        }

        if (Event->Cookie != slot->LastRequestId) {
            YDB_LOG_CTX_DEBUG(ctx, "late response for",
                {"#_slot->IdString(true)", slot->IdString(true)},
                {"Cookie", Event->Cookie},
                {"LastRequestId", slot->LastRequestId});
            return true;
        }
        slot->LastRequestId = 0;

        if (rec.GetStatus() == NKikimrTenantPool::ERROR) {
            YDB_LOG_CTX_ERROR(ctx, "configure error for",
                {"#_slot->IdString(true)", slot->IdString(true)},
                {"GetError", rec.GetError()});

            Modified = slot->AssignedTenant != nullptr;
            Self->RemoveSlot(slot, txc, ctx);
            return true;
        }

        if (rec.GetStatus() == NKikimrTenantPool::NOT_OWNER) {
            YDB_LOG_CTX_ERROR(ctx, "cannot configure because of lack of ownership",
                {"#_slot->IdString(true)", slot->IdString(true)});

            Self->DisconnectNodeSlots(nodeId, ctx);

            YDB_LOG_CTX_DEBUG(ctx, "Taking ownership of tenant pool on node",
                {"nodeId", nodeId});

            ctx.Send(MakeTenantPoolID(nodeId), new TEvTenantPool::TEvTakeOwnership(Self->Generation()));
            return true;
        }

        if (rec.GetStatus() == NKikimrTenantPool::UNKNOWN_SLOT) {
            YDB_LOG_CTX_ERROR(ctx, "is reported to be unknown",
                {"#_slot->IdString(true)", slot->IdString(true)});

            Modified = slot->AssignedTenant != nullptr;
            Self->RemoveSlot(slot, txc, ctx);
            return true;
        }

        if (rec.GetStatus() == NKikimrTenantPool::UNKNOWN_TENANT) {
            YDB_LOG_CTX_ERROR(ctx, "cannot be assigned",
                {"#_slot->IdString(true)", slot->IdString(true)},
                {"to_tenant", rec.GetError()});

            Modified = false; // prevent infinite loop (attach-detach-attach...)
            if (slot->AssignedTenant) {
                slot->AssignedTenant->GetAllocation(slot->UsedAs)->DecPending();
            }
            Self->DetachSlotNoConfigure(slot, txc);
            return true;
        }

        if (rec.GetStatus() != NKikimrTenantPool::SUCCESS) {
            YDB_LOG_CTX_ERROR(ctx, "Unknown status for",
                {"#_slot->IdString(true)", slot->IdString(true)},
                {"#_NKikimrTenantPool::EStatus_Name(rec.GetStatus())", NKikimrTenantPool::EStatus_Name(rec.GetStatus())},
                {"GetError", rec.GetError()});

            Modified = slot->AssignedTenant != nullptr;
            Self->RemoveSlot(slot, txc, ctx);
            return true;
        }

        if (slot->AssignedTenant) {
            if (slot->AssignedTenant->Name != rec.GetSlotStatus().GetAssignedTenant()) {
                YDB_LOG_CTX_ERROR(ctx, "configure result for has wrong tenant",
                    {"#_slot->IdString(true)", slot->IdString(true)});

                Self->SendConfigureSlot(slot, ctx);
            } else {
                YDB_LOG_CTX_DEBUG(ctx, "confirmed resource assignment for",
                    {"#_slot->IdString(true)", slot->IdString(true)});

                slot->AssignedTenant->GetAllocation(slot->UsedAs)->DecPending();
            }
        } else {
            if (rec.GetSlotStatus().GetAssignedTenant()) {
                YDB_LOG_CTX_ERROR(ctx, "has wrongly assigned tenant",
                    {"#_slot->IdString(true)", slot->IdString(true)});

                Self->SendConfigureSlot(slot, ctx);
            } else if (slot->IsFree()) {
                YDB_LOG_CTX_DEBUG(ctx, "confirmed free slot",
                    {"IdString", slot->IdString()});

                Self->FreeSlots.Add(slot);
                Modified = true;
            } else {
                YDB_LOG_CTX_DEBUG(ctx, "confirmed detached slot",
                    {"IdString", slot->IdString()});
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
