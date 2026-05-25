#include "tenant_slot_broker_impl.h"

#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::TENANT_SLOT_BROKER

namespace NKikimr {
namespace NTenantSlotBroker {

class TTenantSlotBroker::TTxUpdatePoolStatus : public TTransactionBase<TTenantSlotBroker> {
public:
    TTxUpdatePoolStatus(TTenantSlotBroker *self, TEvTenantPool::TEvTenantPoolStatus::TPtr ev)
        : TBase(self)
        , Event(std::move(ev))
        , NewFreeSlot(false)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        ui32 nodeId = Event->Sender.NodeId();
        auto &rec = Event->Get()->Record;

        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdatePoolStatus execute for node",
            {"nodeId", nodeId});

        TString dc = ANY_DATA_CENTER;
        if (Self->NodeIdToDataCenter.contains(nodeId))
            dc = Self->NodeIdToDataCenter[nodeId];

        for (auto &slotProto : rec.GetSlots()) {
            auto &slotId = slotProto.GetId();
            auto &slotType = slotProto.GetType();
            auto &tenantName = slotProto.GetAssignedTenant();

            auto it = Self->Slots.find(TSlotId(nodeId, slotId));
            if (it == Self->Slots.end()) {
                // Add new known slot, detach any tenant from it.
                TSlot::TPtr slot = new TSlot({nodeId, slotId}, slotType, dc);
                slot->IsConnected = true;

                Self->AddSlot(slot, txc, ctx);

                YDB_LOG_CTX_DEBUG(ctx, "New slot connected",
                    {"#_slot->IdString(true)", slot->IdString(true)});

                if (slot->IsFree())
                    NewFreeSlot = true;

                if (tenantName || slot->IsPinned)
                    ToConfigure.push_back(slot);
            } else {
                auto &slot = it->second;

                if (slot->IsPending())
                    slot->AssignedTenant->GetAllocation(slot->UsedAs)->DecPending();

                Self->SlotConnected(slot);
                slot->LastRequestId = 0;

                YDB_LOG_CTX_DEBUG(ctx, "Reconnected to",
                    {"#_slot->IdString(true)", slot->IdString(true)});

                if (slot->IsFree()) {
                    Self->FreeSlots.Add(slot);
                    NewFreeSlot = true;
                }

                bool detached = Self->UpdateSlotType(slot, slotType, txc, ctx) && !slot->IsFree();

                // Check if re-configuration is required or assigned resource is confirmed.
                if ((!detached && !slot->AssignedTenant && tenantName)
                    || (slot->AssignedTenant && slot->AssignedTenant->Name != tenantName)
                    || (slot->AssignedTenant && slot->Label != slotProto.GetLabel())) {
                    ToConfigure.push_back(slot);
                }
            }
        }

        if (!Self->NodeIdToDataCenter.contains(nodeId))
            ctx.Send(GetNameserviceActorId(), new TEvInterconnect::TEvGetNode(nodeId));

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        ui32 nodeId = Event->Sender.NodeId();
        YDB_LOG_CTX_DEBUG(ctx, "TTxUpdatePoolStatus complete for node",
            {"nodeId", nodeId});

        for (auto &slot : ToConfigure) {
            Self->SendConfigureSlot(slot, ctx);
        }

        if (NewFreeSlot && Self->HasUnhappyTenant())
            Self->ScheduleTxAssignFreeSlots(ctx);

        Self->TxCompleted(this, ctx);
    }

private:
    TEvTenantPool::TEvTenantPoolStatus::TPtr Event;
    TVector<TSlot::TPtr> ToConfigure;
    bool NewFreeSlot;
};

ITransaction *TTenantSlotBroker::CreateTxUpdatePoolStatus(TEvTenantPool::TEvTenantPoolStatus::TPtr &ev)
{
    return new TTxUpdatePoolStatus(this, std::move(ev));
}

} // NTenantSlotBroker
} // NKikimr
