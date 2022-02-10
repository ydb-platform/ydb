#include "tenant_slot_broker_impl.h"

namespace NKikimr {
namespace NTenantSlotBroker {

class TTenantSlotBroker::TTxUpdateNodeLocation : public TTransactionBase<TTenantSlotBroker> {
public:
    TTxUpdateNodeLocation(TTenantSlotBroker *self, TEvInterconnect::TEvNodeInfo::TPtr ev)
        : TBase(self)
        , Event(std::move(ev))
        , Modified(false)
    {
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxUpdateNodeLocation Execute");

        auto nodeId = Event->Get()->NodeId;
        auto &nodeInfo = Event->Get()->Node;

        // We probably have no access to NodeBroker now and cannot
        // determine node's DC. In this case keep the previous value.
        if (!nodeInfo)
            return true;

        auto dc = nodeInfo->Location.GetDataCenterId();

        if (Self->NodeIdToDataCenter.contains(nodeId)
            && Self->NodeIdToDataCenter[nodeId] == dc)
            return true;

        Self->NodeIdToDataCenter[nodeId] = dc;

        // Update data center for affected slots.
        auto it = Self->SlotsByNodeId.find(nodeId);
        if (it == Self->SlotsByNodeId.end())
            return true;

        NIceDb::TNiceDb db(txc.DB);
        for (auto &slot : it->second) {
            Modified = Self->UpdateSlotDataCenter(slot, dc, txc, ctx) || Modified;
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::TENANT_SLOT_BROKER, "TTxUpdateNodeLocation Complete");

        if (Modified && Self->HasUnhappyTenant())
            Self->ScheduleTxAssignFreeSlots(ctx);

        Self->TxCompleted(this, ctx);
    }

private:
    TEvInterconnect::TEvNodeInfo::TPtr Event;
    TVector<TSlot::TPtr> ToConfigure;
    bool Modified;
};

ITransaction *TTenantSlotBroker::CreateTxUpdateNodeLocation(TEvInterconnect::TEvNodeInfo::TPtr &ev)
{
    return new TTxUpdateNodeLocation(this, std::move(ev));
}

} // NTenantSlotBroker
} // NKikimr
