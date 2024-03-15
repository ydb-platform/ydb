#include "node_broker_impl.h"
#include "node_broker__scheme.h"

namespace NKikimr {
namespace NNodeBroker {

using namespace NKikimrNodeBroker;

class TNodeBroker::TTxReleaseSlot : public TTransactionBase<TNodeBroker> {
public:
    TTxReleaseSlot(TNodeBroker *self, TEvNodeBroker::TEvReleaseSlot::TPtr &ev)
        : TBase(self)
        , NodeId(ev->Get()->Record.GetNodeId())
        , ReleaseSlot(false) {}

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "TTxReleaseSlot Execute node #" << NodeId);

        if (!Self->EnableGenerateSlotNames) {
            return true;
        }
        
        ReleaseSlot = DbTryReleaseSlot(Self->Nodes, txc) || DbTryReleaseSlot(Self->ExpiredNodes, txc);

        if (!ReleaseSlot) {
            LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                        "Cannot release slot for node #" << NodeId << ": unknown node");
        }

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxReleaseSlot Complete");

        if (ReleaseSlot) {
            TryReleaseSlot(Self->Nodes);
            TryReleaseSlot(Self->ExpiredNodes);
        }
        
        Self->TxCompleted(NodeId, this, ctx);
    }

    bool DbTryReleaseSlot(const THashMap<ui32, TNodeInfo>& nodes, TTransactionContext &txc) {
        auto it = nodes.find(NodeId);
        if (it != nodes.end()) {
            const auto& node = it->second;
            if (node.SlotIndex.has_value()) {
                NIceDb::TNiceDb db(txc.DB);
                db.Table<Schema::Nodes>().Key(node.NodeId)
                    .UpdateToNull<Schema::Nodes::SlotIndex>();
                return true;
            }
        }
        return false;
    }

    void TryReleaseSlot(THashMap<ui32, TNodeInfo>& nodes) {
        auto it = nodes.find(NodeId);
        if (it != nodes.end()) {
            auto& node = it->second;
            Self->SlotIndexesPools[node.SubdomainKey].Release(node.SlotIndex.value());
            node.SlotIndex = std::nullopt;
        }
    }

private:
    const ui32 NodeId;
    bool ReleaseSlot;
};

ITransaction *TNodeBroker::CreateTxReleaseSlot(TEvNodeBroker::TEvReleaseSlot::TPtr &ev)
{
    return new TTxReleaseSlot(this, ev);
}

} // NNodeBroker
} // NKikimr
