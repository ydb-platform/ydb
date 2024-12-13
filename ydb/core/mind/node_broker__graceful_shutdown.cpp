#include "node_broker_impl.h"

#include <ydb/core/protos/counters_node_broker.pb.h>

namespace NKikimr::NNodeBroker {

using namespace NKikimrNodeBroker;

class TNodeBroker::TTxGracefulShutdown : public TTransactionBase<TNodeBroker> {
public:
    TTxGracefulShutdown(TNodeBroker *self, TEvNodeBroker::TEvGracefulShutdownRequest::TPtr &ev)
        : TBase(self)
        , Event(ev)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_GRACESFUL_SHUTDOWN; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        const auto& rec = Event->Get()->Record;
        const auto nodeId = rec.GetNodeId();

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "TTxGracefulShutdown Execute. Graceful Shutdown request from " << nodeId << " ");

        Response = MakeHolder<TEvNodeBroker::TEvGracefulShutdownResponse>();
        const auto it = Self->Nodes.find(nodeId);

        if (it != Self->Nodes.end()) {
            auto& node = it->second;
            Self->SlotIndexesPools[node.ServicedSubDomain].Release(node.SlotIndex.value());
            Self->DbReleaseSlotIndex(node, txc);
            node.SlotIndex.reset();

            Response->Record.MutableStatus()->SetCode(TStatus::OK);

            return true;
        }

        Response->Record.MutableStatus()->SetCode(TStatus::ERROR);
        Response->Record.MutableStatus()->SetReason(TStringBuilder() << "Cannot find node " << nodeId);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxGracefulShutdown Complete");
        ctx.Send(Event->Sender, Response.Release());
        Self->TxCompleted(this, ctx);
    }

private:
    TEvNodeBroker::TEvGracefulShutdownRequest::TPtr Event;
    THolder<TEvNodeBroker::TEvGracefulShutdownResponse> Response;
};

ITransaction *TNodeBroker::CreateTxGracefulShutdown(TEvNodeBroker::TEvGracefulShutdownRequest::TPtr &ev)
{
    return new TTxGracefulShutdown(this, ev);
}

} // NKikimr::NNodeBroker
