#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/counters_node_broker.pb.h>

namespace NKikimr {
namespace NNodeBroker {

using namespace NKikimrNodeBroker;

class TNodeBroker::TTxExtendLease : public TTransactionBase<TNodeBroker> {
public:
    TTxExtendLease(TNodeBroker *self, TEvNodeBroker::TEvExtendLeaseRequest::TPtr &ev)
        : TBase(self)
        , Event(ev)
        , Update(false)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_EXTEND_LEASE; }

    bool Error(TStatus::ECode code,
               const TString &reason,
               const TActorContext &ctx)
    {
        auto nodeId = Event->Get()->Record.GetNodeId();

        LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                    "Cannot extend lease for node #" << nodeId
                    << ": " << code << ": " << reason);

        Response->Record.MutableStatus()->SetCode(code);
        Response->Record.MutableStatus()->SetReason(reason);

        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto nodeId = Event->Get()->Record.GetNodeId();

        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "TTxExtendLease Execute node #" << nodeId);

        Response = new TEvNodeBroker::TEvExtendLeaseResponse;
        Response->Record.SetNodeId(nodeId);

        auto it = Self->Nodes.find(nodeId);
        if (it == Self->Nodes.end()) {
            if (Self->ExpiredNodes.contains(nodeId))
                return Error(TStatus::WRONG_REQUEST, "Node has expired", ctx);
            else
                return Error(TStatus::WRONG_REQUEST, "Unknown node", ctx);
        }

        if (Self->IsBannedId(nodeId))
            return Error(TStatus::WRONG_REQUEST, "Node ID is banned", ctx);

        auto &node = it->second;
        if (!node.IsFixed()) {
            Self->DbUpdateNodeLease(node, txc);
            Response->Record.SetExpire(Self->Epoch.NextEnd.GetValue());
            Update = true;
        } else {
            Response->Record.SetExpire(TInstant::Max().GetValue());
        }

        Response->Record.MutableStatus()->SetCode(TStatus::OK);
        Self->Epoch.Serialize(*Response->Record.MutableEpoch());

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxExtendLease Complete");

        Y_ABORT_UNLESS(Response);
        LOG_TRACE_S(ctx, NKikimrServices::NODE_BROKER,
                    "TTxExtendLease reply with: " << Response->ToString());
        ctx.Send(Event->Sender, Response.Release());

        if (Update)
            Self->ExtendLease(Self->Nodes.at(Event->Get()->Record.GetNodeId()));

        Self->TxCompleted(Event->Get()->Record.GetNodeId(), this, ctx);
    }

private:
    TEvNodeBroker::TEvExtendLeaseRequest::TPtr Event;
    TAutoPtr<TEvNodeBroker::TEvExtendLeaseResponse> Response;
    bool Update;
};

ITransaction *TNodeBroker::CreateTxExtendLease(TEvNodeBroker::TEvExtendLeaseRequest::TPtr &ev)
{
    return new TTxExtendLease(this, ev);
}

} // NNodeBroker
} // NKikimr
