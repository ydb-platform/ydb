#include "node_broker_impl.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/protos/counters_node_broker.pb.h>

namespace NKikimr {
namespace NNodeBroker {

using namespace NKikimrNodeBroker;

class TNodeBroker::TTxDecommissionNode : public TTransactionBase<TNodeBroker> {
public:
    TTxDecommissionNode(TNodeBroker *self, TEvNodeBroker::TEvDecommissionRequest::TPtr &ev)
        : TBase(self)
        , Event(ev)
        //, SlotIndex(resolvedEv->Get()->SlotIndex)
        //, ServicedSubDomain(resolvedEv->Get()->ServicedSubDomain)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_DECOMMISSION_NODE; }

    bool Error(TStatus::ECode code,
               const TString &reason,
               const TActorContext &ctx)
    {
        const auto &rec = Event->Get()->Record;
        auto host = rec.GetHost();
        auto port = rec.GetPort();
        LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                    "Cannot Decommission node " << host << ":" << port << ": " << code << ": " << reason);

        Response->Record.MutableStatus()->SetCode(code);
        Response->Record.MutableStatus()->SetReason(reason);

        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        Y_UNUSED(txc);

        auto &rec = Event->Get()->Record;
        auto host = rec.GetHost();
        ui16 port = (ui16)rec.GetPort();
        TString addr = rec.GetAddress();

        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxDecommissionNode Execute");
        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Decommission request from " << host << ":" << port << " ");

        auto it = Self->Hosts.find(std::make_tuple(host, addr, port));
        if (it != Self->Hosts.end()) {
            auto &node = Self->Nodes.find(it->second)->second;

            if (Self->EnableDecomissionNode) {
                Self->SlotIndexesPools[node.ServicedSubDomain].Release(node.SlotIndex.value());
            }
            return true;
        }

        return Error(TStatus::ERROR,
                         TStringBuilder() << "Cannot find host " << host << ":" << port << ". Address: " << addr,
                         ctx);
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxDecommissionNode Complete");

        ctx.Send(Event->Sender, Response.Release());
    }

private:
    TEvNodeBroker::TEvDecommissionRequest::TPtr Event;
    //const std::optional<ui32> SlotIndex;
    //const TSubDomainKey ServicedSubDomain;
    TAutoPtr<TEvNodeBroker::TEvDecommissionResponse> Response;
};

ITransaction *TNodeBroker::CreateTxDecommissionNode(TEvNodeBroker::TEvDecommissionRequest::TPtr &ev)
{
    return new TTxDecommissionNode(this, ev);
}

} // NNodeBroker
} // NKikimr
