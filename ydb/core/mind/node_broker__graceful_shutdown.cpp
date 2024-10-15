#include "node_broker_impl.h"
#include "node_broker__scheme.h"

#include <ydb/core/base/appdata.h>
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

    bool Error(TStatus::ECode code,
               const TString &reason,
               const TActorContext &ctx)
    {
        const auto &rec = Event->Get()->Record;
        auto host = rec.GetHost();
        auto port = rec.GetPort();
        LOG_ERROR_S(ctx, NKikimrServices::NODE_BROKER,
                    "Cannot Graceful Shutdown " << host << ":" << port << ": " << code << ": " << reason);

        Response->Record.MutableStatus()->SetCode(code);
        Response->Record.MutableStatus()->SetReason(reason);

        return true;
    }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        auto &rec = Event->Get()->Record;
        auto host = rec.GetHost();
        ui16 port = (ui16)rec.GetPort();
        TString addr = rec.GetAddress();

        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxGracefulShutdown Execute");
        LOG_DEBUG_S(ctx, NKikimrServices::NODE_BROKER,
                    "Graceful Shutdown request from " << host << ":" << port << " ");

        Response = new TEvNodeBroker::TEvGracefulShutdownResponse;

        auto it = Self->Hosts.find(std::make_tuple(host, addr, port));
        if (it != Self->Hosts.end()) {
            auto &node = Self->Nodes.find(it->second)->second;

            Self->SlotIndexesPools[node.ServicedSubDomain].Release(node.SlotIndex.value());
            Self->DbReleaseSlotIndex(node, txc);
            
            return true;
        }

        return Error(TStatus::ERROR,
                         TStringBuilder() << "Cannot find host " << host << ":" << port << ". Address: " << addr,
                         ctx);
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxGracefulShutdown Complete");
        Response->Record.MutableStatus()->SetCode(TStatus::OK);
        ctx.Send(Event->Sender, Response.Release());
    }

private:
    TEvNodeBroker::TEvGracefulShutdownRequest::TPtr Event;
    TAutoPtr<TEvNodeBroker::TEvGracefulShutdownResponse> Response;
};

ITransaction *TNodeBroker::CreateTxGracefulShutdown(TEvNodeBroker::TEvGracefulShutdownRequest::TPtr &ev)
{
    return new TTxGracefulShutdown(this, ev);
}

} // NKikimr::NNodeBroker
