#include "node_broker_impl.h"

#include <ydb/core/protos/counters_node_broker.pb.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::NODE_BROKER

namespace NKikimr::NNodeBroker {

using namespace NKikimrNodeBroker;

class TNodeBroker::TTxGracefulShutdown : public TTransactionBase<TNodeBroker> {
public:
    TTxGracefulShutdown(TNodeBroker *self, TEvNodeBroker::TEvGracefulShutdownRequest::TPtr &ev)
        : TBase(self)
        , Event(ev)
        , Update(false)
    {
    }

    TTxType GetTxType() const override { return TXTYPE_GRACESFUL_SHUTDOWN; }

    bool Execute(TTransactionContext &txc, const TActorContext &ctx) override
    {
        const auto& rec = Event->Get()->Record;
        const auto nodeId = rec.GetNodeId();

        YDB_LOG_CTX_DEBUG(ctx, "TTxGracefulShutdown Execute. Graceful Shutdown request from",
            {"nodeId", nodeId});

        Response = MakeHolder<TEvNodeBroker::TEvGracefulShutdownResponse>();
        const auto it = Self->Dirty.Nodes.find(nodeId);

        if (it != Self->Dirty.Nodes.end()) {
            auto& node = it->second;
            Self->Dirty.ReleaseSlotIndex(node);
            Self->Dirty.DbAddNode(node, txc);

            Response->Record.MutableStatus()->SetCode(TStatus::OK);

            Update = true;
            return true;
        }

        Response->Record.MutableStatus()->SetCode(TStatus::ERROR);
        Response->Record.MutableStatus()->SetReason(TStringBuilder() << "Cannot find node " << nodeId);

        return true;
    }

    void Complete(const TActorContext &ctx) override
    {
        LOG_DEBUG(ctx, NKikimrServices::NODE_BROKER, "TTxGracefulShutdown Complete");
        if (Update) {
            Self->Committed.ReleaseSlotIndex(Self->Committed.Nodes.at(Event->Get()->Record.GetNodeId()));
        }
        ctx.Send(Event->Sender, Response.Release());

        Self->UpdateCommittedStateCounters();
    }

private:
    TEvNodeBroker::TEvGracefulShutdownRequest::TPtr Event;
    THolder<TEvNodeBroker::TEvGracefulShutdownResponse> Response;
    bool Update;
};

ITransaction *TNodeBroker::CreateTxGracefulShutdown(TEvNodeBroker::TEvGracefulShutdownRequest::TPtr &ev)
{
    return new TTxGracefulShutdown(this, ev);
}

} // NKikimr::NNodeBroker
