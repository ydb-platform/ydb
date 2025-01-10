#include "interconnect.h"
#include "interconnect_impl.h"
#include "interconnect_address.h"
#include "events_local.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/memory_log/memlog.h>

namespace NActors {

    template<typename TDerived>
    class TInterconnectNameserverBase : public TActor<TDerived> {
    protected:
        const TMap<ui32, TTableNameserverSetup::TNodeInfo>& NodeTable;

        TInterconnectNameserverBase(void (TDerived::*func)(TAutoPtr<IEventHandle>& ev)
            , const TMap<ui32, TTableNameserverSetup::TNodeInfo>& nodeTable)
            : TActor<TDerived>(func)
            , NodeTable(nodeTable)
        {
        }
    public:

        void HandleMissedNodeId(TEvInterconnect::TEvResolveNode::TPtr& ev,
                                const TActorContext& ctx,
                                const TInstant&) {
            auto reply = new TEvLocalNodeInfo;
            reply->NodeId = ev->Get()->Record.GetNodeId();
            ctx.Send(ev->Sender, reply);
        }

        void Handle(TEvInterconnect::TEvResolveNode::TPtr& ev,
                    const TActorContext& ctx) {
            const TEvInterconnect::TEvResolveNode* request = ev->Get();
            auto& record = request->Record;
            const ui32 nodeId = record.GetNodeId();
            const TInstant deadline = record.HasDeadline() ? TInstant::FromValue(record.GetDeadline()) : TInstant::Max();
            auto it = NodeTable.find(nodeId);

            if (it == NodeTable.end()) {
                static_cast<TDerived*>(this)->HandleMissedNodeId(ev, ctx, deadline);
            } else {
                IActor::RegisterWithSameMailbox(
                    CreateResolveActor(nodeId, it->second, ev->Sender, this->SelfId(), deadline));
            }
        }

        void Handle(TEvResolveAddress::TPtr& ev,
                    const TActorContext&) {
            const TEvResolveAddress* request = ev->Get();

            IActor::RegisterWithSameMailbox(
                CreateResolveActor(request->Address, request->Port, ev->Sender, this->SelfId(), TInstant::Max()));
        }

        void Handle(TEvInterconnect::TEvListNodes::TPtr& ev,
                    const TActorContext& ctx) {
            auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>();
            nodes->reserve(NodeTable.size());
            for (const auto& pr : NodeTable) {
                nodes->emplace_back(pr.first,
                                    pr.second.Address, pr.second.Host, pr.second.ResolveHost,
                                    pr.second.Port, pr.second.Location);
            }
            ctx.Send(ev->Sender, new TEvInterconnect::TEvNodesInfo(nodes));
        }

        void Handle(TEvInterconnect::TEvGetNode::TPtr& ev,
                    const TActorContext& ctx) {
            ui32 nodeId = ev->Get()->NodeId;
            THolder<TEvInterconnect::TEvNodeInfo>
                reply(new TEvInterconnect::TEvNodeInfo(nodeId));
            auto it = NodeTable.find(nodeId);
            if (it != NodeTable.end()) {
                reply->Node = MakeHolder<TEvInterconnect::TNodeInfo>(it->first, it->second.Address,
                                                                     it->second.Host, it->second.ResolveHost,
                                                                     it->second.Port, it->second.Location);
            }
            ctx.Send(ev->Sender, reply.Release());
        }
    };
}
