#include "dynamic_nameserver.h"

#include <ydb/library/yql/utils/log/log.h>

#include <ydb/library/actors/interconnect/interconnect.h>
#include <ydb/library/actors/interconnect/interconnect_impl.h>
#include <ydb/library/actors/interconnect/interconnect_address.h>
#include <ydb/library/actors/interconnect/events_local.h>

#include <ydb/library/actors/core/hfunc.h>

#include <ydb/library/yql/providers/dq/worker_manager/interface/events.h>

namespace NYql::NDqs {
    using namespace NActors;

    class TDynamicNameserver: public TActor<TDynamicNameserver> {
        TMap<ui32, TTableNameserverSetup::TNodeInfo> NodeTable;

    public:
        static constexpr EActivityType ActorActivityType() {
            return EActivityType::INTERCONNECT_COMMON;
        }

        TDynamicNameserver(
            const TIntrusivePtr<TTableNameserverSetup>& setup,
            ui32 /*resolvePoolId*/ = 0)
            : TActor(&TDynamicNameserver::StateFunc)
            , NodeTable(setup->StaticNodeTable)
        {
            Y_ABORT_UNLESS(setup->IsEntriesUnique());
        }

        STFUNC(StateFunc) {
            switch (ev->GetTypeRewrite()) {
                HFunc(TEvInterconnect::TEvResolveNode, Handle);
                HFunc(TEvResolveAddress, Handle);
                HFunc(TEvInterconnect::TEvListNodes, Handle);
                HFunc(TEvInterconnect::TEvGetNode, Handle);
                HFunc(TEvInterconnect::TEvNodesInfo, UpdateNodeTable);
                HFunc(TEvRegisterNode, RegisterNodeHandle);
            }
        }

        void PrintInfo() {
            YQL_CLOG(TRACE, ProviderDq) << "Table ";
            for (auto& [nodeId, node] : NodeTable) {
                YQL_CLOG(TRACE, ProviderDq) << " > Node " << nodeId << " `" << node.Address << "':" << node.Port;
            }
        }

        bool IsNodeUpdated(const ui32 nodeId, const TString& address, const ui32 port) {
            bool printInfo = false;
            auto it = NodeTable.find(nodeId);
            if (it == NodeTable.end()) {
                YQL_CLOG(DEBUG, ProviderDq) << "New node " << nodeId << " `" << address << "':" << port;
                printInfo = true;
            } else if (it->second.Address != address || it->second.Port != port) {
                YQL_CLOG(DEBUG, ProviderDq) << "Updated node " << nodeId << " "
                    << "`" << address << "':" << port
                    << " Was "
                    << "`" << it->second.Address << "':" << it->second.Port;
                printInfo = true;

                Send(TActivationContext::InterconnectProxy(nodeId), new TEvInterconnect::TEvDisconnect);
            }
            return printInfo;
        }

        void UpdateNodeTable(TEvInterconnect::TEvNodesInfo::TPtr& ev, const TActorContext&) {
            auto* req = ev->Get();

            bool printInfo = false;
            for (auto& node : req->Nodes) {
                printInfo |= IsNodeUpdated(node.NodeId, node.Address, node.Port);

                NodeTable[node.NodeId] = TTableNameserverSetup::TNodeInfo(
                    node.Address,
                    node.Host,
                    node.ResolveHost,
                    node.Port,
                    node.Location);
            }

            if (printInfo) {
                PrintInfo();
            }
        }

        void RegisterNodeHandle(TEvRegisterNode::TPtr& ev, const TActorContext& ctx) {
            try {
                TEvRegisterNode* req = ev->Get();
                auto& request = req->Record.GetRequest();

                if (!request.GetZombie()) {
                    bool printInfo = IsNodeUpdated(
                        request.GetNodeId(),
                        request.GetAddress(),
                        request.GetPort());
                    NodeTable[request.GetNodeId()] = std::make_pair(
                        request.GetAddress(),
                        request.GetPort());

                    if (printInfo) {
                        PrintInfo();
                    }
                }

                auto response = MakeHolder<TEvRegisterNodeResponse>(GetNodesInfo(), request.GetEpoch());
                *response->Record.MutableResponse()->MutableDownloadList() = request.GetDownloadList();
                ctx.Send(ev->Sender, response.Release());
            } catch (...) {
                // never
                Y_ABORT_UNLESS(false);
            }
        }

        void Handle(TEvInterconnect::TEvResolveNode::TPtr& ev,
                    const TActorContext& ctx)
        {
            const TEvInterconnect::TEvResolveNode* request = ev->Get();
            const ui32 nodeId = request->Record.GetNodeId();
            const TInstant deadline = request->Record.HasDeadline() ? TInstant::FromValue(request->Record.GetDeadline()) : TInstant::Max();
            auto it = NodeTable.find(nodeId);

            if (it == NodeTable.end()) {
                auto reply = new TEvLocalNodeInfo;
                reply->NodeId = nodeId;
                ctx.Send(ev->Sender, reply);
                return;
            }

            RegisterWithSameMailbox(CreateResolveActor(it->second.ResolveHost,
                                             it->second.Port,
                                             nodeId,
                                             it->second.Address,
                                             ev->Sender,
                                             SelfId(),
                                             deadline));
        }

        void Handle(TEvResolveAddress::TPtr& ev,
                    const TActorContext& ctx)
        {
            Y_UNUSED(ctx);
            const TEvResolveAddress* request = ev->Get();

            RegisterWithSameMailbox(CreateResolveActor(request->Address,
                                             request->Port,
                                             ev->Sender,
                                             SelfId(),
                                             TInstant::Max()));
        }

        TVector<NActors::TEvInterconnect::TNodeInfo> GetNodesInfo()
        {
            TVector<NActors::TEvInterconnect::TNodeInfo> nodes;
            nodes.reserve(NodeTable.size());
            for (const auto& pr : NodeTable) {
                nodes.emplace_back(pr.first,
                                   pr.second.Address, pr.second.Host, pr.second.ResolveHost,
                                   pr.second.Port, pr.second.Location);
            }
            return nodes;
        }

        void ReplyListNodes(const NActors::TActorId sender, const TActorContext& ctx)
        {
            auto nodes = MakeIntrusive<TIntrusiveVector<TEvInterconnect::TNodeInfo>>(GetNodesInfo());
            ctx.Send(sender, new TEvInterconnect::TEvNodesInfo(nodes));
        }

        void Handle(TEvInterconnect::TEvListNodes::TPtr& ev,
                    const TActorContext& ctx) {
            try {
                ReplyListNodes(ev->Sender, ctx);
            } catch (...) {
                // on error - do nothing
            }
        }

        void Handle(TEvInterconnect::TEvGetNode::TPtr& ev,
                    const TActorContext& ctx) {
            try {
                ui32 nodeId = ev->Get()->NodeId;
                THolder<TEvInterconnect::TEvNodeInfo>
                    reply(new TEvInterconnect::TEvNodeInfo(nodeId));
                auto it = NodeTable.find(nodeId);
                if (it != NodeTable.end())
                    reply->Node.Reset(new TEvInterconnect::TNodeInfo(it->first, it->second.Address,
                                                                 it->second.Host, it->second.ResolveHost,
                                                                 it->second.Port, it->second.Location));
                ctx.Send(ev->Sender, reply.Release());
            } catch (...) {
                // on error - do nothing
            }
        }
    };

    IActor* CreateDynamicNameserver(
        const TIntrusivePtr<TTableNameserverSetup>& setup,
        ui32 poolId) {
        return new TDynamicNameserver(setup, poolId);
    }

}
