#include "interconnect.h"
#include "interconnect_impl.h"
#include "interconnect_address.h"
#include "interconnect_nameserver_base.h"
#include "events_local.h"
#include "logging.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>

namespace NActors {

    class TInterconnectDynamicNameserver
        : public TInterconnectNameserverBase<TInterconnectDynamicNameserver>
        , public TInterconnectLoggingBase
    {
        struct TPendingRequest {
            TEvInterconnect::TEvResolveNode::TPtr Request;
            TInstant Deadline;

            TPendingRequest(TEvInterconnect::TEvResolveNode::TPtr request, const TInstant& deadline)
                : Request(request), Deadline(deadline)
            {
            }
        };

        TMap<ui32, TTableNameserverSetup::TNodeInfo> NodeTable;
        TVector<TPendingRequest> PendingRequests;
        TDuration PendingPeriod;

        void PrintInfo() {
            TString logMsg = TStringBuilder() << "Table size: " << NodeTable.size();
            for (const auto& [nodeId, node] : NodeTable) {
                TString str = TStringBuilder() << "\n > Node " << nodeId << " `" << node.Address << "`:" << node.Port << ", host: " << node.Host << ", resolveHost: " << node.ResolveHost;
                logMsg += str;
            }
            LOG_TRACE_IC("ICN01", "%s", logMsg.c_str());
        }

        bool IsNodeUpdated(const ui32 nodeId, const TString& address, const ui32 port) {
            bool printInfo = false;
            auto it = NodeTable.find(nodeId);
            if (it == NodeTable.end()) {
                LOG_TRACE_IC("ICN02", "New node %u `%s`: %u",
                    nodeId, address.c_str(), port);
                printInfo = true;
            } else if (it->second.Address != address || it->second.Port != port) {
                LOG_TRACE_IC("ICN03", "Updated node %u `%s`: %u (from `%s`: %u)",
                    nodeId, address.c_str(), port, it->second.Address.c_str(), it->second.Port);
                printInfo = true;
                Send(TActivationContext::InterconnectProxy(nodeId), new TEvInterconnect::TEvDisconnect);
            }
            return printInfo;
        }

        void DiscardTimedOutRequests(const TActorContext& ctx, ui32 compactionCount = 0) {

            auto now = Now();

            for (auto& pending : PendingRequests) {
                if (pending.Request && pending.Deadline > now) {
                    LOG_ERROR_IC("ICN06", "Unknown nodeId: %u", pending.Request->Get()->Record.GetNodeId());
                    auto reply = new TEvLocalNodeInfo;
                    reply->NodeId = pending.Request->Get()->Record.GetNodeId();
                    ctx.Send(pending.Request->Sender, reply);
                    pending.Request.Reset();
                    compactionCount++;
                }
            }

            if (compactionCount) {
                TVector<TPendingRequest> requests;
                if (compactionCount < PendingRequests.size()) { // sanity check
                    requests.reserve(PendingRequests.size() - compactionCount);
                }
                for (auto& pending : PendingRequests) {
                    if (pending.Request) {
                        requests.emplace_back(pending.Request, pending.Deadline);
                    }
                }
                PendingRequests.swap(requests);
            }
        }

        void SchedulePeriodic() {
            Schedule(TDuration::MilliSeconds(200), new TEvents::TEvWakeup());
        }

    public:
        static constexpr EActivityType ActorActivityType() {
            return EActivityType::NAMESERVICE;
        }

        TInterconnectDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup>& setup, const TDuration& pendingPeriod, ui32 /*resolvePoolId*/ )
            : TInterconnectNameserverBase<TInterconnectDynamicNameserver>(&TInterconnectDynamicNameserver::StateFunc, NodeTable)
            , NodeTable(setup->StaticNodeTable)
            , PendingPeriod(pendingPeriod)
        {
            Y_ABORT_UNLESS(setup->IsEntriesUnique());
        }

        STFUNC(StateFunc) {
            try {
                switch (ev->GetTypeRewrite()) {
                    HFunc(TEvInterconnect::TEvResolveNode, Handle);
                    HFunc(TEvResolveAddress, Handle);
                    HFunc(TEvInterconnect::TEvListNodes, Handle);
                    HFunc(TEvInterconnect::TEvGetNode, Handle);
                    HFunc(TEvInterconnect::TEvNodesInfo, HandleUpdate);
                    CFunc(TEvents::TEvWakeup::EventType, HandlePeriodic);
                }
            } catch (...) {
                LOG_ERROR_IC("ICN09", "%s", CurrentExceptionMessage().c_str());
            }
        }

        void HandleMissedNodeId(TEvInterconnect::TEvResolveNode::TPtr& ev,
                    const TActorContext& ctx,
                    const TInstant& deadline) {
            if (PendingPeriod) {
                if (PendingRequests.size() == 0) {
                    SchedulePeriodic();
                }
                PendingRequests.emplace_back(std::move(ev), Min(deadline, Now() + PendingPeriod));
            } else {
                LOG_ERROR_IC("ICN07", "Unknown nodeId: %u", ev->Get()->Record.GetNodeId());
                TInterconnectNameserverBase::HandleMissedNodeId(ev, ctx, deadline);
            }
        }

        void HandleUpdate(TEvInterconnect::TEvNodesInfo::TPtr& ev,
                    const TActorContext& ctx) {

            auto request = ev->Get();
            LOG_TRACE_IC("ICN04", "Update TEvNodesInfo with sz: %lu ", request->Nodes.size());

            bool printInfo = false;
            ui32 compactionCount = 0;

            for (const auto& node : request->Nodes) {
                printInfo |= IsNodeUpdated(node.NodeId, node.Address, node.Port);

                NodeTable[node.NodeId] = TTableNameserverSetup::TNodeInfo(
                    node.Address, node.Host, node.ResolveHost, node.Port, node.Location);

                for (auto& pending : PendingRequests) {
                    if (pending.Request && pending.Request->Get()->Record.GetNodeId() == node.NodeId) {
                        LOG_TRACE_IC("ICN05", "Pending nodeId: %u discovered", node.NodeId);
                        RegisterWithSameMailbox(
                            CreateResolveActor(node.NodeId, NodeTable[node.NodeId], pending.Request->Sender, SelfId(), pending.Deadline));
                        pending.Request.Reset();
                        compactionCount++;
                    }
                }
            }

            if (printInfo) {
                PrintInfo();
            }

            DiscardTimedOutRequests(ctx, compactionCount);
        }

        void HandlePeriodic(const TActorContext& ctx) {
            DiscardTimedOutRequests(ctx, 0);
            if (PendingRequests.size()) {
                SchedulePeriodic();
            }
        }
    };

    IActor* CreateDynamicNameserver(const TIntrusivePtr<TTableNameserverSetup>& setup,
                                    const TDuration& pendingPeriod,
                                    ui32 poolId) {
        return new TInterconnectDynamicNameserver(setup, pendingPeriod, poolId);
    }

}
