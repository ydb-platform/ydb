#include "interconnect.h"
#include "interconnect_impl.h"
#include "interconnect_nameserver_base.h"
#include "events_local.h"

#include <ydb/library/actors/core/hfunc.h>
#include <ydb/library/actors/core/log.h>
#include <ydb/library/actors/interconnect/logging/logging.h>

#define YDB_LOG_THIS_FILE_COMPONENT ::NActorsServices::INTERCONNECT

namespace NActors {

    class TInterconnectDynamicNameserver
        : public TInterconnectNameserverBase<TInterconnectDynamicNameserver>
        , public TInterconnectLoggingBase
    {
        struct TPendingRequest {
            TEvInterconnect::TEvResolveNode::TPtr Request;
            TMonotonic Deadline;

            TPendingRequest(TEvInterconnect::TEvResolveNode::TPtr request, const TMonotonic& deadline)
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
            YDB_LOG_TRACE(logMsg,
                {"marker", "ICN01"});
        }

        bool IsNodeUpdated(const ui32 nodeId, const TString& address, const ui32 port) {
            bool printInfo = false;
            auto it = NodeTable.find(nodeId);
            if (it == NodeTable.end()) {
                YDB_LOG_TRACE("New node",
                    {"marker", "ICN02"},
                    {"nodeId", nodeId},
                    {"address", address},
                    {"port", port});
                printInfo = true;
            } else if (it->second.Address != address || it->second.Port != port) {
                YDB_LOG_TRACE("Updated node",
                    {"marker", "ICN03"},
                    {"nodeId", nodeId},
                    {"address", address},
                    {"port", port},
                    {"oldAddress", it->second.Address},
                    {"oldPort", it->second.Port});
                printInfo = true;
                Send(TActivationContext::InterconnectProxy(nodeId), new TEvInterconnect::TEvDisconnect);
            }
            return printInfo;
        }

        void DiscardTimedOutRequests(const TActorContext& ctx, ui32 compactionCount = 0) {

            auto now = ctx.Monotonic();

            for (auto& pending : PendingRequests) {
                if (pending.Request && pending.Deadline > now) {
                    YDB_LOG_ERROR("Unknown",
                        {"marker", "ICN06"},
                        {"nodeId", pending.Request->Get()->NodeId});
                    auto reply = new TEvLocalNodeInfo;
                    reply->NodeId = pending.Request->Get()->NodeId;
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
                YDB_LOG_ERROR("Catch exception",
                    {"marker", "ICN09"},
                    {"exception", CurrentExceptionMessage()});
            }
        }

        void HandleMissedNodeId(TEvInterconnect::TEvResolveNode::TPtr& ev,
                    const TActorContext& ctx,
                    const TMonotonic& deadline) {
            if (PendingPeriod) {
                if (PendingRequests.size() == 0) {
                    SchedulePeriodic();
                }
                PendingRequests.emplace_back(std::move(ev), Min(deadline, ctx.Monotonic() + PendingPeriod));
            } else {
                YDB_LOG_ERROR("Unknown",
                    {"marker", "ICN07"},
                    {"nodeId", ev->Get()->NodeId});
                TInterconnectNameserverBase::HandleMissedNodeId(ev, ctx, deadline);
            }
        }

        void HandleUpdate(TEvInterconnect::TEvNodesInfo::TPtr& ev,
                    const TActorContext& ctx) {

            auto request = ev->Get();
            YDB_LOG_TRACE("Update TEvNodesInfo with",
                {"marker", "ICN04"},
                {"sz", request->Nodes.size()});

            bool printInfo = false;
            ui32 compactionCount = 0;

            for (const auto& node : request->Nodes) {
                printInfo |= IsNodeUpdated(node.NodeId, node.Address, node.Port);

                NodeTable[node.NodeId] = TTableNameserverSetup::TNodeInfo(
                    node.Address, node.Host, node.ResolveHost, node.Port, node.Location);

                for (auto& pending : PendingRequests) {
                    if (pending.Request && pending.Request->Get()->NodeId == node.NodeId) {
                        YDB_LOG_TRACE("Pending discovered",
                            {"marker", "ICN05"},
                            {"nodeId", node.NodeId});
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
