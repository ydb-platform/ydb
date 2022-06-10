#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerConnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BD01, "TEvServerConnected", (TabletId, TabletID()), (Msg, ev->Get()->ToString()));
        const auto [it, inserted] = PipeServerToNode.emplace(ev->Get()->ServerId, std::nullopt);
        Y_VERIFY(inserted);
    }

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BD02, "TEvServerDisconnected", (TabletId, TabletID()), (Msg, ev->Get()->ToString()));
        const auto it = PipeServerToNode.find(ev->Get()->ServerId);
        Y_VERIFY(it != PipeServerToNode.end());
        if (const auto& nodeId = it->second) {
            if (const auto agentIt = Agents.find(*nodeId); agentIt != Agents.end()) {
                if (TAgentInfo& agent = agentIt->second; agent.ConnectedAgent == it->first) {
                    OnAgentDisconnect(agent);
                    agent.ConnectedAgent.reset();
                    agent.ConnectedNodeId = 0;
                    agent.ExpirationTimestamp = TActivationContext::Now() + ExpirationTimeout;
                }
            }
        }
        PipeServerToNode.erase(it);
    }

    void TBlobDepot::OnAgentDisconnect(TAgentInfo& agent) {
        BlocksManager.OnAgentDisconnect(agent);
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvRegisterAgent::TPtr ev) {
        const auto it = PipeServerToNode.find(ev->Recipient);
        Y_VERIFY(it != PipeServerToNode.end());
        const ui32 nodeId = ev->Sender.NodeId();
        Y_VERIFY(!it->second || *it->second == nodeId);
        it->second = nodeId;
        auto& agent = Agents[nodeId];
        STLOG(PRI_DEBUG, BLOB_DEPOT, BD03, "TEvRegisterAgent", (TabletId, TabletID()), (Msg, ev->Get()->Record),
            (NodeId, nodeId), (PipeServerId, it->first));
        agent.ConnectedAgent = it->first;
        agent.ConnectedNodeId = nodeId;
        agent.ExpirationTimestamp = TInstant::Max();
        OnAgentConnect(agent);

        auto response = std::make_unique<TEvBlobDepot::TEvRegisterAgentResult>();
        auto& record = response->Record;
        record.SetGeneration(Executor()->Generation());
        for (const auto& channel : Info()->Channels) {
            Y_VERIFY(channel.Channel == record.ChannelGroupsSize());
            record.AddChannelGroups(channel.History ? channel.History.back().GroupID : 0);
        }

        SendResponseToAgent(*ev, std::move(response));
    }

    void TBlobDepot::OnAgentConnect(TAgentInfo& agent) {
        BlocksManager.OnAgentConnect(agent);
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvAllocateIds::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BD04, "TEvAllocateIds", (TabletId, TabletID()), (Msg, ev->Get()->Record),
            (PipeServerId, ev->Recipient));
        auto response = std::make_unique<TEvBlobDepot::TEvAllocateIdsResult>();
        auto& record = response->Record;
        const ui32 generation = Executor()->Generation();
        record.SetGeneration(generation);
        record.SetRangeBegin(NextBlobSeqId);
        NextBlobSeqId += PreallocatedIdCount;
        record.SetRangeEnd(NextBlobSeqId);
        SendResponseToAgent(*ev, std::move(response));
    }

    TBlobDepot::TAgentInfo& TBlobDepot::GetAgent(const TActorId& pipeServerId) {
        const auto it = PipeServerToNode.find(pipeServerId);
        Y_VERIFY(it != PipeServerToNode.end());
        Y_VERIFY(it->second);
        const auto agentIt = Agents.find(*it->second);
        Y_VERIFY(agentIt != Agents.end());
        Y_VERIFY(agentIt->second.ConnectedAgent == pipeServerId);
        return agentIt->second;
    }

    void TBlobDepot::SendResponseToAgent(IEventHandle& request, std::unique_ptr<IEventBase> response) {
        auto handle = std::make_unique<IEventHandle>(request.Sender, SelfId(), response.release(), 0, request.Cookie);
        if (request.InterconnectSession) {
            handle->Rewrite(TEvInterconnect::EvForward, request.InterconnectSession);
        }
        TActivationContext::Send(handle.release());
    }

} // NKikimr::NBlobDepot
