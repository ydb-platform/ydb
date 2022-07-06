#include "blob_depot_tablet.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerConnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "TEvServerConnected", (TabletId, TabletID()), (Msg, ev->Get()->ToString()));
        const auto [it, inserted] = PipeServerToNode.emplace(ev->Get()->ServerId, std::nullopt);
        Y_VERIFY(inserted);
    }

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT02, "TEvServerDisconnected", (TabletId, TabletID()), (Msg, ev->Get()->ToString()));
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

    void TBlobDepot::OnAgentDisconnect(TAgentInfo& /*agent*/) {
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvRegisterAgent::TPtr ev) {
        const auto it = PipeServerToNode.find(ev->Recipient);
        Y_VERIFY(it != PipeServerToNode.end());
        const ui32 nodeId = ev->Sender.NodeId();
        Y_VERIFY(!it->second || *it->second == nodeId);
        it->second = nodeId;
        auto& agent = Agents[nodeId];
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT03, "TEvRegisterAgent", (TabletId, TabletID()), (Msg, ev->Get()->Record),
            (NodeId, nodeId), (PipeServerId, it->first));
        agent.ConnectedAgent = it->first;
        agent.ConnectedNodeId = nodeId;
        agent.ExpirationTimestamp = TInstant::Max();
        OnAgentConnect(agent);

        auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, SelfId());

        record->SetGeneration(Executor()->Generation());
        for (const auto& [k, v] : ChannelKinds) {
            auto *proto = record->AddChannelKinds();
            proto->SetChannelKind(k);
            for (const auto& [channel, groupId] : v.ChannelGroups) {
                auto *cg = proto->AddChannelGroups();
                cg->SetChannel(channel);
                cg->SetGroupId(groupId);
            }
        }

        TActivationContext::Send(response.release());
    }

    void TBlobDepot::OnAgentConnect(TAgentInfo& /*agent*/) {
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvAllocateIds::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT04, "TEvAllocateIds", (TabletId, TabletID()), (Msg, ev->Get()->Record),
            (PipeServerId, ev->Recipient));

        auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, SelfId(), ev->Get()->Record.GetChannelKind(),
            Executor()->Generation());

        if (const auto it = ChannelKinds.find(record->GetChannelKind()); it != ChannelKinds.end()) {
            auto& nextBlobSeqId = it->second.NextBlobSeqId;
            record->SetRangeBegin(nextBlobSeqId);
            nextBlobSeqId += PreallocatedIdCount;
            record->SetRangeEnd(nextBlobSeqId);
        }

        TActivationContext::Send(response.release());
    }

    TBlobDepot::TAgentInfo& TBlobDepot::GetAgent(const TActorId& pipeServerId) {
        const auto it = PipeServerToNode.find(pipeServerId);
        Y_VERIFY(it != PipeServerToNode.end());
        Y_VERIFY(it->second);
        TAgentInfo& agent = GetAgent(*it->second);
        Y_VERIFY(agent.ConnectedAgent == pipeServerId);
        return agent;
    }

    TBlobDepot::TAgentInfo& TBlobDepot::GetAgent(ui32 nodeId) {
        const auto agentIt = Agents.find(nodeId);
        Y_VERIFY(agentIt != Agents.end());
        return agentIt->second;
    }

    void TBlobDepot::InitChannelKinds() {
        TTabletStorageInfo *info = Info();
        const ui32 generation = Executor()->Generation();

        ui32 channel = 0;
        for (const auto& profile : Config.GetChannelProfiles()) {
            for (ui32 i = 0, count = profile.GetCount(); i < count; ++i, ++channel) {
                if (channel >= 2) {
                    const auto kind = profile.GetChannelKind();
                    auto& p = ChannelKinds[kind];
                    p.ChannelToIndex[channel] = p.ChannelGroups.size();
                    p.ChannelGroups.emplace_back(channel, info->GroupFor(channel, generation));
                }
            }
        }
        for (auto& [k, v] : ChannelKinds) {
            v.NextBlobSeqId = TBlobSeqId{v.ChannelGroups.front().first, generation, 1, 0}.ToBinary(v);
        }
    }

} // NKikimr::NBlobDepot
