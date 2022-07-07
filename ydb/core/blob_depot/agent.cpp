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
                if (TAgent& agent = agentIt->second; agent.ConnectedAgent == it->first) {
                    OnAgentDisconnect(agent);
                    agent.ConnectedAgent.reset();
                    agent.ConnectedNodeId = 0;
                    agent.ExpirationTimestamp = TActivationContext::Now() + ExpirationTimeout;
                }
            }
        }
        PipeServerToNode.erase(it);
    }

    void TBlobDepot::OnAgentDisconnect(TAgent& /*agent*/) {
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

    void TBlobDepot::OnAgentConnect(TAgent& /*agent*/) {
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvAllocateIds::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT04, "TEvAllocateIds", (TabletId, TabletID()), (Msg, ev->Get()->Record),
            (PipeServerId, ev->Recipient));

        const ui32 generation = Executor()->Generation();

        auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, SelfId(), ev->Get()->Record.GetChannelKind(), generation);

        if (const auto it = ChannelKinds.find(record->GetChannelKind()); it != ChannelKinds.end()) {
            auto& kind = it->second;

            const ui64 rangeBegin = kind.NextBlobSeqId;
            kind.NextBlobSeqId += ev->Get()->Record.GetCount();
            const ui64 rangeEnd = kind.NextBlobSeqId;

            TGivenIdRange range;
            range.IssueNewRange(rangeBegin, rangeEnd);

            range.ToProto(record->MutableGivenIdRange());

            TAgent& agent = GetAgent(ev->Recipient);
            agent.ChannelKinds[it->first].GivenIdRanges.Join(TGivenIdRange(range));
            kind.GivenIdRanges.Join(std::move(range));

            for (ui64 value = rangeBegin; value < rangeEnd; ++value) {
                const auto blobSeqId = TBlobSeqId::FromBinary(generation, kind, value);
                PerChannelRecords[blobSeqId.Channel].GivenStepIndex.emplace(blobSeqId.Step, blobSeqId.Index);
            }
        }

        TActivationContext::Send(response.release());
    }

    TBlobDepot::TAgent& TBlobDepot::GetAgent(const TActorId& pipeServerId) {
        const auto it = PipeServerToNode.find(pipeServerId);
        Y_VERIFY(it != PipeServerToNode.end());
        Y_VERIFY(it->second);
        TAgent& agent = GetAgent(*it->second);
        Y_VERIFY(agent.ConnectedAgent == pipeServerId);
        return agent;
    }

    TBlobDepot::TAgent& TBlobDepot::GetAgent(ui32 nodeId) {
        const auto agentIt = Agents.find(nodeId);
        Y_VERIFY(agentIt != Agents.end());
        return agentIt->second;
    }

    void TBlobDepot::InitChannelKinds() {
        TTabletStorageInfo *info = Info();
        const ui32 generation = Executor()->Generation();

        Y_VERIFY(ChannelToKind.empty());
        ChannelToKind.resize(info->Channels.size(), NKikimrBlobDepot::TChannelKind::System);

        ui32 channel = 0;
        for (const auto& profile : Config.GetChannelProfiles()) {
            for (ui32 i = 0, count = profile.GetCount(); i < count; ++i, ++channel) {
                if (channel >= 2) {
                    const auto kind = profile.GetChannelKind();
                    auto& p = ChannelKinds[kind];
                    p.ChannelToIndex[channel] = p.ChannelGroups.size();
                    p.ChannelGroups.emplace_back(channel, info->GroupFor(channel, generation));

                    Y_VERIFY(channel < ChannelToKind.size());
                    ChannelToKind[channel] = kind;
                }
            }
        }

        Y_VERIFY_S(channel == ChannelToKind.size(), "channel# " << channel
            << " ChannelToKind.size# " << ChannelToKind.size());

        for (auto& [k, v] : ChannelKinds) {
            v.NextBlobSeqId = TBlobSeqId{v.ChannelGroups.front().first, generation, 1, 0}.ToBinary(v);
        }
    }

} // NKikimr::NBlobDepot
