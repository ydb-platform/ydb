#include "blob_depot_tablet.h"
#include "data.h"
#include "space_monitor.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerConnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "TEvServerConnected", (Id, GetLogId()), (ClientId, ev->Get()->ClientId),
            (ServerId, ev->Get()->ServerId));
        const auto [it, inserted] = PipeServers.try_emplace(ev->Get()->ServerId);
        Y_ABORT_UNLESS(inserted);
    }

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT02, "TEvServerDisconnected", (Id, GetLogId()), (PipeServerId, ev->Get()->ServerId));

        const auto it = PipeServers.find(ev->Get()->ServerId);
        Y_ABORT_UNLESS(it != PipeServers.end());
        if (const auto& nodeId = it->second.NodeId) {
            if (const auto agentIt = Agents.find(*nodeId); agentIt != Agents.end() && agentIt->second.Connection &&
                    agentIt->second.Connection->PipeServerId == it->first) {
                OnAgentDisconnect(agentIt->second);
                agentIt->second.Connection.reset();
                agentIt->second.ExpirationTimestamp = TActivationContext::Now() + ExpirationTimeout;
            }
        }
        PipeServers.erase(it);
    }

    void TBlobDepot::OnAgentDisconnect(TAgent& agent) {
        agent.InvalidateStepRequests.clear();
        agent.PushCallbacks.clear();
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvRegisterAgent::TPtr ev) {
        const ui32 nodeId = ev->Sender.NodeId();
        const TActorId& pipeServerId = ev->Recipient;
        const auto& req = ev->Get()->Record;

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT03, "TEvRegisterAgent", (Id, GetLogId()), (Msg, req), (NodeId, nodeId),
            (PipeServerId, pipeServerId), (Id, ev->Cookie));

        const auto it = PipeServers.find(pipeServerId);
        Y_ABORT_UNLESS(it != PipeServers.end());
        Y_ABORT_UNLESS(!it->second.NodeId || *it->second.NodeId == nodeId);
        it->second.NodeId = nodeId;
        auto& agent = Agents[nodeId];
        agent.Connection = {
            .PipeServerId = pipeServerId,
            .AgentId = ev->Sender,
            .NodeId = nodeId,
        };
        agent.ExpirationTimestamp = TInstant::Max();
        agent.LastPushedSpaceColor = SpaceMonitor->GetSpaceColor();
        agent.LastPushedApproximateFreeSpaceShare = SpaceMonitor->GetApproximateFreeSpaceShare();

        if (agent.AgentInstanceId && *agent.AgentInstanceId != req.GetAgentInstanceId()) {
            ResetAgent(agent);
        }
        agent.AgentInstanceId = req.GetAgentInstanceId();

        OnAgentConnect(agent);

        auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, Executor()->Generation());
        record->SetSpaceColor(agent.LastPushedSpaceColor);
        record->SetApproximateFreeSpaceShare(agent.LastPushedApproximateFreeSpaceShare);

        for (const auto& [k, v] : ChannelKinds) {
            auto *proto = record->AddChannelKinds();
            proto->SetChannelKind(k);
            for (const auto& [channel, groupId] : v.ChannelGroups) {
                auto *cg = proto->AddChannelGroups();
                cg->SetChannel(channel);
                cg->SetGroupId(groupId);
            }
        }

        if (Config.GetIsDecommittingGroup()) {
            record->SetDecommitGroupId(Config.GetVirtualGroupId());
        }

        TActivationContext::Send(response.release());

        if (!agent.InvalidatedStepInFlight.empty()) {
            const ui32 generation = Executor()->Generation();
            const ui64 id = ++agent.LastRequestId;

            auto reply = std::make_unique<TEvBlobDepot::TEvPushNotify>();
            auto& request = agent.InvalidateStepRequests[id];
            for (const auto& [channel, invalidatedStep] : agent.InvalidatedStepInFlight) {
                auto *item = reply->Record.AddInvalidatedSteps();
                item->SetChannel(channel);
                item->SetGeneration(generation);
                item->SetInvalidatedStep(invalidatedStep);
                request[channel] = invalidatedStep;
            }

            std::vector<TActorId> blockActorsPending;

            for (const auto& [tabletId, data] : agent.BlockToDeliver) {
                auto *item = reply->Record.AddBlockedTablets();
                item->SetTabletId(tabletId);
                const auto& [blockedGeneration, issuerGuid, actorId] = data;
                item->SetBlockedGeneration(blockedGeneration);
                item->SetIssuerGuid(issuerGuid);
                blockActorsPending.push_back(actorId);
            }

            auto r = std::make_unique<IEventHandle>(ev->Sender, ev->Recipient, reply.release(), 0, id);
            if (ev->InterconnectSession) {
                r->Rewrite(TEvInterconnect::EvForward, ev->InterconnectSession);
            }
            TActivationContext::Send(r.release());

            agent.PushCallbacks.emplace(id, [this, sender = ev->Sender, m = std::move(blockActorsPending)](
                    TEvBlobDepot::TEvPushNotifyResult::TPtr ev) {
                for (const TActorId& actorId : m) {
                    auto clone = std::make_unique<TEvBlobDepot::TEvPushNotifyResult>();
                    clone->Record.CopyFrom(ev->Get()->Record);
                    TActivationContext::Send(new IEventHandle(actorId, sender, clone.release()));
                }
                Data->OnPushNotifyResult(ev);
            });
        }
    }

    void TBlobDepot::OnAgentConnect(TAgent& /*agent*/) {
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvAllocateIds::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT04, "TEvAllocateIds", (Id, GetLogId()), (Msg, ev->Get()->Record),
            (PipeServerId, ev->Recipient));

        const ui32 generation = Executor()->Generation();
        auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, ev->Get()->Record.GetChannelKind(), generation);

        std::vector<ui8> channels(ev->Get()->Record.GetCount());
        if (PickChannels(record->GetChannelKind(), channels)) {
            auto *givenIdRange = record->MutableGivenIdRange();

            THashMap<ui8, NKikimrBlobDepot::TGivenIdRange::TChannelRange*> issuedRanges;
            for (ui8 channelIndex : channels) {
                TChannelInfo& channel = Channels[channelIndex];
                const ui64 value = channel.NextBlobSeqId++;

                // fill in range item
                auto& range = issuedRanges[channelIndex];
                if (!range || range->GetEnd() != value) {
                    range = givenIdRange->AddChannelRanges();
                    range->SetChannel(channelIndex);
                    range->SetBegin(value);
                }
                range->SetEnd(value + 1);
            }

            // register issued ranges in agent and global records
            TAgent& agent = GetAgent(ev->Recipient);
            for (const auto& range : givenIdRange->GetChannelRanges()) {
                agent.GivenIdRanges[range.GetChannel()].IssueNewRange(range.GetBegin(), range.GetEnd());
                Channels[range.GetChannel()].GivenIdRanges.IssueNewRange(range.GetBegin(), range.GetEnd());

                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT05, "IssueNewRange", (Id, GetLogId()),
                    (AgentId, agent.Connection->NodeId), (Channel, range.GetChannel()),
                    (Begin, range.GetBegin()), (End, range.GetEnd()));
            }
        }

        TActivationContext::Send(response.release());
    }

    TBlobDepot::TAgent& TBlobDepot::GetAgent(const TActorId& pipeServerId) {
        const auto it = PipeServers.find(pipeServerId);
        Y_ABORT_UNLESS(it != PipeServers.end());
        Y_ABORT_UNLESS(it->second.NodeId);
        TAgent& agent = GetAgent(*it->second.NodeId);
        Y_ABORT_UNLESS(agent.Connection && agent.Connection->PipeServerId == pipeServerId);
        return agent;
    }

    TBlobDepot::TAgent& TBlobDepot::GetAgent(ui32 nodeId) {
        const auto agentIt = Agents.find(nodeId);
        Y_ABORT_UNLESS(agentIt != Agents.end());
        TAgent& agent = agentIt->second;
        return agent;
    }

    void TBlobDepot::ResetAgent(TAgent& agent) {
        for (auto& [channel, agentGivenIdRange] : agent.GivenIdRanges) {
            if (agentGivenIdRange.IsEmpty()) {
                continue;
            }

            // calculate if this agent can be blocking garbage collection by holding least conserved blob sequence id
            auto& givenIdRanges = Channels[channel].GivenIdRanges;
            const bool unblock = givenIdRanges.GetMinimumValue() == agentGivenIdRange.GetMinimumValue();

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT06, "ResetAgent", (Id, GetLogId()), (AgentId, agent.Connection->NodeId),
                (Channel, int(channel)), (GivenIdRanges, givenIdRanges), (Agent.GivenIdRanges, agentGivenIdRange),
                (Unblock, unblock));

            givenIdRanges.Subtract(std::exchange(agentGivenIdRange, {}));

            if (unblock) {
                Data->OnLeastExpectedBlobIdChange(channel);
            }
        }
        agent.InvalidatedStepInFlight.clear();
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvPushNotifyResult::TPtr ev) {
        TAgent& agent = GetAgent(ev->Recipient);
        if (const auto it = agent.PushCallbacks.find(ev->Get()->Record.GetId()); it != agent.PushCallbacks.end()) {
            auto callback = std::move(it->second);
            agent.PushCallbacks.erase(it);
            callback(ev);
        }
    }

    void TBlobDepot::ProcessRegisterAgentQ() {
        if (!ReadyForAgentQueries()) {
            return;
        }
        for (auto& [pipeServerId, info] : PipeServers) {
            for (auto& ev : std::exchange(info.PostponeQ, {})) {
                TActivationContext::Send(ev.release());
                ++info.InFlightDeliveries;
            }
        }
    }

    void TBlobDepot::OnSpaceColorChange(NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor, float approximateFreeSpaceShare) {
        for (auto& [nodeId, agent] : Agents) {
            if (agent.Connection && (agent.LastPushedSpaceColor != spaceColor || agent.LastPushedApproximateFreeSpaceShare != approximateFreeSpaceShare)) {
                Y_ABORT_UNLESS(agent.Connection->NodeId == nodeId);
                const ui64 id = ++agent.LastRequestId;
                agent.PushCallbacks.emplace(id, [](TEvBlobDepot::TEvPushNotifyResult::TPtr) {});
                auto ev = std::make_unique<TEvBlobDepot::TEvPushNotify>();
                ev->Record.SetSpaceColor(spaceColor);
                ev->Record.SetApproximateFreeSpaceShare(approximateFreeSpaceShare);
                Send(agent.Connection->AgentId, ev.release(), 0, id);
                agent.LastPushedSpaceColor = spaceColor;
                agent.LastPushedApproximateFreeSpaceShare = approximateFreeSpaceShare;
            }
        }
    }

} // NKikimr::NBlobDepot
