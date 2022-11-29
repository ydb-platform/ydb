#include "blob_depot_tablet.h"
#include "data.h"
#include "space_monitor.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerConnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "TEvServerConnected", (Id, GetLogId()), (ClientId, ev->Get()->ClientId),
            (ServerId, ev->Get()->ServerId));
        const auto [it, inserted] = PipeServers.try_emplace(ev->Get()->ServerId);
        Y_VERIFY(inserted);
    }

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT02, "TEvServerDisconnected", (Id, GetLogId()), (PipeServerId, ev->Get()->ServerId));

        const auto it = PipeServers.find(ev->Get()->ServerId);
        Y_VERIFY(it != PipeServers.end());
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
        if (!Configured || (Config.GetIsDecommittingGroup() && DecommitState < EDecommitState::BlocksFinished)) {
            const auto it = PipeServers.find(ev->Recipient);
            Y_VERIFY(it != PipeServers.end());
            it->second.PostponeFromAgent = true;
            it->second.PostponeQ.emplace_back(ev.Release());
            return;
        }

        const ui32 nodeId = ev->Sender.NodeId();
        const TActorId& pipeServerId = ev->Recipient;
        const auto& req = ev->Get()->Record;

        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT03, "TEvRegisterAgent", (Id, GetLogId()), (Msg, req), (NodeId, nodeId),
            (PipeServerId, pipeServerId), (Id, ev->Cookie));

        const auto it = PipeServers.find(pipeServerId);
        Y_VERIFY(it != PipeServers.end());
        Y_VERIFY(!it->second.NodeId || *it->second.NodeId == nodeId);
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

        auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, SelfId(), Executor()->Generation());
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

            TActivationContext::Send(new IEventHandle(ev->Sender, ev->Recipient, reply.release(), 0, id));

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
        auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, SelfId(), ev->Get()->Record.GetChannelKind(), generation);

        if (const auto it = ChannelKinds.find(record->GetChannelKind()); it != ChannelKinds.end()) {
            auto& kind = it->second;
            auto *givenIdRange = record->MutableGivenIdRange();

            struct TGroupInfo {
                std::vector<TChannelInfo*> Channels;
            };
            std::unordered_map<ui32, TGroupInfo> groups;

            for (const auto& [channel, groupId] : kind.ChannelGroups) {
                Y_VERIFY_DEBUG(channel < Channels.size() && Channels[channel].ChannelKind == it->first);
                groups[groupId].Channels.push_back(&Channels[channel]);
            }

            std::vector<std::tuple<ui64, const TGroupInfo*>> options;

            ui64 accum = 0;
            for (const auto& [groupId, group] : groups) {
                if (const ui64 w = SpaceMonitor->GetGroupAllocationWeight(groupId)) {
                    accum += w;
                    options.emplace_back(accum, &group);
                }
            }

            if (accum) {
                THashMap<ui8, NKikimrBlobDepot::TGivenIdRange::TChannelRange*> issuedRanges;
                for (ui32 i = 0, count = ev->Get()->Record.GetCount(); i < count; ++i) {
                    const ui64 selection = RandomNumber(accum);
                    const auto it = std::upper_bound(options.begin(), options.end(), selection,
                        [](ui64 x, const auto& y) { return x < std::get<0>(y); });
                    const auto& [_, group] = *it;

                    const size_t channelIndex = RandomNumber(group->Channels.size());
                    TChannelInfo* const channel = group->Channels[channelIndex];

                    const ui64 value = channel->NextBlobSeqId++;

                    // fill in range item
                    auto& range = issuedRanges[channel->Index];
                    if (!range || range->GetEnd() != value) {
                        range = givenIdRange->AddChannelRanges();
                        range->SetChannel(channel->Index);
                        range->SetBegin(value);
                    }
                    range->SetEnd(value + 1);
                }
            } else {
                Y_VERIFY_DEBUG(false); // TODO(alexvru): handle this situation somehow -- agent needs to retry this query?
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
        Y_VERIFY(it != PipeServers.end());
        Y_VERIFY(it->second.NodeId);
        TAgent& agent = GetAgent(*it->second.NodeId);
        Y_VERIFY(agent.Connection && agent.Connection->PipeServerId == pipeServerId);
        return agent;
    }

    TBlobDepot::TAgent& TBlobDepot::GetAgent(ui32 nodeId) {
        const auto agentIt = Agents.find(nodeId);
        Y_VERIFY(agentIt != Agents.end());
        TAgent& agent = agentIt->second;
        return agent;
    }

    void TBlobDepot::ResetAgent(TAgent& agent) {
        for (auto& [channel, agentGivenIdRange] : agent.GivenIdRanges) {
            if (agentGivenIdRange.IsEmpty()) {
                continue;
            }

            // calculate if this agent can be blocking garbage collection by holding least conserved blob sequence id
            const bool unblock = Channels[channel].GivenIdRanges.GetMinimumValue() == agentGivenIdRange.GetMinimumValue();

            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT06, "ResetAgent", (Id, GetLogId()), (AgentId, agent.Connection->NodeId),
                (Channel, int(channel)), (GivenIdRanges, Channels[channel].GivenIdRanges),
                (Agent.GivenIdRanges, agentGivenIdRange), (Unblock, unblock));

            if (unblock) {
                Data->OnLeastExpectedBlobIdChange(channel);
            }
        }
        agent.InvalidatedStepInFlight.clear();
    }

    void TBlobDepot::InitChannelKinds() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT07, "InitChannelKinds", (Id, GetLogId()));

        TTabletStorageInfo *info = Info();
        const ui32 generation = Executor()->Generation();

        Y_VERIFY(Channels.empty());

        ui32 channel = 0;
        for (const auto& profile : Config.GetChannelProfiles()) {
            for (ui32 i = 0, count = profile.GetCount(); i < count; ++i, ++channel) {
                if (channel >= 2) {
                    const auto kind = profile.GetChannelKind();
                    auto& p = ChannelKinds[kind];
                    p.ChannelToIndex[channel] = p.ChannelGroups.size();
                    p.ChannelGroups.emplace_back(channel, info->GroupFor(channel, generation));
                    Channels.push_back({
                        ui8(channel),
                        kind,
                        &p,
                        {},
                        TBlobSeqId{channel, generation, 1, 0}.ToSequentialNumber(),
                    });
                } else {
                    Channels.push_back({
                        ui8(channel),
                        NKikimrBlobDepot::TChannelKind::System,
                        nullptr,
                        {},
                        0
                    });
                }
            }
        }
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvPushNotifyResult::TPtr ev) {
        class TTxInvokeCallback : public NTabletFlatExecutor::TTransactionBase<TBlobDepot> {
            const ui32 NodeId;
            TEvBlobDepot::TEvPushNotifyResult::TPtr Ev;

        public:
            TTxInvokeCallback(TBlobDepot *self, ui32 nodeId, TEvBlobDepot::TEvPushNotifyResult::TPtr ev)
                : TTransactionBase(self)
                , NodeId(nodeId)
                , Ev(ev)
            {}

            bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
                TAgent& agent = Self->GetAgent(NodeId);
                if (const auto it = agent.PushCallbacks.find(Ev->Get()->Record.GetId()); it != agent.PushCallbacks.end()) {
                    auto callback = std::move(it->second);
                    agent.PushCallbacks.erase(it);
                    callback(Ev);
                }
                return true;
            }

            void Complete(const TActorContext&) override {}
        };

        TAgent& agent = GetAgent(ev->Recipient);
        Execute(std::make_unique<TTxInvokeCallback>(this, agent.Connection->NodeId, ev));
    }

    void TBlobDepot::ProcessRegisterAgentQ() {
        if (!Configured || (Config.GetIsDecommittingGroup() && DecommitState < EDecommitState::BlocksFinished)) {
            return;
        }

        for (auto& [pipeServerId, info] : PipeServers) {
            if (info.PostponeFromAgent) {
                info.PostponeFromAgent = false;
                for (auto& ev : std::exchange(info.PostponeQ, {})) {
                    Y_VERIFY(ev->Cookie == info.NextExpectedMsgId);
                    ++info.NextExpectedMsgId;

                    TAutoPtr<IEventHandle> tmp(ev.release());
                    HandleFromAgent(tmp);
                }
            }
        }
    }

    void TBlobDepot::OnSpaceColorChange(NKikimrBlobStorage::TPDiskSpaceColor::E spaceColor, float approximateFreeSpaceShare) {
        for (auto& [nodeId, agent] : Agents) {
            if (agent.Connection && (agent.LastPushedSpaceColor != spaceColor || agent.LastPushedApproximateFreeSpaceShare != approximateFreeSpaceShare)) {
                Y_VERIFY(agent.Connection->NodeId == nodeId);
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
