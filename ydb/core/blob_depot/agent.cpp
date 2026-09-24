#include "blob_depot_tablet.h"
#include "data.h"
#include "space_monitor.h"
#include "s3.h"

#define YDB_LOG_THIS_FILE_COMPONENT BLOB_DEPOT

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerConnected::TPtr ev) {
        YDB_LOG_DEBUG("TEvServerConnected",
            {"marker", "BDT01"},
            {"id", GetLogId()},
            {"clientId", ev->Get()->ClientId},
            {"serverId", ev->Get()->ServerId});
        const auto [it, inserted] = PipeServers.try_emplace(ev->Get()->ServerId);
        Y_ABORT_UNLESS(inserted);
        it->second.ConnectionSeq = ++NextPipeServerSeq;
    }

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev) {
        YDB_LOG_DEBUG("TEvServerDisconnected",
            {"marker", "BDT02"},
            {"id", GetLogId()},
            {"pipeServerId", ev->Get()->ServerId});

        const auto it = PipeServers.find(ev->Get()->ServerId);
        Y_ABORT_UNLESS(it != PipeServers.end());

        // Requests parked in tablet-wide queues still name this pipe server as their recipient. It is about to go
        // away and there is no one left to answer to, so drop them before anything below may try to resolve the
        // agent behind them.
        S3Manager->DropPendingPrepareWrites(it->first);

        if (const auto& nodeId = it->second.NodeId) {
            if (const auto agentIt = Agents.find(*nodeId); agentIt != Agents.end() && agentIt->second.Connection &&
                    agentIt->second.Connection->PipeServerId == it->first) {
                OnAgentDisconnect(agentIt->second);
                agentIt->second.Connection.reset();
                UpdateAgentBlockingGC(agentIt->second);
                TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_AGENTS_CONNECTED] -= 1;
                agentIt->second.ExpirationTimestamp = TActivationContext::Now() + ExpirationTimeout;
                ScheduleCheckExpiredAgents();
            }
        }
        PipeServers.erase(it);
    }

    void TBlobDepot::OnAgentDisconnect(TAgent& agent) {
        Y_ABORT_UNLESS(agent.Connection); // this releases what one particular connection held

        // Anything still parked in a tablet-wide queue on behalf of this connection can no longer be answered. Drop
        // it before releasing the write slots below, because that release runs the pending-write drain and it must
        // not dispatch a request bound to the connection we are tearing down.
        S3Manager->DropPendingPrepareWrites(agent.Connection->PipeServerId);

        agent.InvalidateStepRequests.clear();
        agent.PushCallbacks.clear();

        for (TS3Locator locator : agent.S3WritesInFlight) {
            // they were not in InFlightTrashS3, so we just have to delete them
            S3Manager->AddTrashToCollect(locator);
        }
        if (const ui32 numAbandoned = agent.S3WritesInFlight.size()) {
            // The agent is never going to commit or discard these writes now. Their slots have to be given back
            // explicitly: otherwise they stay occupied for the rest of this tablet generation and, once enough of
            // them leak, TEvPrepareWriteS3 from *every* agent gets queued in PendingPrepareWrites forever.
            S3Manager->OnS3WritesInFlightAbandoned(numAbandoned);
        }
        agent.S3WritesInFlight.clear();
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvRegisterAgent::TPtr ev) {
        const ui32 nodeId = ev->Sender.NodeId();
        const TActorId& pipeServerId = ev->Recipient;
        const auto& req = ev->Get()->Record;

        YDB_LOG_DEBUG("TEvRegisterAgent",
            {"marker", "BDT03"},
            {"id", GetLogId()},
            {"msg", req},
            {"nodeId", nodeId},
            {"pipeServerId", pipeServerId},
            {"cookie", ev->Cookie});

        const auto it = PipeServers.find(pipeServerId);
        Y_ABORT_UNLESS(it != PipeServers.end());
        Y_ABORT_UNLESS(!it->second.NodeId || *it->second.NodeId == nodeId);
        it->second.NodeId = nodeId;
        auto& agent = Agents[nodeId];

        if (agent.Connection && agent.Connection->PipeServerId != pipeServerId) {
            // A registration can wait in PostponeQ until the tablet is ready to serve agents, and ProcessRegisterAgentQ
            // replays those in PipeServers order -- so this one may well predate the connection we are already using.
            // Honouring it would point Connection at a dead pipe and strand the agent on its live one; note that the
            // stale-connection guard in handleDelivery cannot catch this, as NodeId is only set once we get here.
            const auto currentIt = PipeServers.find(agent.Connection->PipeServerId);
            if (currentIt != PipeServers.end() && it->second.ConnectionSeq < currentIt->second.ConnectionSeq) {
                YDB_LOG_WARN("dropping a TEvRegisterAgent replayed on a superseded pipe",
                    {"marker", "BDT99"},
                    {"id", GetLogId()},
                    {"nodeId", nodeId},
                    {"pipeServerId", pipeServerId},
                    {"connectionSeq", it->second.ConnectionSeq},
                    {"currentPipeServerId", agent.Connection->PipeServerId},
                    {"currentConnectionSeq", currentIt->second.ConnectionSeq});
                return;
            }

            // Otherwise this registration supersedes the older connection. Its TEvServerDisconnected either has not
            // been processed yet or never will be -- either way that handler skips it once Connection has moved on,
            // so nothing else would ever release what the old connection held. Note that this covers a plain
            // reconnect of the same agent instance too, not just an AgentInstanceId change, and that it runs before
            // Connection is replaced so the resources are attributed to the connection that held them.
            OnAgentDisconnect(agent);
        }
        if (!agent.Connection) {
            TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_AGENTS_CONNECTED] += 1;
        }
        agent.Connection = {
            .PipeServerId = pipeServerId,
            .AgentId = ev->Sender,
            .NodeId = nodeId,
        };
        agent.ExpirationTimestamp = TInstant::Max();
        agent.LastPushedSpaceColor = SpaceMonitor->GetSpaceColor();
        agent.LastPushedApproximateFreeSpaceShare = SpaceMonitor->GetApproximateFreeSpaceShare();

        if (agent.AgentInstanceId && *agent.AgentInstanceId != req.GetAgentInstanceId()) {
            ResetAgent(nodeId, agent);
        }
        agent.AgentInstanceId = req.GetAgentInstanceId();
        agent.SupportsIdRangeExpiry = req.GetSupportsIdRangeExpiry();

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

        if (Config.HasS3BackendSettings()) {
            record->MutableS3BackendSettings()->CopyFrom(Config.GetS3BackendSettings());
        }

        if (Config.HasName()) {
            record->SetName(Config.GetName());
        }

        // The agent applies these before it resumes serving queries, so anything it still holds down there is
        // dropped rather than committed against blobs we may already have collected.
        for (const auto& [channel, step] : agent.ExpiredSteps) {
            auto *item = record->AddInvalidatedSteps();
            item->SetChannel(channel);
            item->SetGeneration(Executor()->Generation());
            item->SetInvalidatedStep(step);
        }

        TActivationContext::Send(response.release());

        if (!agent.InvalidatedStepInFlight.empty() || !agent.BlockToDeliver.empty()) {
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

    void TBlobDepot::OnAgentConnect(TAgent& agent) {
        UpdateAgentBlockingGC(agent);
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvAllocateIds::TPtr ev) {
        YDB_LOG_DEBUG("TEvAllocateIds",
            {"marker", "BDT04"},
            {"id", GetLogId()},
            {"msg", ev->Get()->Record},
            {"pipeServerId", ev->Recipient});

        const ui32 generation = Executor()->Generation();
        const auto channelKind = ev->Get()->Record.GetChannelKind();
        auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, channelKind, generation);

        // Both of these come straight off the wire. A kind we have no channels configured for would trip the
        // Y_ABORT_UNLESS inside PickChannels, and an unbounded Count would let a single request size a vector and
        // burn that many sequence numbers. Replying without a range is a case the agent already handles
        // (TChannelKind::ProcessQueriesWaitingForId), so degrade to that instead of trusting the peer.
        const ui32 count = Min<ui32>(ev->Get()->Record.GetCount(), MaxBlobSeqIdsPerAllocation);
        std::vector<ui8> channels(count);

        if (!count) {
            // Answering with an empty-but-present GivenIdRange reads as success to the agent: it wakes every query
            // waiting for an id, each finds nothing to allocate, re-enqueues itself and asks again. Leave the range
            // out so the agent takes its explicit failure path instead.
            YDB_LOG_WARN("TEvAllocateIds asks for no ids at all",
                {"marker", "BDT100"},
                {"id", GetLogId()},
                {"pipeServerId", ev->Recipient});
        } else if (!ChannelKinds.contains(channelKind)) {
            YDB_LOG_ERROR("TEvAllocateIds for a channel kind this BlobDepot has no channels for",
                {"marker", "BDT95"},
                {"id", GetLogId()},
                {"channelKind", int(channelKind)},
                {"pipeServerId", ev->Recipient});
        } else if (PickChannels(channelKind, channels)) {
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

                YDB_LOG_DEBUG("IssueNewRange",
                    {"marker", "BDT05"},
                    {"id", GetLogId()},
                    {"agentId", agent.Connection->NodeId},
                    {"channel", range.GetChannel()},
                    {"begin", range.GetBegin()},
                    {"end", range.GetEnd()});
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

    TBlobDepot::TAgent *TBlobDepot::FindAgent(const TActorId& pipeServerId) {
        const auto it = PipeServers.find(pipeServerId);
        if (it == PipeServers.end() || !it->second.NodeId) {
            return nullptr;
        }
        const auto agentIt = Agents.find(*it->second.NodeId);
        if (agentIt == Agents.end()) {
            return nullptr;
        }
        TAgent& agent = agentIt->second;
        return agent.Connection && agent.Connection->PipeServerId == pipeServerId ? &agent : nullptr;
    }

    void TBlobDepot::ResetAgent(ui32 nodeId, TAgent& agent) {
        for (auto& [channel, agentGivenIdRange] : agent.GivenIdRanges) {
            if (agentGivenIdRange.IsEmpty()) {
                continue;
            }

            // calculate if this agent can be blocking garbage collection by holding least conserved blob sequence id
            auto& givenIdRanges = Channels[channel].GivenIdRanges;
            const bool unblock = givenIdRanges.GetMinimumValue() == agentGivenIdRange.GetMinimumValue();

            YDB_LOG_DEBUG("ResetAgent",
                {"marker", "BDT06"},
                {"id", GetLogId()},
                {"agentId", nodeId},
                {"channel", int(channel)},
                {"givenIdRanges", givenIdRanges},
                {"Agent.GivenIdRanges", agentGivenIdRange},
                {"unblock", unblock});

            givenIdRanges.Subtract(std::exchange(agentGivenIdRange, {}));

            if (unblock) {
                Data->OnLeastExpectedBlobIdChange(channel);
            }
        }
        agent.InvalidatedStepInFlight.clear();
        UpdateAgentBlockingGC(agent);
    }

    void TBlobDepot::UpdateAgentBlockingGC(TAgent& agent) {
        const bool blocking = !agent.Connection && HasGivenIdRanges(agent);
        if (blocking == agent.BlockingGC) {
            return;
        }

        agent.BlockingGC = blocking;
        if (blocking) {
            ++AgentsBlockingGC;
        } else {
            Y_ABORT_UNLESS(AgentsBlockingGC);
            --AgentsBlockingGC;
        }
        TabletCounters->Simple()[NKikimrBlobDepot::COUNTER_AGENTS_BLOCKING_GC] = AgentsBlockingGC;
    }

    void TBlobDepot::ScheduleCheckExpiredAgents() {
        if (!std::exchange(CheckExpiredAgentsScheduled, true)) {
            // A single timer for all agents: they share ExpirationTimeout, so the worst an agent waits is twice it
            TActivationContext::Schedule(ExpirationTimeout, new IEventHandle(TEvPrivate::EvCheckExpiredAgents, 0,
                SelfId(), {}, nullptr, 0));
        }
    }

    void TBlobDepot::HandleCheckExpiredAgents() {
        CheckExpiredAgentsScheduled = false;

        const TInstant now = TActivationContext::Now();
        bool morePending = false;

        // Collected first and expired afterwards: ExpireAgent unblocks garbage collection, which walks Agents of
        // its own accord (TData::HandleTrash), and we would rather not be iterating it at the same time.
        std::vector<ui32> expired;

        for (auto& [nodeId, agent] : Agents) {
            if (agent.Connection) {
                continue; // it is back
            } else if (!agent.SupportsIdRangeExpiry) {
                continue; // we have no way to make this agent drop the ids, so we must keep them reserved
            }

            if (!HasGivenIdRanges(agent)) {
                continue;
            }

            if (agent.ExpirationTimestamp <= now) {
                expired.push_back(nodeId);
            } else {
                morePending = true;
            }
        }

        for (const ui32 nodeId : expired) {
            ExpireAgent(nodeId, GetAgent(nodeId));
        }

        if (morePending) {
            ScheduleCheckExpiredAgents();
        }
    }

    void TBlobDepot::ExpireAgent(ui32 nodeId, TAgent& agent) {
        Y_ABORT_UNLESS(!agent.Connection);
        Y_ABORT_UNLESS(agent.SupportsIdRangeExpiry);

        const ui32 generation = Executor()->Generation();

        for (const auto& [channelIndex, range] : agent.GivenIdRanges) {
            if (range.IsEmpty()) {
                continue;
            }
            Y_ABORT_UNLESS(channelIndex < Channels.size());
            TChannelInfo& channel = Channels[channelIndex];
            Y_ABORT_UNLESS(channel.NextBlobSeqId);

            // Everything this agent could still be holding on this channel -- free ids and writes in flight alike --
            // was issued below NextBlobSeqId, so ordering it to drop that whole step range covers all of it.
            ui32& step = agent.ExpiredSteps[channelIndex];
            step = Max(step, TBlobSeqId::FromSequentalNumber(channelIndex, generation, channel.NextBlobSeqId - 1).Step);

            // Ids issued from now on have to survive that trim, exactly as TData::HandleTrash arranges when it
            // invalidates a step; otherwise a freshly allocated id could share the step we just told them to drop.
            channel.AdvanceNextBlobSeqIdPastStep(generation, step);

            YDB_LOG_WARN("reclaiming blob sequence range of an expired agent",
                {"marker", "BDT96"},
                {"id", GetLogId()},
                {"agentId", nodeId},
                {"channel", int(channelIndex)},
                {"invalidatedStep", step},
                {"nextBlobSeqId", channel.NextBlobSeqId});
        }

        ResetAgent(nodeId, agent); // releases the ranges and lets garbage collection move on
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
                // Must be sent *from the pipe server*, like every other TEvPushNotify: the agent drops any push
                // notification whose sender is not its current pipe server (see TBlobDepotAgent::Handle).
                TActivationContext::Send(new IEventHandle(agent.Connection->AgentId, agent.Connection->PipeServerId,
                    ev.release(), 0, id));
                agent.LastPushedSpaceColor = spaceColor;
                agent.LastPushedApproximateFreeSpaceShare = approximateFreeSpaceShare;
            }
        }
    }

} // NKikimr::NBlobDepot
