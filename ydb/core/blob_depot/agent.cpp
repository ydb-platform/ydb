#include "blob_depot_tablet.h"
#include "data.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerConnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT01, "TEvServerConnected", (TabletId, TabletID()),
            (PipeServerId, ev->Get()->ServerId));
        const auto [it, inserted] = PipeServerToNode.emplace(ev->Get()->ServerId, std::nullopt);
        Y_VERIFY(inserted);
    }

    void TBlobDepot::Handle(TEvTabletPipe::TEvServerDisconnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT02, "TEvServerDisconnected", (TabletId, TabletID()),
            (PipeServerId, ev->Get()->ServerId));
        const auto it = PipeServerToNode.find(ev->Get()->ServerId);
        Y_VERIFY(it != PipeServerToNode.end());
        if (const auto& nodeId = it->second) {
            if (const auto agentIt = Agents.find(*nodeId); agentIt != Agents.end()) {
                if (TAgent& agent = agentIt->second; agent.PipeServerId == it->first) {
                    OnAgentDisconnect(agent);
                    agent.PipeServerId.reset();
                    agent.AgentId.reset();
                    agent.ConnectedNodeId = 0;
                    agent.ExpirationTimestamp = TActivationContext::Now() + ExpirationTimeout;
                }
            }
        }
        PipeServerToNode.erase(it);
    }

    void TBlobDepot::OnAgentDisconnect(TAgent& agent) {
        agent.InvalidateStepRequests.clear();
        agent.PushCallbacks.clear();
    }

    void TBlobDepot::Handle(TEvBlobDepot::TEvRegisterAgent::TPtr ev) {
        const ui32 nodeId = ev->Sender.NodeId();
        const TActorId& pipeServerId = ev->Recipient;
        const auto& req = ev->Get()->Record;
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT03, "TEvRegisterAgent", (TabletId, TabletID()), (Msg, req), (NodeId, nodeId),
            (PipeServerId, pipeServerId), (Id, ev->Cookie));

        const auto it = PipeServerToNode.find(pipeServerId);
        Y_VERIFY(it != PipeServerToNode.end());
        Y_VERIFY(!it->second || *it->second == nodeId);
        it->second = nodeId;
        auto& agent = Agents[nodeId];
        agent.PipeServerId = pipeServerId;
        agent.AgentId = ev->Sender;
        agent.ConnectedNodeId = nodeId;
        agent.ExpirationTimestamp = TInstant::Max();

        if (agent.AgentInstanceId && *agent.AgentInstanceId != req.GetAgentInstanceId()) {
            ResetAgent(agent);
        }
        agent.AgentInstanceId = req.GetAgentInstanceId();

        OnAgentConnect(agent);

        auto [response, record] = TEvBlobDepot::MakeResponseFor(*ev, SelfId(), Executor()->Generation());

        for (const auto& [k, v] : ChannelKinds) {
            auto *proto = record->AddChannelKinds();
            proto->SetChannelKind(k);
            for (const auto& [channel, groupId] : v.ChannelGroups) {
                auto *cg = proto->AddChannelGroups();
                cg->SetChannel(channel);
                cg->SetGroupId(groupId);
            }
        }

        record->MutableDecommittingGroups()->CopyFrom(Config.GetDecommittingGroups());

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
            auto *givenIdRange = record->MutableGivenIdRange();

            // FIXME: optimize for faster range selection

            struct THeapItem {
                ui64 Size;
                std::vector<TChannelInfo*> Channels;

                struct TCompare {
                    bool operator ()(const THeapItem& x, const THeapItem& y) const { return x.Size > y.Size; }
                };

                struct TChannelCompare {
                    bool operator ()(TChannelInfo *x, TChannelInfo *y) const { return x->NextBlobSeqId > y->NextBlobSeqId; }
                };

                void MakeChannelHeap() {
                    std::make_heap(Channels.begin(), Channels.end(), TChannelCompare());
                }

                std::pair<ui8, ui64> PickChannelBlobSeq() {
                    std::pop_heap(Channels.begin(), Channels.end(), TChannelCompare());
                    TChannelInfo *channel = Channels.back();
                    auto res = std::make_pair(channel->Index, channel->NextBlobSeqId++);
                    std::push_heap(Channels.begin(), Channels.end(), TChannelCompare());
                    Size += 4 << 20; // assume each written blob of this size in a first approximation
                    return res;
                }
            };
            std::vector<THeapItem> heap;
            THashMap<ui32, size_t> groupToHeapIndex;

            for (const auto& [channel, groupId] : kind.ChannelGroups) {
                Y_VERIFY_DEBUG(channel < Channels.size() && Channels[channel].ChannelKind == it->first);

                const auto [it, inserted] = groupToHeapIndex.emplace(groupId, heap.size());
                if (inserted) {
                    heap.push_back(THeapItem{Groups[groupId].AllocatedBytes, {1, &Channels[channel]}});
                } else {
                    heap[it->second].Channels.push_back(&Channels[channel]);
                }
            }

            for (auto& item : heap) {
                item.MakeChannelHeap();
            }

            std::make_heap(heap.begin(), heap.end(), THeapItem::TCompare());

            THashMap<ui8, NKikimrBlobDepot::TGivenIdRange::TChannelRange*> issuedRanges;
            for (ui32 i = 0, count = ev->Get()->Record.GetCount(); i < count; ++i) {
                // pick channel/sequence number
                std::pop_heap(heap.begin(), heap.end(), THeapItem::TCompare());
                auto [channel, value] = heap.back().PickChannelBlobSeq();
                std::push_heap(heap.begin(), heap.end(), THeapItem::TCompare());

                // fill in range item
                auto& range = issuedRanges[channel];
                if (!range || range->GetEnd() != value) {
                    range = givenIdRange->AddChannelRanges();
                    range->SetChannel(channel);
                    range->SetBegin(value);
                }
                range->SetEnd(value + 1);
            }

            // register issued ranges in agent and global records
            TAgent& agent = GetAgent(ev->Recipient);
            for (const auto& range : givenIdRange->GetChannelRanges()) {
                agent.GivenIdRanges[range.GetChannel()].IssueNewRange(range.GetBegin(), range.GetEnd());

                auto& givenIdRanges = Channels[range.GetChannel()].GivenIdRanges;
                const bool wasEmpty = givenIdRanges.IsEmpty();
                givenIdRanges.IssueNewRange(range.GetBegin(), range.GetEnd());
                if (wasEmpty) {
                    Data->OnLeastExpectedBlobIdChange(range.GetChannel());
                }

                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT05, "IssueNewRange", (TabletId, TabletID()),
                    (AgentId, agent.ConnectedNodeId), (Channel, range.GetChannel()),
                    (Begin, range.GetBegin()), (End, range.GetEnd()));
            }
        }

        TActivationContext::Send(response.release());
    }

    TBlobDepot::TAgent& TBlobDepot::GetAgent(const TActorId& pipeServerId) {
        const auto it = PipeServerToNode.find(pipeServerId);
        Y_VERIFY(it != PipeServerToNode.end());
        Y_VERIFY(it->second);
        TAgent& agent = GetAgent(*it->second);
        Y_VERIFY(agent.PipeServerId == pipeServerId);
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
            Channels[channel].GivenIdRanges.Subtract(agentGivenIdRange);
            const ui32 channel_ = channel;
            const auto& agentGivenIdRange_ = agentGivenIdRange;
            STLOG(PRI_DEBUG, BLOB_DEPOT, BDT06, "ResetAgent", (TabletId, TabletID()), (AgentId, agent.ConnectedNodeId),
                (Channel, channel_), (GivenIdRanges, Channels[channel_].GivenIdRanges),
                (Agent.GivenIdRanges, agentGivenIdRange_));
            agentGivenIdRange = {};
        }
        Data->HandleTrash();
    }

    void TBlobDepot::InitChannelKinds() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT07, "InitChannelKinds", (TabletId, TabletID()));

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
            TEvBlobDepot::TEvPushNotifyResult::TPtr Ev;

        public:
            TTxInvokeCallback(TBlobDepot *self, TEvBlobDepot::TEvPushNotifyResult::TPtr ev)
                : TTransactionBase(self)
                , Ev(ev)
            {}

            bool Execute(TTransactionContext& /*txc*/, const TActorContext&) override {
                TAgent& agent = Self->GetAgent(Ev->Recipient);
                if (const auto it = agent.PushCallbacks.find(Ev->Cookie); it != agent.PushCallbacks.end()) {
                    auto callback = std::move(it->second);
                    agent.PushCallbacks.erase(it);
                    callback(Ev);
                }
                return true;
            }

            void Complete(const TActorContext&) override {}
        };

        Execute(std::make_unique<TTxInvokeCallback>(this, ev));
    }

} // NKikimr::NBlobDepot
