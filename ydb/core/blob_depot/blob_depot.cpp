#include "blob_depot.h"
#include "blob_depot_tablet.h"
#include "blocks.h"
#include "garbage_collection.h"
#include "data.h"
#include "data_uncertain.h"
#include "space_monitor.h"

namespace NKikimr::NBlobDepot {

    TBlobDepot::TBlobDepot(TActorId tablet, TTabletStorageInfo *info)
        : TActor(&TThis::StateInit)
        , TTabletExecutedFlat(info, tablet, new NMiniKQL::TMiniKQLFactory)
        , TabletCountersPtr(new TProtobufTabletCounters<
                NKikimrBlobDepot::ESimpleCounters_descriptor,
                NKikimrBlobDepot::ECumulativeCounters_descriptor,
                NKikimrBlobDepot::EPercentileCounters_descriptor,
                NKikimrBlobDepot::ETxTypes_descriptor
            >())
        , TabletCounters(TabletCountersPtr.Get())
        , BlocksManager(new TBlocksManager(this))
        , BarrierServer(new TBarrierServer(this))
        , Data(new TData(this))
        , SpaceMonitor(new TSpaceMonitor(this))
        , JsonHandler(std::bind(&TBlobDepot::RenderJson, this, std::placeholders::_1), TEvPrivate::EvJsonTimer, TEvPrivate::EvJsonUpdate)
    {}

    TBlobDepot::~TBlobDepot()
    {}

    void TBlobDepot::HandleFromAgent(STATEFN_SIG) {
        switch (const ui32 type = ev->GetTypeRewrite()) {
            hFunc(TEvBlobDepot::TEvRegisterAgent, Handle);
            hFunc(TEvBlobDepot::TEvAllocateIds, Handle);
            hFunc(TEvBlobDepot::TEvCommitBlobSeq, Handle);
            hFunc(TEvBlobDepot::TEvDiscardSpoiledBlobSeq, Handle);
            hFunc(TEvBlobDepot::TEvResolve, Data->Handle);
            hFunc(TEvBlobDepot::TEvBlock, BlocksManager->Handle);
            hFunc(TEvBlobDepot::TEvQueryBlocks, BlocksManager->Handle);
            hFunc(TEvBlobDepot::TEvCollectGarbage, BarrierServer->Handle);
            hFunc(TEvBlobDepot::TEvPushNotifyResult, Handle);

            default:
                Y_ABORT();
        }
    }

    STFUNC(TBlobDepot::StateWork) {
        try {
            auto handleDelivery = [this](auto& ev) {
                const auto it = PipeServers.find(ev->Recipient);
                if (it == PipeServers.end()) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT29, "HandleDelivery dropped", (Id, GetLogId()),
                        (RequestId, ev->Cookie), (Sender, ev->Sender), (PipeServerId, ev->Recipient), (Type, ev->Type));
                    return;
                }
                auto& info = it->second;

                Y_ABORT_UNLESS(info.InFlightDeliveries);
                --info.InFlightDeliveries;

                // return original event type
                ev->Rewrite(ev->Type, ev->GetRecipientRewrite());

                // ensure correct ordering of incoming messages
                Y_VERIFY_S(ev->Cookie == info.NextExpectedMsgId, "message reordering detected Cookie# " << ev->Cookie
                    << " NextExpectedMsgId# " << info.NextExpectedMsgId << " Type# " << Sprintf("%08" PRIx32,
                    ev->GetTypeRewrite()) << " Id# " << GetLogId());
                ++info.NextExpectedMsgId;
                HandleFromAgent(ev);
            };

            auto handleFromAgentPipe = [this](auto& ev) {
                const auto it = PipeServers.find(ev->Recipient);
                if (it == PipeServers.end()) {
                    STLOG(PRI_DEBUG, BLOB_DEPOT, BDT23, "HandleFromAgentPipe dropped", (Id, GetLogId()),
                        (RequestId, ev->Cookie), (Sender, ev->Sender), (PipeServerId, ev->Recipient), (Type, ev->Type));
                    return; // this may be a race with TEvServerDisconnected and postpone queue; it's okay to have this
                }
                auto& info = it->second;

                STLOG(PRI_DEBUG, BLOB_DEPOT, BDT69, "HandleFromAgentPipe", (Id, GetLogId()), (RequestId, ev->Cookie),
                    (Sender, ev->Sender), (PipeServerId, ev->Recipient), (NextExpectedMsgId, info.NextExpectedMsgId),
                    (PostponeQ.size, info.PostponeQ.size()), (InFlightDeliveries, info.InFlightDeliveries),
                    (ReadyForAgentQueries, ReadyForAgentQueries()), (Type, ev->Type));

                Y_ABORT_UNLESS(ev->Type == ev->GetTypeRewrite());
                ev->Rewrite(TEvPrivate::EvDeliver, ev->GetRecipientRewrite());

                if (!ReadyForAgentQueries()) { // we can't handle agent queries now -- enqueue this message
                    info.PostponeQ.emplace_back(ev.Release());
                } else if (!info.PostponeQ.empty()) {
                    Y_ABORT("PostponeQ can't be nonempty while agent is running");
                } else if (info.InFlightDeliveries++) {
                    TActivationContext::Send(ev.Release());
                } else { // handle event as delivery one
                    StateWork(ev);
                }
            };

            switch (const ui32 type = ev->GetTypeRewrite()) {
                hFunc(TEvBlobDepot::TEvApplyConfig, Handle);

                fFunc(TEvBlobDepot::EvRegisterAgent, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvAllocateIds, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvCommitBlobSeq, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvDiscardSpoiledBlobSeq, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvResolve, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvBlock, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvQueryBlocks, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvPushNotifyResult, handleFromAgentPipe);
                fFunc(TEvBlobDepot::EvCollectGarbage, handleFromAgentPipe);

                fFunc(TEvPrivate::EvDeliver, handleDelivery);

                hFunc(TEvBlobDepot::TEvPushMetrics, Handle);

                hFunc(TEvBlobStorage::TEvCollectGarbageResult, Data->Handle);
                hFunc(TEvBlobStorage::TEvGetResult, Data->UncertaintyResolver->Handle);

                hFunc(TEvBlobStorage::TEvStatusResult, SpaceMonitor->Handle);
                cFunc(TEvPrivate::EvKickSpaceMonitor, KickSpaceMonitor);

                hFunc(TEvTabletPipe::TEvServerConnected, Handle);
                hFunc(TEvTabletPipe::TEvServerDisconnected, Handle);

                cFunc(TEvPrivate::EvCommitCertainKeys, Data->HandleCommitCertainKeys);
                cFunc(TEvPrivate::EvDoGroupMetricsExchange, DoGroupMetricsExchange);
                hFunc(TEvBlobStorage::TEvControllerGroupMetricsExchange, Handle);
                cFunc(TEvPrivate::EvUpdateThroughputs, UpdateThroughputs);

                cFunc(TEvPrivate::EvJsonTimer, JsonHandler.HandleTimer);
                cFunc(TEvPrivate::EvJsonUpdate, JsonHandler.HandleUpdate);

                default:
                    if (!HandleDefaultEvents(ev, SelfId())) {
                        Y_ABORT("unexpected event Type# 0x%08" PRIx32, type);
                    }
                    break;
            }
        } catch (...) {
            Y_FAIL_S("unexpected exception# " << CurrentExceptionMessage());
        }
    }

    void TBlobDepot::PassAway() {
        for (const TActorId& actorId : {GroupAssimilatorId}) {
            if (actorId) {
                TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, actorId, SelfId(), nullptr, 0));
            }
        }

        TActor::PassAway();
    }

    void TBlobDepot::InitChannelKinds() {
        STLOG(PRI_DEBUG, BLOB_DEPOT, BDT07, "InitChannelKinds", (Id, GetLogId()));

        TTabletStorageInfo *info = Info();
        const ui32 generation = Executor()->Generation();

        Y_ABORT_UNLESS(Channels.empty());

        ui32 channel = 0;
        for (const auto& profile : Config.GetChannelProfiles()) {
            for (ui32 i = 0, count = profile.GetCount(); i < count; ++i, ++channel) {
                const ui32 groupId = info->GroupFor(channel, generation);
                if (channel >= 2) {
                    const auto kind = profile.GetChannelKind();
                    auto& p = ChannelKinds[kind];
                    p.ChannelToIndex[channel] = p.ChannelGroups.size();
                    p.ChannelGroups.emplace_back(channel, groupId);
                    Channels.push_back({
                        ui8(channel),
                        groupId,
                        kind,
                        &p,
                        {},
                        TBlobSeqId{channel, generation, 1, 0}.ToSequentialNumber(),
                        {},
                        {},
                        {},
                    });
                    Groups[groupId].Channels[kind].push_back(channel);
                } else {
                    Channels.push_back({
                        ui8(channel),
                        groupId,
                        NKikimrBlobDepot::TChannelKind::System,
                        nullptr,
                        {},
                        0,
                        {},
                        {},
                        {},
                    });
                }
            }
        }
    }

    void TBlobDepot::InvalidateGroupForAllocation(ui32 groupId) {
        const auto groupIt = Groups.find(groupId);
        Y_ABORT_UNLESS(groupIt != Groups.end());
        const auto& group = groupIt->second;
        for (const auto& [kind, channels] : group.Channels) {
            const auto kindIt = ChannelKinds.find(kind);
            Y_ABORT_UNLESS(kindIt != ChannelKinds.end());
            auto& kindv = kindIt->second;
            kindv.GroupAccumWeights.clear(); // invalidate
        }
    }

    bool TBlobDepot::PickChannels(NKikimrBlobDepot::TChannelKind::E kind, std::vector<ui8>& channels) {
        const auto kindIt = ChannelKinds.find(kind);
        Y_ABORT_UNLESS(kindIt != ChannelKinds.end());
        auto& kindv = kindIt->second;

        if (kindv.GroupAccumWeights.empty()) {
            for (const bool stopOnLightYellow : {true, false}) {
                // recalculate group weights
                ui64 accum = 0;
                THashSet<ui32> seenGroups;
                for (const auto& [channel, groupId] : kindv.ChannelGroups) {
                    if (const auto& [_, inserted] = seenGroups.insert(groupId); inserted) {
                        if (const ui64 w = SpaceMonitor->GetGroupAllocationWeight(groupId, stopOnLightYellow)) {
                            accum += w;
                            kindv.GroupAccumWeights.emplace_back(groupId, accum);
                        }
                    }
                }
                if (!kindv.GroupAccumWeights.empty()) {
                    break;
                }
            }
            if (kindv.GroupAccumWeights.empty()) {
                return false; // no allocation possible
            }
        }

        const auto [_, accum] = kindv.GroupAccumWeights.back();
        for (ui8& channel : channels) {
            const ui64 random = RandomNumber(accum);
            const auto comp = [](ui64 x, const auto& y) { return x < std::get<1>(y); };
            const auto it = std::upper_bound(kindv.GroupAccumWeights.begin(), kindv.GroupAccumWeights.end(), random, comp);
            Y_ABORT_UNLESS(it != kindv.GroupAccumWeights.end());
            const auto [groupId, _] = *it;

            const auto groupIt = Groups.find(groupId);
            Y_ABORT_UNLESS(groupIt != Groups.end());
            auto& group = groupIt->second;

            const auto channelsIt = group.Channels.find(kind);
            Y_ABORT_UNLESS(channelsIt != group.Channels.end());
            const auto& channels = channelsIt->second;

            const size_t channelIndex = RandomNumber(channels.size());
            channel = channels[channelIndex];
        }

        return true;
    }

    IActor *CreateBlobDepot(const TActorId& tablet, TTabletStorageInfo *info) {
        return new TBlobDepot(tablet, info);
    }

} // NKikimr::NBlobDepot
