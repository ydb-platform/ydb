#include "agent_impl.h"
#include "blocks.h"
#include <ydb/library/actors/struct_log/create_message_impl.h>
#include <ydb/library/actors/struct_log/create_message_impl.h>

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        auto& msg = *ev->Get();
        YDBLOG_COMP_DEBUG(BLOB_DEPOT_AGENT, "TEvClientConnected", {"Marker", "BDA03"},
            {"AgentId", LogId},
            {"TabletId", msg.TabletId},
            {"Status", msg.Status},
            {"ClientId", msg.ClientId},
            {"ServerId", msg.ServerId});
        Y_VERIFY_DEBUG_S(msg.Status == NKikimrProto::OK, "Status# " << NKikimrProto::EReplyStatus_Name(msg.Status));
        if (msg.Status != NKikimrProto::OK) {
            ConnectToBlobDepot();
        } else {
            PipeServerId = msg.ServerId;
            SwitchMode(EMode::Registering);
        }
    }

    void TBlobDepotAgent::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
        auto& msg = *ev->Get();
        YDBLOG_COMP_INFO(BLOB_DEPOT_AGENT, "TEvClientDestroyed", {"Marker", "BDA04"},
            {"AgentId", LogId},
            {"ClientId", msg.ClientId},
            {"ServerId", msg.ServerId});
        PipeId = PipeServerId = {};
        OnDisconnect();
        ConnectToBlobDepot();
    }

    void TBlobDepotAgent::ConnectToBlobDepot() {
        Y_ABORT_UNLESS(!PipeId);
        PipeId = Register(NTabletPipe::CreateClient(SelfId(), TabletId, NTabletPipe::TClientRetryPolicy::WithRetries()));
        NextTabletRequestId = 1;
        const ui64 id = NextTabletRequestId++;
        YDBLOG_COMP_DEBUG(BLOB_DEPOT_AGENT, "ConnectToBlobDepot", {"Marker", "BDA05"},
            {"AgentId", LogId},
            {"PipeId", PipeId},
            {"RequestId", id});
        NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobDepot::TEvRegisterAgent(VirtualGroupId, AgentInstanceId), id);
        RegisterRequest(id, this, nullptr, {}, true);
        SwitchMode(EMode::ConnectPending);
    }

    void TBlobDepotAgent::Handle(TRequestContext::TPtr /*context*/, NKikimrBlobDepot::TEvRegisterAgentResult& msg) {
        YDBLOG_COMP_DEBUG(BLOB_DEPOT_AGENT, "TEvRegisterAgentResult", {"Marker", "BDA06"},
            {"AgentId", LogId},
            {"Msg", msg});
        BlobDepotGeneration = msg.GetGeneration();
        DecommitGroupId = msg.HasDecommitGroupId() ? std::make_optional(msg.GetDecommitGroupId()) : std::nullopt;

        THashSet<NKikimrBlobDepot::TChannelKind::E> vanishedKinds;
        for (const auto& [kind, _] : ChannelKinds) {
            vanishedKinds.insert(kind);
        }

        ChannelToKind.clear();

        for (const auto& ch : msg.GetChannelKinds()) {
            const NKikimrBlobDepot::TChannelKind::E kind = ch.GetChannelKind();
            vanishedKinds.erase(kind);

            auto [it, inserted] = ChannelKinds.try_emplace(kind, kind);
            auto& v = it->second;

            v.ChannelToIndex.fill(0);
            v.ChannelGroups.clear();

            for (const auto& channelGroup : ch.GetChannelGroups()) {
                const ui8 channel = channelGroup.GetChannel();
                const ui32 groupId = channelGroup.GetGroupId();
                v.ChannelToIndex[channel] = v.ChannelGroups.size();
                v.ChannelGroups.emplace_back(channel, groupId);
                ChannelToKind[channel] = &v;
            }
        }

        for (const NKikimrBlobDepot::TChannelKind::E kind : vanishedKinds) {
            YDBLOG_COMP_INFO(BLOB_DEPOT_AGENT, "kind vanished", {"Marker", "BDA07"},
                {"AgentId", LogId},
                {"Kind", kind});
            ChannelKinds.erase(kind);
        }

        for (const auto& [channel, kind] : ChannelToKind) {
            kind->Trim(channel, BlobDepotGeneration - 1, Max<ui32>());

            auto& wif = kind->WritesInFlight;
            const TBlobSeqId min{channel, 0, 0, 0};
            const TBlobSeqId max{channel, BlobDepotGeneration - 1, Max<ui32>(), TBlobSeqId::MaxIndex};
            wif.erase(wif.lower_bound(min), wif.upper_bound(max));
        }

        for (auto& [_, kind] : ChannelKinds) {
            IssueAllocateIdsIfNeeded(kind);
        }

        SpaceColor = msg.GetSpaceColor();
        ApproximateFreeSpaceShare = msg.GetApproximateFreeSpaceShare();

        S3BackendSettings = msg.HasS3BackendSettings()
            ? std::make_optional(msg.GetS3BackendSettings())
            : std::nullopt;

        if (S3WrapperId) {
            TActivationContext::Send(new IEventHandle(TEvents::TSystem::Poison, 0, S3WrapperId, SelfId(), nullptr, 0));
            S3WrapperId = {};
        }

#ifndef KIKIMR_DISABLE_S3_OPS
        InitS3(msg.GetName());
#endif

        OnConnect();
    }

    void TBlobDepotAgent::IssueAllocateIdsIfNeeded(TChannelKind& kind) {
        if (!kind.IdAllocInFlight && kind.GetNumAvailableItems() < 100 && IsConnected) {
            const ui64 id = NextTabletRequestId++;
            YDBLOG_COMP_DEBUG(BLOB_DEPOT_AGENT, "IssueAllocateIdsIfNeeded", {"Marker", "BDA08"},
                {"AgentId", LogId},
                {"ChannelKind", NKikimrBlobDepot::TChannelKind::E_Name(kind.Kind)},
                {"IdAllocInFlight", kind.IdAllocInFlight},
                {"NumAvailableItems", kind.GetNumAvailableItems()},
                {"RequestId", id});
            NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobDepot::TEvAllocateIds(kind.Kind, 100), id);
            RegisterRequest(id, this, std::make_shared<TAllocateIdsContext>(kind.Kind), {}, true);
            kind.IdAllocInFlight = true;
        }
    }

    void TBlobDepotAgent::Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvAllocateIdsResult& msg) {
        auto& allocateIdsContext = context->Obtain<TAllocateIdsContext>();
        const auto it = ChannelKinds.find(allocateIdsContext.ChannelKind);
        Y_VERIFY_S(it != ChannelKinds.end(), "Kind# " << NKikimrBlobDepot::TChannelKind::E_Name(allocateIdsContext.ChannelKind)
            << " Msg# " << SingleLineProto(msg));
        auto& kind = it->second;

        Y_ABORT_UNLESS(kind.IdAllocInFlight);
        kind.IdAllocInFlight = false;

        Y_ABORT_UNLESS(msg.GetChannelKind() == allocateIdsContext.ChannelKind);
        Y_ABORT_UNLESS(msg.GetGeneration() == BlobDepotGeneration);

        if (msg.HasGivenIdRange()) {
            kind.IssueGivenIdRange(msg.GetGivenIdRange());
        } else {
            kind.ProcessQueriesWaitingForId(false);
        }

        YDBLOG_COMP_DEBUG(BLOB_DEPOT_AGENT, "TEvAllocateIdsResult", {"Marker", "BDA09"},
            {"AgentId", LogId},
            {"Msg", msg},
            {"NumAvailableItems", kind.GetNumAvailableItems()});
    }

    void TBlobDepotAgent::OnConnect() {
        IsConnected = true;
        SwitchMode(EMode::Connected);

        HandlePendingEvent();
    }

    void TBlobDepotAgent::OnDisconnect() {
        ++ConnectionInstance;

        while (!TabletRequestInFlight.empty()) {
            auto node = TabletRequestInFlight.extract(TabletRequestInFlight.begin());
            auto& requestInFlight = node.value();
            requestInFlight.Sender->OnRequestComplete(requestInFlight, TTabletDisconnected{}, nullptr);
        }

        for (auto& [_, kind] : ChannelKinds) {
            kind.IdAllocInFlight = false;
        }

        ClearPendingEventQueue("BlobDepot tablet disconnected");

        SwitchMode(EMode::None);
        IsConnected = false;
    }

    void TBlobDepotAgent::ProcessResponse(ui64 /*id*/, TRequestContext::TPtr context, TResponse response) {
        std::visit([&](auto&& response) {
            using T = std::decay_t<decltype(response)>;
            if constexpr (std::is_same_v<T, TEvBlobDepot::TEvRegisterAgentResult*>
                    || std::is_same_v<T, TEvBlobDepot::TEvAllocateIdsResult*>) {
                Handle(std::move(context), response->Record);
            } else if constexpr (!std::is_same_v<T, TTabletDisconnected>) {
                Y_FAIL_S("unexpected response received Type# " << TypeName<T>());
            }
        }, response);
    }

    template<typename T, typename TEvent>
    ui64 TBlobDepotAgent::Issue(T msg, TRequestSender *sender, TRequestContext::TPtr context) {
        auto ev = std::make_unique<TEvent>();
        msg.Swap(&ev->Record);
        return Issue(std::move(ev), sender, std::move(context));
    }

    template ui64 TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvCollectGarbage msg, TRequestSender *sender, TRequestContext::TPtr context);
    template ui64 TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvQueryBlocks msg, TRequestSender *sender, TRequestContext::TPtr context);
    template ui64 TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvBlock msg, TRequestSender *sender, TRequestContext::TPtr context);
    template ui64 TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvResolve msg, TRequestSender *sender, TRequestContext::TPtr context);
    template ui64 TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvCommitBlobSeq msg, TRequestSender *sender, TRequestContext::TPtr context);
    template ui64 TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvDiscardSpoiledBlobSeq msg, TRequestSender *sender, TRequestContext::TPtr context);
    template ui64 TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvPrepareWriteS3 msg, TRequestSender *sender, TRequestContext::TPtr context);

    ui64 TBlobDepotAgent::Issue(std::unique_ptr<IEventBase> ev, TRequestSender *sender, TRequestContext::TPtr context) {
        const ui64 id = NextTabletRequestId++;
        YDBLOG_COMP_DEBUG(BLOB_DEPOT_AGENT, "Issue", {"Marker", "BDA10"},
            {"AgentId", LogId},
            {"RequestId", id},
            {"Msg", ev->ToString()});
        NTabletPipe::SendData(SelfId(), PipeId, ev.release(), id);
        RegisterRequest(id, sender, std::move(context), {}, true);
        return id;
    }

    void TBlobDepotAgent::Handle(TEvBlobDepot::TEvPushNotify::TPtr ev) {
        auto& msg = ev->Get()->Record;
        YDBLOG_COMP_DEBUG(BLOB_DEPOT_AGENT, "TEvPushNotify", {"Marker", "BDA11"},
            {"AgentId", LogId},
            {"Msg", msg},
            {"Id", ev->Cookie},
            {"Sender", ev->Sender},
            {"PipeServerId", PipeServerId},
            {"Match", ev->Sender == PipeServerId});
        if (ev->Sender != PipeServerId) {
            return; // race with previous connection
        }

        auto response = std::make_unique<TEvBlobDepot::TEvPushNotifyResult>();
        response->Record.SetId(ev->Cookie);

        BlocksManager.OnBlockedTablets(msg.GetBlockedTablets());

        for (const auto& item : msg.GetInvalidatedSteps()) {
            const ui8 channel = item.GetChannel();
            Y_ABORT_UNLESS(item.GetGeneration() == BlobDepotGeneration);
            const auto it = ChannelToKind.find(channel);
            Y_ABORT_UNLESS(it != ChannelToKind.end());
            TChannelKind& kind = *it->second;
            const ui32 numAvailableItemsBefore = kind.GetNumAvailableItems();
            kind.Trim(channel, item.GetGeneration(), item.GetInvalidatedStep());

            // report writes in flight that are trimmed
            const TBlobSeqId first{channel, item.GetGeneration(), 0, 0};
            const TBlobSeqId last{channel, item.GetGeneration(), item.GetInvalidatedStep(), Max<ui32>()};
            for (auto it = kind.WritesInFlight.lower_bound(first); it != kind.WritesInFlight.end() && *it <= last; ++it) {
                it->ToProto(response->Record.AddWritesInFlight());
            }

            YDBLOG_COMP_DEBUG(BLOB_DEPOT_AGENT, "TrimChannel", {"Marker", "BDA12"},
                {"AgentId", LogId},
                {"Channel", int(channel)},
                {"NumAvailableItemsBefore", numAvailableItemsBefore},
                {"NumAvailableItemsAfter", kind.GetNumAvailableItems()});
        }

        if (msg.HasSpaceColor()) {
            SpaceColor = msg.GetSpaceColor();
        }
        if (msg.HasApproximateFreeSpaceShare()) {
            ApproximateFreeSpaceShare = msg.GetApproximateFreeSpaceShare();
        }

        // it is essential to send response through the pipe -- otherwise we can break order with, for example, commits:
        // this message can outrun previously sent commit and lead to data loss
        YDBLOG_COMP_DEBUG(BLOB_DEPOT_AGENT, "sending TEvPushNotifyResult", {"Marker", "BDA33"},
            {"AgentId", LogId},
            {"RequestId", NextTabletRequestId});
        NTabletPipe::SendData(SelfId(), PipeId, response.release(), NextTabletRequestId++);

        for (auto& [_, kind] : ChannelKinds) {
            IssueAllocateIdsIfNeeded(kind);
        }
    }

} // NKikimr::NBlobDepot
