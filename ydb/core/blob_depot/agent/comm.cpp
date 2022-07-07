#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    void TBlobDepotAgent::Handle(TEvTabletPipe::TEvClientConnected::TPtr ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA02, "TEvClientConnected", (VirtualGroupId, VirtualGroupId),
            (Msg, ev->Get()->ToString()));
    }

    void TBlobDepotAgent::Handle(TEvTabletPipe::TEvClientDestroyed::TPtr ev) {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA03, "TEvClientDestroyed", (VirtualGroupId, VirtualGroupId),
            (Msg, ev->Get()->ToString()));
        PipeId = {};
        OnDisconnect();
        ConnectToBlobDepot();
    }

    void TBlobDepotAgent::ConnectToBlobDepot() {
        PipeId = Register(NTabletPipe::CreateClient(SelfId(), TabletId, NTabletPipe::TClientRetryPolicy::WithRetries()));
        const ui64 id = NextRequestId++;
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA04, "ConnectToBlobDepot", (VirtualGroupId, VirtualGroupId),
            (PipeId, PipeId), (RequestId, id));
        NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobDepot::TEvRegisterAgent(VirtualGroupId), id);
        RegisterRequest(id, this, nullptr, {}, true);
    }

    void TBlobDepotAgent::Handle(TRequestContext::TPtr /*context*/, NKikimrBlobDepot::TEvRegisterAgentResult& msg) {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA04, "TEvRegisterAgentResult", (VirtualGroupId, VirtualGroupId),
            (Msg, msg));
        Registered = true;
        BlobDepotGeneration = msg.GetGeneration();

        THashSet<NKikimrBlobDepot::TChannelKind::E> vanishedKinds;
        for (const auto& [kind, _] : ChannelKinds) {
            vanishedKinds.insert(kind);
        }

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
            }

            IssueAllocateIdsIfNeeded(v);
        }

        for (const NKikimrBlobDepot::TChannelKind::E kind : vanishedKinds) {
            STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA05, "kind vanished", (VirtualGroupId, VirtualGroupId), (Kind, kind));
            ChannelKinds.erase(kind);
        }
    }

    void TBlobDepotAgent::IssueAllocateIdsIfNeeded(TChannelKind& kind) {
        if (!kind.IdAllocInFlight && kind.GivenIdRange.GetNumAvailableItems() < 100 && PipeId) {
            const ui64 id = NextRequestId++;
            STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA05, "IssueAllocateIdsIfNeeded", (VirtualGroupId, VirtualGroupId),
                (ChannelKind, NKikimrBlobDepot::TChannelKind::E_Name(kind.Kind)),
                (IdAllocInFlight, kind.IdAllocInFlight), (NumAvailableItems, kind.GivenIdRange.GetNumAvailableItems()),
                (PreallocatedIdCount, kind.PreallocatedIdCount), (RequestId, id));
            NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobDepot::TEvAllocateIds(kind.Kind, 100), id);
            RegisterRequest(id, this, std::make_shared<TAllocateIdsContext>(kind.Kind), {}, true);
            kind.IdAllocInFlight = true;
        }
    }

    void TBlobDepotAgent::Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvAllocateIdsResult& msg) {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA06, "TEvAllocateIdsResult", (VirtualGroupId, VirtualGroupId),
            (Msg, msg));

        auto& allocateIdsContext = context->Obtain<TAllocateIdsContext>();
        const auto it = ChannelKinds.find(allocateIdsContext.ChannelKind);
        Y_VERIFY_S(it != ChannelKinds.end(), "Kind# " << NKikimrBlobDepot::TChannelKind::E_Name(allocateIdsContext.ChannelKind)
            << " Msg# " << SingleLineProto(msg));
        auto& kind = it->second;

        Y_VERIFY(kind.IdAllocInFlight);
        kind.IdAllocInFlight = false;

        Y_VERIFY(msg.GetChannelKind() == allocateIdsContext.ChannelKind);
        Y_VERIFY(msg.GetGeneration() == BlobDepotGeneration);

        if (msg.HasGivenIdRange()) {
            TGivenIdRange range(msg.GetGivenIdRange());
            kind.GivenIdRange.Join(std::move(range));
            kind.ProcessQueriesWaitingForId();
            IssueAllocateIdsIfNeeded(kind);
        }
    }

    void TBlobDepotAgent::OnDisconnect() {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA07, "OnDisconnect", (VirtualGroupId, VirtualGroupId));

        for (auto& [id, request] : std::exchange(TabletRequestInFlight, {})) {
            request.Sender->OnRequestComplete(id, TTabletDisconnected{});
        }

        for (auto& [_, kind] : ChannelKinds) {
            kind.IdAllocInFlight = false;
        }

        Registered = false;
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

    void TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvBlock msg, TRequestSender *sender, TRequestContext::TPtr context) {
        auto ev = std::make_unique<TEvBlobDepot::TEvBlock>();
        msg.Swap(&ev->Record);
        Issue(std::move(ev), sender, std::move(context));
    }

    void TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvResolve msg, TRequestSender *sender, TRequestContext::TPtr context) {
        auto ev = std::make_unique<TEvBlobDepot::TEvResolve>();
        msg.Swap(&ev->Record);
        Issue(std::move(ev), sender, std::move(context));
    }

    void TBlobDepotAgent::Issue(NKikimrBlobDepot::TEvCommitBlobSeq msg, TRequestSender *sender, TRequestContext::TPtr context) {
        auto ev = std::make_unique<TEvBlobDepot::TEvCommitBlobSeq>();
        msg.Swap(&ev->Record);
        Issue(std::move(ev), sender, std::move(context));
    }

    void TBlobDepotAgent::Issue(std::unique_ptr<IEventBase> ev, TRequestSender *sender, TRequestContext::TPtr context) {
        const ui64 id = NextRequestId++;
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA08, "Issue", (VirtualGroupId, VirtualGroupId), (Id, id), (Msg, ev->ToString()));
        NTabletPipe::SendData(SelfId(), PipeId, ev.release(), id);
        RegisterRequest(id, sender, std::move(context), {}, true);
    }

    void TBlobDepotAgent::Handle(TEvBlobDepot::TEvPushNotify::TPtr ev) {
        auto& msg = ev->Get()->Record;
        OnBlockedTablets(msg.GetBlockedTablets());

        auto [response, _] = TEvBlobDepot::MakeResponseFor(*ev, SelfId());
        TActivationContext::Send(response.release());
    }

} // NKikimr::NBlobDepot
