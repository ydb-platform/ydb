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
        NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobDepot::TEvRegisterAgent(VirtualGroupId), id);
        RegisterRequest(id, this, nullptr, true);
        IssueAllocateIdsIfNeeded(NKikimrBlobDepot::TChannelKind::Data);
        IssueAllocateIdsIfNeeded(NKikimrBlobDepot::TChannelKind::Log);
    }

    void TBlobDepotAgent::Handle(TRequestContext::TPtr /*context*/, NKikimrBlobDepot::TEvRegisterAgentResult& msg) {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA04, "TEvRegisterAgentResult", (VirtualGroupId, VirtualGroupId),
            (Msg, msg));
        Registered = true;
        BlobDepotGeneration = msg.GetGeneration();
        for (const auto& kind : msg.GetChannelKinds()) {
            auto& v = ChannelKinds[kind.GetChannelKind()];
            v.ChannelGroups.clear();
            v.IndexToChannel.clear();
            for (const auto& channelGroup : kind.GetChannelGroups()) {
                const ui8 channel = channelGroup.GetChannel();
                const ui32 groupId = channelGroup.GetGroupId();
                v.ChannelGroups.emplace_back(channel, groupId);
                v.ChannelToIndex[channel] = v.IndexToChannel.size();
                v.IndexToChannel.push_back(channel);
            }
        }
    }

    void TBlobDepotAgent::IssueAllocateIdsIfNeeded(NKikimrBlobDepot::TChannelKind::E channelKind) {
        auto& kind = ChannelKinds[channelKind];

        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA05, "IssueAllocateIdsIfNeeded", (VirtualGroupId, VirtualGroupId),
            (ChannelKind, NKikimrBlobDepot::TChannelKind::E_Name(channelKind)),
            (IdAllocInFlight, kind.IdAllocInFlight), (IdQ.size, kind.IdQ.size()),
            (PreallocatedIdCount, kind.PreallocatedIdCount), (PipeId, PipeId));
        if (!kind.IdAllocInFlight && kind.IdQ.size() < kind.PreallocatedIdCount && PipeId) {
            const ui64 id = NextRequestId++;
            NTabletPipe::SendData(SelfId(), PipeId, new TEvBlobDepot::TEvAllocateIds(channelKind), id);
            RegisterRequest(id, this, std::make_shared<TAllocateIdsContext>(channelKind), true);
            kind.IdAllocInFlight = true;
        }
    }

    void TBlobDepotAgent::Handle(TRequestContext::TPtr context, NKikimrBlobDepot::TEvAllocateIdsResult& msg) {
        auto& allocateIdsContext = context->Obtain<TAllocateIdsContext>();
        auto& kind = ChannelKinds[allocateIdsContext.ChannelKind];

        Y_VERIFY(kind.IdAllocInFlight);
        kind.IdAllocInFlight = false;

        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA06, "TEvAllocateIdsResult", (VirtualGroupId, VirtualGroupId),
            (Msg, msg));
        Y_VERIFY(msg.GetChannelKind() == allocateIdsContext.ChannelKind);
        Y_VERIFY(msg.GetGeneration() == BlobDepotGeneration);

        if (msg.HasRangeBegin() && msg.HasRangeEnd()) {
            kind.IdQ.push_back({BlobDepotGeneration, msg.GetRangeBegin(), msg.GetRangeEnd()});

            // FIXME notify waiting requests about new ids

            // ask for more ids if needed
            IssueAllocateIdsIfNeeded(allocateIdsContext.ChannelKind);
        } else {
            // no such channel allocated
        }
    }

    void TBlobDepotAgent::OnDisconnect() {
        STLOG(PRI_INFO, BLOB_DEPOT_AGENT, BDA07, "OnDisconnect", (VirtualGroupId, VirtualGroupId));

        for (auto& [id, sender] : std::exchange(TabletRequestInFlight, {})) {
            sender->OnRequestComplete(id, TTabletDisconnected{});
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

    void TBlobDepotAgent::Issue(std::unique_ptr<IEventBase> ev, TRequestSender *sender, TRequestContext::TPtr context) {
        const ui64 id = NextRequestId++;
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA08, "Issue", (VirtualGroupId, VirtualGroupId), (Id, id), (Msg, ev->ToString()));
        NTabletPipe::SendData(SelfId(), PipeId, ev.release(), id);
        RegisterRequest(id, sender, std::move(context), true);
    }

} // NKikimr::NBlobDepot
