#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TRequestSender class

    TRequestSender::TRequestSender(TBlobDepotAgent& agent)
        : Agent(agent)
    {}

    TRequestSender::~TRequestSender() {
        if (this != &Agent) {
            for (const auto& [id, context] : RequestsInFlight) {
                const size_t numErased = Agent.TabletRequestInFlight.erase(id) + Agent.OtherRequestInFlight.erase(id);
                Y_VERIFY(numErased == 1);
            }
        }
    }

    void TRequestSender::RegisterRequest(ui64 id, TRequestContext::TPtr context) {
        const auto [_, inserted] = RequestsInFlight.emplace(id, std::move(context));
        Y_VERIFY(inserted);
    }

    void TRequestSender::OnRequestComplete(ui64 id, TResponse response) {
        const auto it = RequestsInFlight.find(id);
        Y_VERIFY(it != RequestsInFlight.end());
        TRequestContext::TPtr context = std::move(it->second);
        RequestsInFlight.erase(it);
        ProcessResponse(id, std::move(context), std::move(response));
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TBlobDepotAgent machinery

    void TBlobDepotAgent::RegisterRequest(ui64 id, TRequestSender *sender, TRequestContext::TPtr context, bool toBlobDepotTablet) {
        auto& map = toBlobDepotTablet ? TabletRequestInFlight : OtherRequestInFlight;
        const auto [_, inserted] = map.emplace(id, sender);
        Y_VERIFY(inserted);
        sender->RegisterRequest(id, std::move(context));
    }

    template<typename TEvent>
    void TBlobDepotAgent::HandleTabletResponse(TAutoPtr<TEventHandle<TEvent>> ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA12, "HandleTabletResponse", (VirtualGroupId, VirtualGroupId),
            (Id, ev->Cookie), (Type, TypeName<TEvent>()));
        auto *event = ev->Get();
        HandleResponse(reinterpret_cast<TAutoPtr<IEventHandle>&>(ev), event, TabletRequestInFlight);
    }

    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvRegisterAgentResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvAllocateIdsResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvBlockResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvQueryBlocksResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvCommitBlobSeqResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvResolveResult::TPtr ev);

    template<typename TEvent>
    void TBlobDepotAgent::HandleOtherResponse(TAutoPtr<TEventHandle<TEvent>> ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA13, "HandleOtherResponse", (VirtualGroupId, VirtualGroupId),
            (Id, ev->Cookie), (Type, TypeName<TEvent>()));
        auto *event = ev->Get();
        HandleResponse(ev, event, OtherRequestInFlight);
    }

    void TBlobDepotAgent::HandleResponse(TAutoPtr<IEventHandle> ev, TResponse response, THashMap<ui64, TRequestSender*>& map) {
        const auto it = map.find(ev->Cookie);
        Y_VERIFY(it != map.end());
        const auto [id, sender] = *it;
        map.erase(it);
        sender->OnRequestComplete(id, std::move(response));
    }

} // NKikimr::NBlobDepot
