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
                const ui64 id_ = id;
                auto tryToProcess = [&](auto& map) {
                    if (const auto it = map.find(id_); it != map.end()) {
                        TBlobDepotAgent::TRequestInFlight& request = it->second;
                        if (request.CancelCallback) {
                            request.CancelCallback();
                        }
                        map.erase(it);
                        return true;
                    } else {
                        return false;
                    }
                };
                const bool success = tryToProcess(Agent.TabletRequestInFlight) || tryToProcess(Agent.OtherRequestInFlight);
                Y_VERIFY(success);
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

    TString TRequestSender::ToString(const TResponse& response) {
        auto printer = [](auto& value) -> TString {
            using T = std::decay_t<decltype(value)>;
            if constexpr (std::is_same_v<T, TTabletDisconnected>) {
                return "TTabletDisconnected";
            } else if constexpr (std::is_same_v<T, TKeyResolved>) {
                return "TKeyResolved";
            } else {
                return value->ToString();
            }
        };
        return std::visit(printer, response);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TBlobDepotAgent machinery

    void TBlobDepotAgent::RegisterRequest(ui64 id, TRequestSender *sender, TRequestContext::TPtr context,
            TRequestInFlight::TCancelCallback cancelCallback, bool toBlobDepotTablet) {
        TRequestsInFlight& map = toBlobDepotTablet ? TabletRequestInFlight : OtherRequestInFlight;
        const auto [_, inserted] = map.emplace(id, TRequestInFlight{sender, std::move(cancelCallback)});
        Y_VERIFY(inserted);
        sender->RegisterRequest(id, std::move(context));
    }

    template<typename TEvent>
    void TBlobDepotAgent::HandleTabletResponse(TAutoPtr<TEventHandle<TEvent>> ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA15, "HandleTabletResponse", (VirtualGroupId, VirtualGroupId),
            (Id, ev->Cookie), (Type, TypeName<TEvent>()));
        OnRequestComplete(ev->Cookie, ev->Get(), TabletRequestInFlight);
    }

    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvRegisterAgentResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvAllocateIdsResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvBlockResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvQueryBlocksResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvCollectGarbageResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvCommitBlobSeqResult::TPtr ev);
    template void TBlobDepotAgent::HandleTabletResponse(TEvBlobDepot::TEvResolveResult::TPtr ev);

    template<typename TEvent>
    void TBlobDepotAgent::HandleOtherResponse(TAutoPtr<TEventHandle<TEvent>> ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA16, "HandleOtherResponse", (VirtualGroupId, VirtualGroupId),
            (Id, ev->Cookie), (Type, TypeName<TEvent>()));
        OnRequestComplete(ev->Cookie, ev->Get(), OtherRequestInFlight);
    }

    template void TBlobDepotAgent::HandleOtherResponse(TEvBlobStorage::TEvGetResult::TPtr ev);
    template void TBlobDepotAgent::HandleOtherResponse(TEvBlobStorage::TEvPutResult::TPtr ev);

    void TBlobDepotAgent::OnRequestComplete(ui64 id, TResponse response, TRequestsInFlight& map) {
        if (const auto it = map.find(id); it != map.end()) {
            TRequestInFlight request = std::move(it->second);
            map.erase(it);
            request.Sender->OnRequestComplete(id, std::move(response));
        }
    }

} // NKikimr::NBlobDepot
