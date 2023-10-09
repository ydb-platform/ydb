#include "agent_impl.h"

namespace NKikimr::NBlobDepot {

    TRequestInFlight::TRequestInFlight(ui64 id, TRequestSender *sender, TRequestContext::TPtr context,
            TCancelCallback cancelCallback, bool toBlobDepotTablet)
        : Id(id)
        , Sender(sender)
        , Context(std::move(context))
        , CancelCallback(std::move(cancelCallback))
        , ToBlobDepotTablet(toBlobDepotTablet)
    {
        Sender->RegisterRequestInFlight(this);
    }

    ////////////////////////////////////////////////////////////////////////////////////////////////////////////////////
    // TRequestSender class

    TRequestSender::TRequestSender(TBlobDepotAgent& agent)
        : Agent(agent)
    {}

    TRequestSender::~TRequestSender() {
        ClearRequestsInFlight();
    }

    void TRequestSender::ClearRequestsInFlight() {
        RequestsInFlight.ForEach([this](TRequestInFlight *requestInFlight) {
            auto& map = requestInFlight->ToBlobDepotTablet ? Agent.TabletRequestInFlight : Agent.OtherRequestInFlight;
            auto node = map.extract(requestInFlight->Id);
            Y_ABORT_UNLESS(node);

            if (requestInFlight->CancelCallback) {
                requestInFlight->CancelCallback();
            }
        });
    }

    void TRequestSender::OnRequestComplete(TRequestInFlight& requestInFlight, TResponse response,
            std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay) {
        if (executionRelay) {
            const size_t num = SubrequestRelays.erase(executionRelay);
            Y_ABORT_UNLESS(num);
        }
        requestInFlight.Unlink();
        ProcessResponse(requestInFlight.Id, std::move(requestInFlight.Context), std::move(response));
    }

    void TRequestSender::RegisterRequestInFlight(TRequestInFlight *requestInFlight) {
        RequestsInFlight.PushBack(requestInFlight);
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
            TRequestInFlight::TCancelCallback cancelCallback, bool toBlobDepotTablet,
            std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay) {
        TRequestsInFlight& map = toBlobDepotTablet ? TabletRequestInFlight : OtherRequestInFlight;
        const bool inserted = map.emplace(id, sender, std::move(context), std::move(cancelCallback),
            toBlobDepotTablet).second;
        Y_ABORT_UNLESS(inserted);
        if (executionRelay) {
            sender->SubrequestRelays.emplace(executionRelay);
        }
    }

    template<typename TEvent>
    void TBlobDepotAgent::HandleTabletResponse(TAutoPtr<TEventHandle<TEvent>> ev) {
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA16, "HandleTabletResponse", (AgentId, LogId),
            (Id, ev->Cookie), (Type, TypeName<TEvent>()), (Sender, ev->Sender), (PipeServerId, PipeServerId),
            (Match, ev->Sender == PipeServerId));
        if (ev->Sender == PipeServerId) {
            Y_ABORT_UNLESS(IsConnected || ev->GetTypeRewrite() == TEvBlobDepot::EvRegisterAgentResult);
            OnRequestComplete(ev->Cookie, ev->Get(), TabletRequestInFlight);
        }
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
        STLOG(PRI_DEBUG, BLOB_DEPOT_AGENT, BDA17, "HandleOtherResponse", (AgentId, LogId), (Id, ev->Cookie),
            (Type, TypeName<TEvent>()), (Msg, *ev->Get()));
        Y_ABORT_UNLESS(ev->Get()->ExecutionRelay);
        OnRequestComplete(ev->Cookie, ev->Get(), OtherRequestInFlight, std::move(ev->Get()->ExecutionRelay));
    }

    template void TBlobDepotAgent::HandleOtherResponse(TEvBlobStorage::TEvGetResult::TPtr ev);
    template void TBlobDepotAgent::HandleOtherResponse(TEvBlobStorage::TEvPutResult::TPtr ev);

    void TBlobDepotAgent::OnRequestComplete(ui64 id, TResponse response, TRequestsInFlight& map,
            std::shared_ptr<TEvBlobStorage::TExecutionRelay> executionRelay) {
        if (auto node = map.extract(id)) {
            auto& requestInFlight = node.value();
            requestInFlight.Sender->OnRequestComplete(requestInFlight, std::move(response), std::move(executionRelay));
        }
    }

    void TBlobDepotAgent::DropTabletRequest(ui64 id) {
        const size_t numErased = TabletRequestInFlight.erase(id);
        Y_ABORT_UNLESS(numErased == 1);
    }

} // NKikimr::NBlobDepot
