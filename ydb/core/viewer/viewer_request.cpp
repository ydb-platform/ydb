#include "viewer_request.h"
#include "viewer_autocomplete.h"
#include "viewer_query_old.h"
#include "viewer_render.h"
#include "viewer_sysinfo.h"
#include "viewer_tabletinfo.h"
#include "wb_req.h"

namespace NKikimr::NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

template<typename TRequestEventType, typename TResponseEventType>
class TViewerWhiteboardRequest : public TWhiteboardRequest<TRequestEventType, TResponseEventType> {
protected:
    using TThis = TViewerWhiteboardRequest<TRequestEventType, TResponseEventType>;
    using TBase = TWhiteboardRequest<TRequestEventType, TResponseEventType>;
    using TResponseType = typename TResponseEventType::ProtoRecordType;
    IViewer* Viewer;
    TEvViewer::TEvViewerRequest::TPtr Event;
    TJsonSettings JsonSettings;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TViewerWhiteboardRequest(TEvViewer::TEvViewerRequest::TPtr& ev)
        : TBase(std::move(ev->TraceId))
        , Event(ev)
    {
    }

    void Bootstrap() override {
        TBase::RequestSettings.MergeFields = TWhiteboardInfo<TResponseType>::GetDefaultMergeField();
        TBase::RequestSettings.Timeout = Event->Get()->Record.GetTimeout();
        for (TNodeId nodeId : Event->Get()->Record.GetLocation().GetNodeId()) {
            TBase::RequestSettings.FilterNodeIds.push_back(nodeId);
        }
        TBase::Bootstrap();
    }

    template<typename ResponseType> void MergeWhiteboardResponses(TEvViewer::TEvViewerResponse* response, TMap<TNodeId, ResponseType>& perNodeStateInfo, const TString& fields);

    template<> void MergeWhiteboardResponses<NKikimrWhiteboard::TEvTabletStateResponse>(TEvViewer::TEvViewerResponse* response, TMap<TNodeId, NKikimrWhiteboard::TEvTabletStateResponse>& perNodeStateInfo, const TString& fields) {
        NKikimr::NViewer::MergeWhiteboardResponses(*(response->Record.MutableTabletResponse()), perNodeStateInfo, fields);
    }

    template<> void MergeWhiteboardResponses<NKikimrWhiteboard::TEvSystemStateResponse>(TEvViewer::TEvViewerResponse* response, TMap<TNodeId, NKikimrWhiteboard::TEvSystemStateResponse>& perNodeStateInfo, const TString& fields) {
        NKikimr::NViewer::MergeWhiteboardResponses(*(response->Record.MutableSystemResponse()), perNodeStateInfo, fields);
    }

    void ReplyAndPassAway() override {
        auto response = MakeHolder<TEvViewer::TEvViewerResponse>();
        auto& locationResponded = (*response->Record.MutableLocationResponded());
        auto perNodeStateInfo = TBase::GetPerNodeStateInfo();
        for (const auto& [nodeId, nodeResponse] : perNodeStateInfo) {
            locationResponded.AddNodeId(nodeId);
        }

        MergeWhiteboardResponses(response.Get(), perNodeStateInfo, TBase::RequestSettings.MergeFields);

        TBase::Send(Event->Sender, response.Release(), 0, Event->Cookie);
        TBase::PassAway();
    }
};

IActor* CreateViewerRequestHandler(TEvViewer::TEvViewerRequest::TPtr& request) {
    switch (request->Get()->Record.GetRequestCase()) {
        case NKikimrViewer::TEvViewerRequest::kTabletRequest:
            return new TViewerWhiteboardRequest<TEvWhiteboard::TEvTabletStateRequest, TEvWhiteboard::TEvTabletStateResponse>(request);
        case NKikimrViewer::TEvViewerRequest::kSystemRequest:
            return new TViewerWhiteboardRequest<TEvWhiteboard::TEvSystemStateRequest, TEvWhiteboard::TEvSystemStateResponse>(request);
        case NKikimrViewer::TEvViewerRequest::kQueryRequest:
            return new TJsonQueryOld(request);
        case NKikimrViewer::TEvViewerRequest::kRenderRequest:
            return new TJsonRender(request);
        case NKikimrViewer::TEvViewerRequest::kAutocompleteRequest:
            return new TJsonAutocomplete(request);
        case NKikimrViewer::TEvViewerRequest::kReserved16:
        case NKikimrViewer::TEvViewerRequest::kReserved17:
        case NKikimrViewer::TEvViewerRequest::kReserved18:
        case NKikimrViewer::TEvViewerRequest::REQUEST_NOT_SET:
            return nullptr;
    }
    return nullptr;
}

bool IsPostContent(const NMon::TEvHttpInfo::TPtr& event) {
    if (event->Get()->Request.GetMethod() == HTTP_METHOD_POST) {
        const THttpHeaders& headers = event->Get()->Request.GetHeaders();

        auto itContentType = FindIf(headers, [](const auto& header) {
            return AsciiEqualsIgnoreCase(header.Name(),  "Content-Type");
        });

        if (itContentType != headers.end()) {
            TStringBuf contentTypeHeader = itContentType->Value();
            TStringBuf contentType = contentTypeHeader.NextTok(';');
            return contentType == "application/json";
        }
    }
    return false;
}

}
