#include "viewer_request.h"
#include "viewer_autocomplete.h"
#include "viewer_query_old.h"
#include "viewer_render.h"
#include "viewer_sysinfo.h"
#include "viewer_tabletinfo.h"
#include "viewer_vdiskinfo.h"
#include "viewer_pdiskinfo.h"
#include "viewer_bsgroupinfo.h"
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
        TBase::RequestSettings.MergeFields = Event->Get()->Record.GetMergeFields();
        TBase::RequestSettings.Timeout = Event->Get()->Record.GetTimeout();
        for (TNodeId nodeId : Event->Get()->Record.GetLocation().GetNodeId()) {
            TBase::RequestSettings.FilterNodeIds.push_back(nodeId);
        }
        TBase::Bootstrap();
    }

    THolder<TRequestEventType> BuildRequest() override;

    template<typename ResponseType>
    void MergeWhiteboardResponses(TEvViewer::TEvViewerResponse* response, TMap<TNodeId, ResponseType>& perNodeStateInfo, const TString& fields);

    template<>
    void MergeWhiteboardResponses<NKikimrWhiteboard::TEvTabletStateResponse>(TEvViewer::TEvViewerResponse* response, TMap<TNodeId, NKikimrWhiteboard::TEvTabletStateResponse>& perNodeStateInfo, const TString& fields) {
        NKikimr::NViewer::MergeWhiteboardResponses(*(response->Record.MutableTabletResponse()), perNodeStateInfo, fields);
    }

    template<>
    void MergeWhiteboardResponses<NKikimrWhiteboard::TEvSystemStateResponse>(TEvViewer::TEvViewerResponse* response, TMap<TNodeId, NKikimrWhiteboard::TEvSystemStateResponse>& perNodeStateInfo, const TString& fields) {
        NKikimr::NViewer::MergeWhiteboardResponses(*(response->Record.MutableSystemResponse()), perNodeStateInfo, fields);
    }

    template<>
    void MergeWhiteboardResponses<NKikimrWhiteboard::TEvVDiskStateResponse>(TEvViewer::TEvViewerResponse* response, TMap<TNodeId, NKikimrWhiteboard::TEvVDiskStateResponse>& perNodeStateInfo, const TString& fields) {
        NKikimr::NViewer::MergeWhiteboardResponses(*(response->Record.MutableVDiskResponse()), perNodeStateInfo, fields);
    }

    template<>
    void MergeWhiteboardResponses<NKikimrWhiteboard::TEvPDiskStateResponse>(TEvViewer::TEvViewerResponse* response, TMap<TNodeId, NKikimrWhiteboard::TEvPDiskStateResponse>& perNodeStateInfo, const TString& fields) {
        NKikimr::NViewer::MergeWhiteboardResponses(*(response->Record.MutablePDiskResponse()), perNodeStateInfo, fields);
    }

    template<>
    void MergeWhiteboardResponses<NKikimrWhiteboard::TEvBSGroupStateResponse>(TEvViewer::TEvViewerResponse* response, TMap<TNodeId, NKikimrWhiteboard::TEvBSGroupStateResponse>& perNodeStateInfo, const TString& fields) {
        NKikimr::NViewer::MergeWhiteboardResponses(*(response->Record.MutableBSGroupResponse()), perNodeStateInfo, fields);
    }

    static void Merge(NKikimrViewer::TEvViewerResponse& viewerResponse, TNodeId nodeId, TResponseType& nodeResponse);

    void ReplyAndPassAway() override {
        auto response = MakeHolder<TEvViewer::TEvViewerResponse>();
        auto& locationResponded = (*response->Record.MutableLocationResponded());

        if (TBase::RequestSettings.MergeFields) {
            auto perNodeStateInfo = TBase::GetPerNodeStateInfo();
            for (const auto& [nodeId, nodeResponse] : perNodeStateInfo) {
                locationResponded.AddNodeId(nodeId);
            }
            MergeWhiteboardResponses(response.Get(), perNodeStateInfo, TBase::RequestSettings.MergeFields);
        } else {
            for (auto& [nodeId, nodeResponse] : TBase::NodeResponses) {
                if (nodeResponse.IsOk()) {
                    locationResponded.AddNodeId(nodeId);
                    Merge(response->Record, nodeId, nodeResponse.Get()->Record);
                }
            }
        }

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
        case NKikimrViewer::TEvViewerRequest::kVDiskRequest:
            return new TViewerWhiteboardRequest<TEvWhiteboard::TEvVDiskStateRequest, TEvWhiteboard::TEvVDiskStateResponse>(request);
        case NKikimrViewer::TEvViewerRequest::kPDiskRequest:
            return new TViewerWhiteboardRequest<TEvWhiteboard::TEvPDiskStateRequest, TEvWhiteboard::TEvPDiskStateResponse>(request);
        case NKikimrViewer::TEvViewerRequest::kBSGroupRequest:
            return new TViewerWhiteboardRequest<TEvWhiteboard::TEvBSGroupStateRequest, TEvWhiteboard::TEvBSGroupStateResponse>(request);
        case NKikimrViewer::TEvViewerRequest::kQueryRequest:
            return new TJsonQueryOld(request);
        case NKikimrViewer::TEvViewerRequest::kRenderRequest:
            return new TJsonRender(request);
        case NKikimrViewer::TEvViewerRequest::kAutocompleteRequest:
            return new TJsonAutocomplete(request);
        default:
            return nullptr;
    }
    return nullptr;
}

template<>
THolder<TEvWhiteboard::TEvTabletStateRequest> TViewerWhiteboardRequest<TEvWhiteboard::TEvTabletStateRequest, TEvWhiteboard::TEvTabletStateResponse>::BuildRequest() {
    auto request = TBase::BuildRequest();
    request->Record.MergeFrom(Event->Get()->Record.GetTabletRequest());
    return request;
}

template<>
void TViewerWhiteboardRequest<TEvWhiteboard::TEvTabletStateRequest, TEvWhiteboard::TEvTabletStateResponse>::Merge(
        NKikimrViewer::TEvViewerResponse& viewerResponse, TNodeId nodeId, NKikimrWhiteboard::TEvTabletStateResponse& nodeResponse) {
    auto& target = *viewerResponse.MutableTabletResponse();
    for (auto& info : *nodeResponse.MutableTabletStateInfo()) {
        auto& i = *target.AddTabletStateInfo();
        i.MergeFrom(info);
        i.SetNodeId(nodeId);
    }
}

template<>
THolder<TEvWhiteboard::TEvSystemStateRequest> TViewerWhiteboardRequest<TEvWhiteboard::TEvSystemStateRequest, TEvWhiteboard::TEvSystemStateResponse>::BuildRequest() {
    auto request = TBase::BuildRequest();
    request->Record.MergeFrom(Event->Get()->Record.GetSystemRequest());
    return request;
}

template<>
void TViewerWhiteboardRequest<TEvWhiteboard::TEvSystemStateRequest, TEvWhiteboard::TEvSystemStateResponse>::Merge(
        NKikimrViewer::TEvViewerResponse& viewerResponse, TNodeId nodeId, NKikimrWhiteboard::TEvSystemStateResponse& nodeResponse) {
    auto& target = *viewerResponse.MutableSystemResponse();
    for (auto& info : *nodeResponse.MutableSystemStateInfo()) {
        auto& i = *target.AddSystemStateInfo();
        i.MergeFrom(info);
        i.SetNodeId(nodeId);
    }
}

template<>
THolder<TEvWhiteboard::TEvVDiskStateRequest> TViewerWhiteboardRequest<TEvWhiteboard::TEvVDiskStateRequest, TEvWhiteboard::TEvVDiskStateResponse>::BuildRequest() {
    auto request = TBase::BuildRequest();
    request->Record.MergeFrom(Event->Get()->Record.GetVDiskRequest());
    return request;
}

template<>
void TViewerWhiteboardRequest<TEvWhiteboard::TEvVDiskStateRequest, TEvWhiteboard::TEvVDiskStateResponse>::Merge(
        NKikimrViewer::TEvViewerResponse& viewerResponse, TNodeId nodeId, NKikimrWhiteboard::TEvVDiskStateResponse& nodeResponse) {
    auto& target = *viewerResponse.MutableVDiskResponse();
    for (auto& info : *nodeResponse.MutableVDiskStateInfo()) {
        auto& i = *target.AddVDiskStateInfo();
        i.MergeFrom(info);
        i.SetNodeId(nodeId);
    }
}

template<>
THolder<TEvWhiteboard::TEvPDiskStateRequest> TViewerWhiteboardRequest<TEvWhiteboard::TEvPDiskStateRequest, TEvWhiteboard::TEvPDiskStateResponse>::BuildRequest() {
    auto request = TBase::BuildRequest();
    request->Record.MergeFrom(Event->Get()->Record.GetPDiskRequest());
    return request;
}

template<>
void TViewerWhiteboardRequest<TEvWhiteboard::TEvPDiskStateRequest, TEvWhiteboard::TEvPDiskStateResponse>::Merge(
        NKikimrViewer::TEvViewerResponse& viewerResponse, TNodeId nodeId, NKikimrWhiteboard::TEvPDiskStateResponse& nodeResponse) {
    auto& target = *viewerResponse.MutablePDiskResponse();
    for (auto& info : *nodeResponse.MutablePDiskStateInfo()) {
        auto& i = *target.AddPDiskStateInfo();
        i.MergeFrom(info);
        i.SetNodeId(nodeId);
    }
}

template<>
THolder<TEvWhiteboard::TEvBSGroupStateRequest> TViewerWhiteboardRequest<TEvWhiteboard::TEvBSGroupStateRequest, TEvWhiteboard::TEvBSGroupStateResponse>::BuildRequest() {
    auto request = TBase::BuildRequest();
    request->Record.MergeFrom(Event->Get()->Record.GetBSGroupRequest());
    return request;
}

template<>
void TViewerWhiteboardRequest<TEvWhiteboard::TEvBSGroupStateRequest, TEvWhiteboard::TEvBSGroupStateResponse>::Merge(
        NKikimrViewer::TEvViewerResponse& viewerResponse, TNodeId nodeId, NKikimrWhiteboard::TEvBSGroupStateResponse& nodeResponse) {
    auto& target = *viewerResponse.MutableBSGroupResponse();
    for (auto& info : *nodeResponse.MutableBSGroupStateInfo()) {
        auto& i = *target.AddBSGroupStateInfo();
        i.MergeFrom(info);
        i.SetNodeId(nodeId);
    }
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
