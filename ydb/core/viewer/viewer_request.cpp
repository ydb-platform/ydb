#include <ydb/core/blobstorage/base/blobstorage_events.h>

#include "viewer_request.h"
#include "wb_req.h"

#include "json_tabletinfo.h"

namespace NKikimr {
namespace NViewer {

using namespace NActors;
using namespace NNodeWhiteboard;

template<typename TRequestEventType, typename TResponseEventType>
class TViewerWhiteboardRequest : public TWhiteboardRequest<TViewerWhiteboardRequest<TRequestEventType, TResponseEventType>, TRequestEventType, TResponseEventType> {
protected:
    using TThis = TViewerWhiteboardRequest<TRequestEventType, TResponseEventType>;
    using TBase = TWhiteboardRequest<TThis, TRequestEventType, TResponseEventType>;
    using TResponseType = typename TResponseEventType::ProtoRecordType;
    IViewer* Viewer;
    TEvViewer::TEvViewerRequest::TPtr Event;
    TJsonSettings JsonSettings;

public:
    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::VIEWER_HANDLER;
    }

    TViewerWhiteboardRequest(TEvViewer::TEvViewerRequest::TPtr& ev)
        : Event(ev)
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

    void ReplyAndPassAway() {
        auto response = MakeHolder<TEvViewer::TEvViewerResponse>();
        auto& locationResponded = (*response->Record.MutableLocationResponded());
        for (const auto& [nodeId, nodeResponse] : TBase::PerNodeStateInfo) {
            locationResponded.AddNodeId(nodeId);
        }
        MergeWhiteboardResponses((*response->Record.MutableTabletResponse()), TBase::PerNodeStateInfo, TBase::RequestSettings.MergeFields); // PerNodeStateInfo will be invalidated

        TBase::Send(Event->Sender, response.Release(), 0, Event->Cookie);
        TBase::PassAway();
    }
};

IActor* CreateViewerRequestHandler(TEvViewer::TEvViewerRequest::TPtr request) {
    switch (request->Get()->Record.GetRequestCase()) {
        case NKikimrViewer::TEvViewerRequest::kTabletRequest:
            return new TViewerWhiteboardRequest<TEvWhiteboard::TEvTabletStateRequest, TEvWhiteboard::TEvTabletStateResponse>(request);
        case NKikimrViewer::TEvViewerRequest::REQUEST_NOT_SET:
            return nullptr;
    }
    return nullptr;
}

}
}
