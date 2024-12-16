#pragma once
#include "viewer.h"

namespace NKikimr::NViewer {

using namespace NActors;

union ViewerWhiteboardCookie {
    struct {
        ui32 NodeId : 32;
        NKikimrViewer::TEvViewerRequest::RequestCase RequestCase : 32;
    } bits;
    ui64 cookie;

    ViewerWhiteboardCookie(NKikimrViewer::TEvViewerRequest::RequestCase requestCase, ui32 nodeId) {
        bits.RequestCase = requestCase;
        bits.NodeId = nodeId;
    }

    ViewerWhiteboardCookie(ui64 value)
        : cookie(value)
    {
    }

    ui64 ToUi64() const {
        return cookie;
    }

    ui32 GetNodeId() {
        return bits.NodeId;
    }

    NKikimrViewer::TEvViewerRequest::RequestCase GetRequestCase() {
        return bits.RequestCase;
    }
};

IActor* CreateViewerRequestHandler(TEvViewer::TEvViewerRequest::TPtr& request);
bool IsPostContent(const NMon::TEvHttpInfo::TPtr& event);

}
