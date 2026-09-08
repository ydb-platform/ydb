#pragma once

#include "viewer_events_fwd.h"

#include <ydb/core/sys_view/common/events.h>
#include <ydb/core/viewer/protos/viewer_events.pb.h>
#include <ydb/library/actors/core/event.h>

namespace NKikimr::NViewer {

class TViewerRequestEvent : public TEventPB<TViewerRequestEvent, NKikimrViewer::TEvViewerRequest, TEvViewer::EvViewerRequest> {
public:
    TViewerRequestEvent() = default;
};

class TViewerResponseEvent : public TEventPB<TViewerResponseEvent, NKikimrViewer::TEvViewerResponse, TEvViewer::EvViewerResponse> {
public:
    TViewerResponseEvent() = default;
};

class TUpdateSharedCacheTabletRequestEvent : public TEventLocal<TUpdateSharedCacheTabletRequestEvent, TEvViewer::EvUpdateSharedCacheTabletRequest> {
public:
    TTabletId TabletId;
    std::unique_ptr<IEventBase> Request;

    TUpdateSharedCacheTabletRequestEvent(TTabletId tabletId, std::unique_ptr<IEventBase> request)
        : TabletId(tabletId)
        , Request(std::move(request))
    {}
};

class TUpdateSharedCacheTabletResponseEvent : public TEventLocal<TUpdateSharedCacheTabletResponseEvent, TEvViewer::EvUpdateSharedCacheTabletResponse> {
public:
    std::variant<
        std::shared_ptr<NSysView::TEvSysView::TEvGetGroupsResponse>,
        std::shared_ptr<NSysView::TEvSysView::TEvGetStoragePoolsResponse>,
        std::shared_ptr<NSysView::TEvSysView::TEvGetVSlotsResponse>,
        std::shared_ptr<NSysView::TEvSysView::TEvGetPDisksResponse>,
        std::shared_ptr<NSysView::TEvSysView::TEvGetStorageStatsResponse>> Response;

    TUpdateSharedCacheTabletResponseEvent(std::shared_ptr<NSysView::TEvSysView::TEvGetGroupsResponse> response)
        : Response(std::move(response))
    {}

    TUpdateSharedCacheTabletResponseEvent(std::shared_ptr<NSysView::TEvSysView::TEvGetStoragePoolsResponse> response)
        : Response(std::move(response))
    {}

    TUpdateSharedCacheTabletResponseEvent(std::shared_ptr<NSysView::TEvSysView::TEvGetVSlotsResponse> response)
        : Response(std::move(response))
    {}

    TUpdateSharedCacheTabletResponseEvent(std::shared_ptr<NSysView::TEvSysView::TEvGetPDisksResponse> response)
        : Response(std::move(response))
    {}

    TUpdateSharedCacheTabletResponseEvent(std::shared_ptr<NSysView::TEvSysView::TEvGetStorageStatsResponse> response)
        : Response(std::move(response))
    {}
};

void UpdateSharedCacheData(IViewer& viewer, std::unique_ptr<TUpdateSharedCacheTabletResponseEvent> ev);

}
