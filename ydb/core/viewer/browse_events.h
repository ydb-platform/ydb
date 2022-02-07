#pragma once

#include <library/cpp/actors/core/events.h>
#include <library/cpp/actors/core/event_local.h>
#include <ydb/core/viewer/protos/viewer.pb.h>

#include <util/generic/string.h>

namespace NKikimr {
namespace NViewer {

namespace NViewerEvents {
    enum EEv {
        EvBrowseResponse = EventSpaceBegin(TKikimrEvents::ES_VIEWER),
        EvBrowseRequestSent,
        EvBrowseRequestCompleted,
        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_VIEWER), "expected EvEnd < EventSpaceEnd");

    using TTabletId = ui64;

    struct TEvBrowseResponse : TEventLocal<TEvBrowseResponse, EvBrowseResponse> {
        TEvBrowseResponse(
                NKikimrViewer::TBrowseInfo&& browseInfo,
                NKikimrViewer::TMetaInfo&& metaInfo
                )
            : BrowseInfo(std::move(browseInfo))
            , MetaInfo(std::move(metaInfo))
        {}

        TEvBrowseResponse(const TString& error)
            : Error(error)
        {}

        NKikimrViewer::TBrowseInfo BrowseInfo;
        NKikimrViewer::TMetaInfo MetaInfo;
        TString Error;
    };

    struct TEvBrowseRequestSent : TEventLocal<TEvBrowseRequestSent, EvBrowseRequestSent> {
        TActorId Actor;
        TTabletId Tablet;
        ui32 Event;

        TEvBrowseRequestSent(const TActorId& actor, TTabletId tablet, ui32 event)
            : Actor(actor)
            , Tablet(tablet)
            , Event(event)
        {}

        TEvBrowseRequestSent(const TActorId& actor, ui32 event)
            : Actor(actor)
            , Tablet(0)
            , Event(event)
        {}
    };

    struct TEvBrowseRequestCompleted : TEventLocal<TEvBrowseRequestCompleted, EvBrowseRequestCompleted> {
        TActorId Actor;
        TTabletId Tablet;
        ui32 Event;

        TEvBrowseRequestCompleted(const TActorId& actor, TTabletId tablet, ui32 event)
            : Actor(actor)
            , Tablet(tablet)
            , Event(event)
        {}

        TEvBrowseRequestCompleted(const TActorId& actor, ui32 event)
            : Actor(actor)
            , Tablet(0)
            , Event(event)
        {}
    };
}   // namespace NViewerEvents

}   // namespace NViewer
}   // namespace NKikimr
