#pragma once

#include "defs.h"

namespace NKikimrExport {
class TExport;
}

namespace NKikimr::NSchemeShard {

struct TEvExport {
    enum EEv {
        EvCreateExportRequest = EventSpaceBegin(TKikimrEvents::ES_EXPORT_SERVICE),
        EvCreateExportResponse,
        EvGetExportRequest,
        EvGetExportResponse,
        EvCancelExportRequest,
        EvCancelExportResponse,
        EvForgetExportRequest,
        EvForgetExportResponse,
        EvListExportsRequest,
        EvListExportsResponse,
        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_EXPORT_SERVICE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_EXPORT_SERVICE)"
    );

    struct TEvCreateExportRequest;
    struct TEvCreateExportResponse;
    struct TEvGetExportRequest;
    struct TEvGetExportResponse;
    struct TEvCancelExportRequest;
    struct TEvCancelExportResponse;
    struct TEvForgetExportRequest;
    struct TEvForgetExportResponse;
    struct TEvListExportsRequest;
    struct TEvListExportsResponse;
};

#define DECLARE_EXPORT_EVENT_HANDLE(event) \
    using event ## __HandlePtr = TAutoPtr<NActors::TEventHandle<TEvExport::event>>;
DECLARE_EXPORT_EVENT_HANDLE(TEvCreateExportRequest)
DECLARE_EXPORT_EVENT_HANDLE(TEvGetExportRequest)
DECLARE_EXPORT_EVENT_HANDLE(TEvCancelExportRequest)
DECLARE_EXPORT_EVENT_HANDLE(TEvForgetExportRequest)
DECLARE_EXPORT_EVENT_HANDLE(TEvListExportsRequest)
#undef DECLARE_EXPORT_EVENT_HANDLE

} // namespace NKikimr::NSchemeShard
