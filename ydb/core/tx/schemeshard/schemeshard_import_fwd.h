#pragma once

#include "defs.h"

namespace NKikimrImport {
class TImport;
}

namespace NKikimr::NSchemeShard {

struct TEvImport {
    enum EEv {
        EvCreateImportRequest = EventSpaceBegin(TKikimrEvents::ES_IMPORT_SERVICE),
        EvCreateImportResponse,
        EvGetImportRequest,
        EvGetImportResponse,
        EvCancelImportRequest,
        EvCancelImportResponse,
        EvForgetImportRequest,
        EvForgetImportResponse,
        EvListImportsRequest,
        EvListImportsResponse,
        EvListObjectsInS3ExportRequest,
        EvListObjectsInS3ExportResponse,
        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_IMPORT_SERVICE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_IMPORT_SERVICE)"
    );

    struct TEvCreateImportRequest;
    struct TEvCreateImportResponse;
    struct TEvGetImportRequest;
    struct TEvGetImportResponse;
    struct TEvCancelImportRequest;
    struct TEvCancelImportResponse;
    struct TEvForgetImportRequest;
    struct TEvForgetImportResponse;
    struct TEvListImportsRequest;
    struct TEvListImportsResponse;
    struct TEvListObjectsInS3ExportRequest;
    struct TEvListObjectsInS3ExportResponse;
};

#define DECLARE_IMPORT_EVENT_HANDLE(event) \
    using event ## __HandlePtr = TAutoPtr<NActors::TEventHandle<TEvImport::event>>;
DECLARE_IMPORT_EVENT_HANDLE(TEvCreateImportRequest)
DECLARE_IMPORT_EVENT_HANDLE(TEvGetImportRequest)
DECLARE_IMPORT_EVENT_HANDLE(TEvCancelImportRequest)
DECLARE_IMPORT_EVENT_HANDLE(TEvForgetImportRequest)
DECLARE_IMPORT_EVENT_HANDLE(TEvListImportsRequest)
DECLARE_IMPORT_EVENT_HANDLE(TEvListObjectsInS3ExportRequest)
#undef DECLARE_IMPORT_EVENT_HANDLE

} // namespace NKikimr::NSchemeShard
