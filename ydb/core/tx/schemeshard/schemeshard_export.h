#pragma once

#include "defs.h"

#include <ydb/core/protos/export.pb.h>

namespace NKikimr {
namespace NSchemeShard {

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

#ifdef DECLARE_EVENT_CLASS
#error DECLARE_EVENT_CLASS macro redefinition
#else
#define DECLARE_EVENT_CLASS(NAME) struct T##NAME: public TEventPB<T##NAME, NKikimrExport::T##NAME, NAME>
#endif

    DECLARE_EVENT_CLASS(EvCreateExportRequest) {
        TEvCreateExportRequest() = default;

        explicit TEvCreateExportRequest(
            const ui64 txId,
            const TString& dbName,
            const NKikimrExport::TCreateExportRequest& request
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }
    };

    DECLARE_EVENT_CLASS(EvCreateExportResponse) {
        TEvCreateExportResponse() = default;

        explicit TEvCreateExportResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DECLARE_EVENT_CLASS(EvGetExportRequest) {
        TEvGetExportRequest() = default;

        explicit TEvGetExportRequest(const TString& dbName, const NKikimrExport::TGetExportRequest& request) {
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }

        explicit TEvGetExportRequest(const TString& dbName, const ui64 exportId) {
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->SetId(exportId);
        }
    };

    DECLARE_EVENT_CLASS(EvGetExportResponse) {
    };

    DECLARE_EVENT_CLASS(EvCancelExportRequest) {
        TEvCancelExportRequest() = default;

        explicit TEvCancelExportRequest(
            const ui64 txId,
            const TString& dbName,
            const NKikimrExport::TCancelExportRequest& request
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }

        explicit TEvCancelExportRequest(
            const ui64 txId,
            const TString& dbName,
            const ui64 exportId
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->SetId(exportId);
        }
    };

    DECLARE_EVENT_CLASS(EvCancelExportResponse) {
        TEvCancelExportResponse() = default;

        explicit TEvCancelExportResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DECLARE_EVENT_CLASS(EvForgetExportRequest) {
        TEvForgetExportRequest() = default;

        explicit TEvForgetExportRequest(
            const ui64 txId,
            const TString& dbName,
            const NKikimrExport::TForgetExportRequest& request
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }

        explicit TEvForgetExportRequest(
            const ui64 txId,
            const TString& dbName,
            const ui64 exportId
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->SetId(exportId);
        }
    };

    DECLARE_EVENT_CLASS(EvForgetExportResponse) {
        TEvForgetExportResponse() = default;

        explicit TEvForgetExportResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DECLARE_EVENT_CLASS(EvListExportsRequest) {
        TEvListExportsRequest() = default;

        explicit TEvListExportsRequest(const TString& dbName, const NKikimrExport::TListExportsRequest& request) {
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }

        explicit TEvListExportsRequest(
            const TString& dbName,
            const ui64 pageSize,
            const TString& pageToken,
            const TString& kind
        ) {
            Record.SetDatabaseName(dbName);

            auto& request = *Record.MutableRequest();
            request.SetPageSize(pageSize);
            request.SetPageToken(pageToken);
            request.SetKind(kind);
        }
    };

    DECLARE_EVENT_CLASS(EvListExportsResponse) {
    };

#undef DECLARE_EVENT_CLASS

}; // TEvExport

} // NSchemeShard
} // NKikimr
