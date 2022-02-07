#pragma once

#include "defs.h"

#include <ydb/core/protos/import.pb.h>

namespace NKikimr {
namespace NSchemeShard {

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

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_IMPORT_SERVICE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_IMPORT_SERVICE)"
    );

#ifdef DECLARE_EVENT_CLASS
#error DECLARE_EVENT_CLASS macro redefinition
#else
#define DECLARE_EVENT_CLASS(NAME) struct T##NAME: public TEventPB<T##NAME, NKikimrImport::T##NAME, NAME>
#endif

    DECLARE_EVENT_CLASS(EvCreateImportRequest) {
        TEvCreateImportRequest() = default;

        explicit TEvCreateImportRequest(
            const ui64 txId,
            const TString& dbName,
            const NKikimrImport::TCreateImportRequest& request
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }
    };

    DECLARE_EVENT_CLASS(EvCreateImportResponse) {
        TEvCreateImportResponse() = default;

        explicit TEvCreateImportResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DECLARE_EVENT_CLASS(EvGetImportRequest) {
        TEvGetImportRequest() = default;

        explicit TEvGetImportRequest(const TString& dbName, const NKikimrImport::TGetImportRequest& request) {
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }

        explicit TEvGetImportRequest(const TString& dbName, const ui64 importId) {
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->SetId(importId);
        }
    };

    DECLARE_EVENT_CLASS(EvGetImportResponse) {
    };

    DECLARE_EVENT_CLASS(EvCancelImportRequest) {
        TEvCancelImportRequest() = default;

        explicit TEvCancelImportRequest(
            const ui64 txId,
            const TString& dbName,
            const NKikimrImport::TCancelImportRequest& request
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }

        explicit TEvCancelImportRequest(
            const ui64 txId,
            const TString& dbName,
            const ui64 importId
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->SetId(importId);
        }
    };

    DECLARE_EVENT_CLASS(EvCancelImportResponse) {
        TEvCancelImportResponse() = default;

        explicit TEvCancelImportResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DECLARE_EVENT_CLASS(EvForgetImportRequest) {
        TEvForgetImportRequest() = default;

        explicit TEvForgetImportRequest(
            const ui64 txId,
            const TString& dbName,
            const NKikimrImport::TForgetImportRequest& request
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }

        explicit TEvForgetImportRequest(
            const ui64 txId,
            const TString& dbName,
            const ui64 importId
        ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->SetId(importId);
        }
    };

    DECLARE_EVENT_CLASS(EvForgetImportResponse) {
        TEvForgetImportResponse() = default;

        explicit TEvForgetImportResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };

    DECLARE_EVENT_CLASS(EvListImportsRequest) {
        TEvListImportsRequest() = default;

        explicit TEvListImportsRequest(const TString& dbName, const NKikimrImport::TListImportsRequest& request) {
            Record.SetDatabaseName(dbName);
            Record.MutableRequest()->CopyFrom(request);
        }

        explicit TEvListImportsRequest(
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

    DECLARE_EVENT_CLASS(EvListImportsResponse) {
    };

#undef DECLARE_EVENT_CLASS

}; // TEvImport

} // NSchemeShard
} // NKikimr
