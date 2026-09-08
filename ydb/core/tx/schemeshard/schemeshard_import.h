#pragma once

#include "schemeshard_import_fwd.h"

#include <ydb/core/protos/import.pb.h>

namespace NKikimr {
namespace NSchemeShard {

#ifdef DECLARE_EVENT_CLASS
#error DECLARE_EVENT_CLASS macro redefinition
#else
#define DECLARE_EVENT_CLASS(NAME) struct TEvImport::T##NAME: public TEventPB<T##NAME, NKikimrImport::T##NAME, TEvImport::NAME>
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

    DECLARE_EVENT_CLASS(EvListObjectsInS3ExportRequest) {
    };

    DECLARE_EVENT_CLASS(EvListObjectsInS3ExportResponse) {
    };

#undef DECLARE_EVENT_CLASS

} // NSchemeShard
} // NKikimr
