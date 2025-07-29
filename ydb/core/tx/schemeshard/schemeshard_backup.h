#pragma once

#include "defs.h"

#include <ydb/public/api/protos/draft/ydb_backup.pb.h>

#include <ydb/core/protos/backup.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TEvBackup {
    enum EEv {
        EvFetchBackupCollectionsRequest = EventSpaceBegin(TKikimrEvents::ES_BACKUP_SERVICE),
        EvFetchBackupCollectionsResponse,
        EvListBackupCollectionsRequest,
        EvListBackupCollectionsResponse,
        EvCreateBackupCollectionRequest,
        EvCreateBackupCollectionResponse,
        EvReadBackupCollectionRequest,
        EvReadBackupCollectionResponse,
        EvUpdateBackupCollectionRequest,
        EvUpdateBackupCollectionResponse,
        EvDeleteBackupCollectionRequest,
        EvDeleteBackupCollectionResponse,

        EvGetIncrementalBackupRequest,
        EvGetIncrementalBackupResponse,
        EvForgetIncrementalBackupRequest,
        EvForgetIncrementalBackupResponse,
        EvListIncrementalBackupsRequest,
        EvListIncrementalBackupsResponse,

        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SERVICE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SERVICE)"
    );

#ifdef DECLARE_EVENT_CLASS
#error DECLARE_EVENT_CLASS macro redefinition
#else
#define DECLARE_EVENT_CLASS(NAME) struct T##NAME: public TEventPB<T##NAME, NKikimrBackup::T##NAME, NAME>
#endif

    DECLARE_EVENT_CLASS(EvFetchBackupCollectionsRequest) {};
    DECLARE_EVENT_CLASS(EvFetchBackupCollectionsResponse) {};
    DECLARE_EVENT_CLASS(EvListBackupCollectionsRequest) {};
    DECLARE_EVENT_CLASS(EvListBackupCollectionsResponse) {};
    DECLARE_EVENT_CLASS(EvCreateBackupCollectionRequest) {};
    DECLARE_EVENT_CLASS(EvCreateBackupCollectionResponse) {};
    DECLARE_EVENT_CLASS(EvReadBackupCollectionRequest) {};
    DECLARE_EVENT_CLASS(EvReadBackupCollectionResponse) {};
    DECLARE_EVENT_CLASS(EvUpdateBackupCollectionRequest) {};
    DECLARE_EVENT_CLASS(EvUpdateBackupCollectionResponse) {};
    DECLARE_EVENT_CLASS(EvDeleteBackupCollectionRequest) {};
    DECLARE_EVENT_CLASS(EvDeleteBackupCollectionResponse) {};

    DECLARE_EVENT_CLASS(EvGetIncrementalBackupRequest) {
        TEvGetIncrementalBackupRequest() = default;

        explicit TEvGetIncrementalBackupRequest(const TString& dbName, ui64 incrementalBackupId) {
            Record.SetDatabaseName(dbName);
            Record.SetIncrementalBackupId(incrementalBackupId);
        }
    };
    DECLARE_EVENT_CLASS(EvGetIncrementalBackupResponse) {};
    DECLARE_EVENT_CLASS(EvForgetIncrementalBackupRequest) {
        TEvForgetIncrementalBackupRequest() = default;

        explicit TEvForgetIncrementalBackupRequest(
            const ui64 txId,
            const TString& dbName,
            ui64 incrementalBackupId
            ) {
            Record.SetTxId(txId);
            Record.SetDatabaseName(dbName);
            Record.SetIncrementalBackupId(incrementalBackupId);
        }
    };
    DECLARE_EVENT_CLASS(EvForgetIncrementalBackupResponse) {
        TEvForgetIncrementalBackupResponse() = default;

        explicit TEvForgetIncrementalBackupResponse(const ui64 txId) {
            Record.SetTxId(txId);
        }
    };
    DECLARE_EVENT_CLASS(EvListIncrementalBackupsRequest) {
        TEvListIncrementalBackupsRequest() = default;

        explicit TEvListIncrementalBackupsRequest(const TString& dbName, ui64 pageSize, TString pageToken) {
            Record.SetDatabaseName(dbName);
            Record.SetPageSize(pageSize);
            Record.SetPageToken(pageToken);
        }
    };
    DECLARE_EVENT_CLASS(EvListIncrementalBackupsResponse) {};


#undef DECLARE_EVENT_CLASS

    template <class T>
    struct TEvApiMapping;

    template <>
    struct TEvApiMapping<Ydb::Backup::FetchBackupCollectionsRequest> {
        using TEv = TEvFetchBackupCollectionsRequest;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::FetchBackupCollectionsResponse> {
        using TEv = TEvFetchBackupCollectionsResponse;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::ListBackupCollectionsRequest> {
        using TEv = TEvListBackupCollectionsRequest;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::ListBackupCollectionsResponse> {
        using TEv = TEvListBackupCollectionsResponse;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::CreateBackupCollectionRequest> {
        using TEv = TEvCreateBackupCollectionRequest;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::CreateBackupCollectionResponse> {
        using TEv = TEvCreateBackupCollectionResponse;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::ReadBackupCollectionRequest> {
        using TEv = TEvReadBackupCollectionRequest;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::ReadBackupCollectionResponse> {
        using TEv = TEvReadBackupCollectionResponse;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::UpdateBackupCollectionRequest> {
        using TEv = TEvUpdateBackupCollectionRequest;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::UpdateBackupCollectionResponse> {
        using TEv = TEvUpdateBackupCollectionResponse;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::DeleteBackupCollectionRequest> {
        using TEv = TEvDeleteBackupCollectionRequest;
    };

    template <>
    struct TEvApiMapping<Ydb::Backup::DeleteBackupCollectionResponse> {
        using TEv = TEvDeleteBackupCollectionResponse;
    };
}; // TEvBackup

} // NSchemeShard
} // NKikimr
