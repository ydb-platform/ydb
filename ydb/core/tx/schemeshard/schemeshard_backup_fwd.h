#pragma once

#include "defs.h"

namespace NKikimrBackup {
class TFullBackup;
}

namespace NKikimr::NSchemeShard {

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
        EvGetBackupCollectionRestoreRequest,
        EvGetBackupCollectionRestoreResponse,
        EvForgetBackupCollectionRestoreRequest,
        EvForgetBackupCollectionRestoreResponse,
        EvListBackupCollectionRestoresRequest,
        EvListBackupCollectionRestoresResponse,
        EvGetFullBackupRequest,
        EvGetFullBackupResponse,
        EvForgetFullBackupRequest,
        EvForgetFullBackupResponse,
        EvListFullBackupsRequest,
        EvListFullBackupsResponse,
        EvEnd
    };

    static_assert(
        EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SERVICE),
        "expect EvEnd < EventSpaceEnd(TKikimrEvents::ES_BACKUP_SERVICE)"
    );

    struct TEvFetchBackupCollectionsRequest;
    struct TEvFetchBackupCollectionsResponse;
    struct TEvListBackupCollectionsRequest;
    struct TEvListBackupCollectionsResponse;
    struct TEvCreateBackupCollectionRequest;
    struct TEvCreateBackupCollectionResponse;
    struct TEvReadBackupCollectionRequest;
    struct TEvReadBackupCollectionResponse;
    struct TEvUpdateBackupCollectionRequest;
    struct TEvUpdateBackupCollectionResponse;
    struct TEvDeleteBackupCollectionRequest;
    struct TEvDeleteBackupCollectionResponse;
    struct TEvGetIncrementalBackupRequest;
    struct TEvGetIncrementalBackupResponse;
    struct TEvForgetIncrementalBackupRequest;
    struct TEvForgetIncrementalBackupResponse;
    struct TEvListIncrementalBackupsRequest;
    struct TEvListIncrementalBackupsResponse;
    struct TEvGetBackupCollectionRestoreRequest;
    struct TEvGetBackupCollectionRestoreResponse;
    struct TEvForgetBackupCollectionRestoreRequest;
    struct TEvForgetBackupCollectionRestoreResponse;
    struct TEvListBackupCollectionRestoresRequest;
    struct TEvListBackupCollectionRestoresResponse;
    struct TEvGetFullBackupRequest;
    struct TEvGetFullBackupResponse;
    struct TEvForgetFullBackupRequest;
    struct TEvForgetFullBackupResponse;
    struct TEvListFullBackupsRequest;
    struct TEvListFullBackupsResponse;

    template <class T>
    struct TEvApiMapping;
};

#define DECLARE_BACKUP_EVENT_HANDLE(event) \
    using event ## __HandlePtr = TAutoPtr<NActors::TEventHandle<TEvBackup::event>>;
DECLARE_BACKUP_EVENT_HANDLE(TEvFetchBackupCollectionsRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvListBackupCollectionsRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvCreateBackupCollectionRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvReadBackupCollectionRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvUpdateBackupCollectionRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvDeleteBackupCollectionRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvGetIncrementalBackupRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvForgetIncrementalBackupRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvListIncrementalBackupsRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvGetBackupCollectionRestoreRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvForgetBackupCollectionRestoreRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvListBackupCollectionRestoresRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvGetFullBackupRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvForgetFullBackupRequest)
DECLARE_BACKUP_EVENT_HANDLE(TEvListFullBackupsRequest)
#undef DECLARE_BACKUP_EVENT_HANDLE

} // namespace NKikimr::NSchemeShard
