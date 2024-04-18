#pragma once

#include "defs.h"

#include <ydb/core/protos/backup.pb.h>

namespace NKikimr {
namespace NSchemeShard {

struct TEvBackup {
    enum EEv {
        EvFetchBackupCollectionsRequest = EventSpaceBegin(TKikimrEvents::ES_BACKUP_SERVICE),
        EvFetchBackupCollectionsResponse,

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

    DECLARE_EVENT_CLASS(EvFetchBackupCollectionsRequest) {
    };

    DECLARE_EVENT_CLASS(EvFetchBackupCollectionsResponse) {
    };

#undef DECLARE_EVENT_CLASS

}; // TEvBackup

} // NSchemeShard
} // NKikimr
