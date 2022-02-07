#pragma once

#include "defs.h"

#include "types.h"

namespace NKikimr::NBsController {

    struct TEvControllerNotifyGroupChange : TEventLocal<TEvControllerNotifyGroupChange, TEvBlobStorage::EvControllerNotifyGroupChange> {
        TVector<TGroupId> Created;
        TVector<TGroupId> Deleted;
    };

    struct TEvControllerCommitGroupLatencies : TEventLocal<TEvControllerCommitGroupLatencies, TEvBlobStorage::EvControllerCommitGroupLatencies> {
        TMap<TGroupId, TGroupLatencyStats> Updates;
    };

    IActor *CreateStatProcessorActor();

} // NKikimr::NBsController
