#pragma once

#include <ydb/core/base/events.h>

namespace NKikimr {

struct TEvTxProxySchemeCache {
    enum EEv {
        EvResolveKeySet = EventSpaceBegin(TKikimrEvents::ES_SCHEME_CACHE),
        EvInvalidateDistEntry, // unused
        EvResolveKeySetResult,
        EvNavigateKeySet,
        EvNavigateKeySetResult,
        EvInvalidateTable,
        EvInvalidateTableResult,
        EvWatchPathId,
        EvWatchRemove,
        EvWatchNotifyUpdated,
        EvWatchNotifyDeleted,
        EvWatchNotifyUnavailable,

        EvEnd,
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_SCHEME_CACHE), "expect EvEnd < EventSpaceEnd(ES_SCHEME_CACHE)");

private:
    template <typename TDerived, ui32 EventType, typename TRequest>
    struct TEvBasic;

public:
    struct TEvResolveKeySet;
    struct TEvResolveKeySetResult;
    struct TEvNavigateKeySet;
    struct TEvNavigateKeySetResult;
    struct TEvInvalidateTable;
    struct TEvInvalidateTableResult;
    struct TEvWatchPathId;
    struct TEvWatchRemove;
    struct TEvWatchNotifyUpdated;
    struct TEvWatchNotifyDeleted;
    struct TEvWatchNotifyUnavailable;
};

inline TActorId MakeSchemeCacheID() {
    return TActorId(0, TStringBuf("SchmCcheSrv"));
}

} // NKikimr
