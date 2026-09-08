#pragma once

#include <ydb/core/base/events.h>
#include <ydb/core/scheme/scheme_pathid.h>
#include <ydb/core/scheme/scheme_tabledefs.h>

#include <ydb/library/actors/core/event_local.h>

#include <util/generic/ptr.h>
#include <util/generic/string.h>

namespace NKikimr {

namespace NSchemeCache {
class TDescribeResult;
struct TSchemeCacheNavigate;
struct TSchemeCacheRequest;
}

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
    struct TEvBasic : public TEventLocal<TDerived, EventType> {
        TAutoPtr<TRequest> Request;

        TEvBasic(TAutoPtr<TRequest> request)
            : Request(request)
        {}
    };

public:
    struct TEvResolveKeySet : public TEvBasic<TEvResolveKeySet, EvResolveKeySet, NSchemeCache::TSchemeCacheRequest> {
        using TEvBasic::TEvBasic;
    };

    struct TEvResolveKeySetResult : public TEvBasic<TEvResolveKeySetResult, EvResolveKeySetResult, NSchemeCache::TSchemeCacheRequest> {
        using TEvBasic::TEvBasic;
    };

    struct TEvNavigateKeySet : public TEvBasic<TEvNavigateKeySet, EvNavigateKeySet, NSchemeCache::TSchemeCacheNavigate> {
        using TEvBasic::TEvBasic;
    };

    struct TEvNavigateKeySetResult : public TEvBasic<TEvNavigateKeySetResult, EvNavigateKeySetResult, NSchemeCache::TSchemeCacheNavigate> {
        using TEvBasic::TEvBasic;
    };

    struct TEvInvalidateTable : public TEventLocal<TEvInvalidateTable, EvInvalidateTable> {
        const TTableId TableId;
        const TActorId Sender;

        TEvInvalidateTable(const TTableId& tableId, const TActorId& sender)
            : TableId(tableId)
            , Sender(sender)
        {}
    };

    struct TEvInvalidateTableResult : public TEventLocal<TEvInvalidateTableResult, EvInvalidateTableResult> {
        const TActorId Sender;

        TEvInvalidateTableResult(const TActorId& sender)
            : Sender(sender)
        {}
    };

    struct TEvWatchPathId : public TEventLocal<TEvWatchPathId, EvWatchPathId> {
        const TPathId PathId;
        const ui64 Key;

        explicit TEvWatchPathId(const TPathId& pathId, ui64 key = 0)
            : PathId(pathId)
            , Key(key)
        {}
    };

    struct TEvWatchRemove : public TEventLocal<TEvWatchRemove, EvWatchRemove> {
        const ui64 Key;

        explicit TEvWatchRemove(ui64 key = 0)
            : Key(key)
        {}
    };

    struct TEvWatchNotifyUpdated : public TEventLocal<TEvWatchNotifyUpdated, EvWatchNotifyUpdated> {
        using TDescribeResult = NSchemeCache::TDescribeResult;

        const ui64 Key;
        const TString Path;
        const TPathId PathId;
        TIntrusiveConstPtr<TDescribeResult> Result;

        TEvWatchNotifyUpdated(ui64 key, const TString& path, const TPathId& pathId, TIntrusiveConstPtr<TDescribeResult> result);
        ~TEvWatchNotifyUpdated();
    };

    struct TEvWatchNotifyDeleted : public TEventLocal<TEvWatchNotifyDeleted, EvWatchNotifyDeleted> {
        const ui64 Key;
        const TString Path;
        const TPathId PathId;

        TEvWatchNotifyDeleted(ui64 key, const TString& path, const TPathId& pathId)
            : Key(key)
            , Path(path)
            , PathId(pathId)
        {}
    };

    struct TEvWatchNotifyUnavailable : public TEventLocal<TEvWatchNotifyUnavailable, EvWatchNotifyUnavailable> {
        const ui64 Key;
        const TString Path;
        const TPathId PathId;

        TEvWatchNotifyUnavailable(ui64 key, const TString& path, const TPathId& pathId)
            : Key(key)
            , Path(path)
            , PathId(pathId)
        {}
    };
};

inline TActorId MakeSchemeCacheID() {
    return TActorId(0, TStringBuf("SchmCcheSrv"));
}

} // namespace NKikimr
