#pragma once
#include "defs.h"
#include <ydb/core/base/memobserver.h>
#include <ydb/core/protos/shared_cache.pb.h>
#include <ydb/core/util/cache_cache.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr {

struct TEvSharedPageCache {
    enum EEv {
        EvBegin = EventSpaceBegin(TKikimrEvents::ES_FLAT_EXECUTOR) + 1536,

        EvConfigure = EvBegin + 0,
    };

    struct TEvConfigure
        : public TEventPB<TEvConfigure, NKikimrSharedCache::TSharedCacheConfig, EvConfigure>
    {
        TEvConfigure() = default;
    };
};

struct TSharedPageCacheCounters final : public TAtomicRefCount<TSharedPageCacheCounters> {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;

    const TCounterPtr MemLimitBytes;
    const TCounterPtr ConfigLimitBytes;
    const TCounterPtr ActivePages;
    const TCounterPtr ActiveBytes;
    const TCounterPtr ActiveLimitBytes;
    const TCounterPtr PassivePages;
    const TCounterPtr PassiveBytes;
    const TCounterPtr RequestedPages;
    const TCounterPtr RequestedBytes;
    const TCounterPtr CacheHitPages;
    const TCounterPtr CacheHitBytes;
    const TCounterPtr CacheMissPages;
    const TCounterPtr CacheMissBytes;
    const TCounterPtr LoadInFlyPages;
    const TCounterPtr LoadInFlyBytes;
    const TCounterPtr MemTableTotalBytes;
    const TCounterPtr MemTableCompactingBytes;
    const TCounterPtr MemTableCompactedBytes;

    explicit TSharedPageCacheCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters> &group);
};

struct TSharedPageCacheConfig {
    TIntrusivePtr<TCacheCacheConfig> CacheConfig;
    ui64 TotalScanQueueInFlyLimit = 512 * 1024 * 1024;
    ui64 TotalAsyncQueueInFlyLimit = 512 * 1024 * 1024;
    TString CacheName = "SharedPageCache";
    TIntrusivePtr<TSharedPageCacheCounters> Counters;
    ui32 ActivePagesReservationPercent = 50;
    ui32 MemTableReservationPercent = 20;
};

IActor* CreateSharedPageCache(THolder<TSharedPageCacheConfig> config, TIntrusivePtr<TMemObserver> memObserver);

inline TActorId MakeSharedPageCacheId(ui64 id = 0) {
    char x[12] = { 's', 'h', 's', 'c' };
    WriteUnaligned<ui64>((ui64*)(x+4), id);
    return TActorId(0, TStringBuf(x, 12));
}

}
