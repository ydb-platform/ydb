#pragma once
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/protos/shared_cache.pb.h>
#include <ydb/core/util/cache_cache.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr::NSharedCache {

using TSharedCacheConfig = NKikimrSharedCache::TSharedCacheConfig;

struct TSharedPageCacheCounters final : public TAtomicRefCount<TSharedPageCacheCounters> {
    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
    using TReplacementPolicy = NKikimrSharedCache::TReplacementPolicy;

    const TIntrusivePtr<::NMonitoring::TDynamicCounters> Counters;

    const TCounterPtr FreshBytes;
    const TCounterPtr StagingBytes;
    const TCounterPtr WarmBytes;

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

    explicit TSharedPageCacheCounters(const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters);

    TCounterPtr ReplacementPolicySize(TReplacementPolicy policy);
};

IActor* CreateSharedPageCache(
    const TSharedCacheConfig& config,
    const TIntrusivePtr<::NMonitoring::TDynamicCounters>& counters
);

inline TActorId MakeSharedPageCacheId(ui64 id = 0) {
    char x[12] = { 's', 'h', 's', 'c' };
    WriteUnaligned<ui64>((ui64*)(x+4), id);
    return TActorId(0, TStringBuf(x, 12));
}

}
