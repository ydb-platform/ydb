#pragma once

#include "shared_cache_counters.h"
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/protos/shared_cache.pb.h>
#include <ydb/core/util/cache_cache.h>
#include <util/system/unaligned_mem.h>

namespace NKikimr::NSharedCache {

using TSharedCacheConfig = NKikimrSharedCache::TSharedCacheConfig;

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
