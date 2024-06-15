#pragma once

#include "read.h"
#include "pq_l2_service.h"

#include <ydb/core/protos/pqconfig.pb.h>
#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <library/cpp/cache/cache.h>

#include <util/generic/hash.h>

namespace NKikimr {
namespace NPQ {

struct TL2Counters {
    ::NMonitoring::TDynamicCounters::TCounterPtr TotalSize;
    ::NMonitoring::TDynamicCounters::TCounterPtr TotalCount;
    ::NMonitoring::TDynamicCounters::TCounterPtr Hits;
    ::NMonitoring::TDynamicCounters::TCounterPtr Misses;
    ::NMonitoring::TDynamicCounters::TCounterPtr Touches;
    ::NMonitoring::TDynamicCounters::TCounterPtr Evictions;
    ::NMonitoring::TDynamicCounters::TCounterPtr Used;
    ::NMonitoring::TDynamicCounters::TCounterPtr Unused;
    ::NMonitoring::TDynamicCounters::TCounterPtr Retention;

    TL2Counters(TIntrusivePtr<::NMonitoring::TDynamicCounters> group)
    {
        TotalSize = group->GetCounter("NodeCacheSizeBytes", false);
        TotalCount = group->GetCounter("NodeCacheSizeBlobs", false);
        Hits = group->GetCounter("NodeCacheHits", true);
        Misses = group->GetCounter("NodeCacheMisses", true);
        Touches = group->GetCounter("NodeCacheTouches", true);
        Evictions = group->GetCounter("NodeCacheEvictions", true);
        Used = group->GetCounter("NodeCacheEvictUsed", true);
        Unused = group->GetCounter("NodeCacheEvictUnused", true);
        Retention = group->GetCounter("NodeCacheRetentionTimeSeconds", false);
    }
};

/// PersQueue shared (L2) cache
class TPersQueueCacheL2 : public TActorBootstrapped<TPersQueueCacheL2> {
public:
    struct TKey {
        ui64 TabletId;
        TPartitionId Partition;
        ui64 Offset;
        ui16 PartNo;

        TKey(ui64 tabletId, const TCacheBlobL2& blob)
            : TabletId(tabletId)
            , Partition(blob.Partition)
            , Offset(blob.Offset)
            , PartNo(blob.PartNo)
        {
            KeyHash = Hash128to32(TabletId, (static_cast<ui64>(Partition.InternalPartitionId) << 17) + PartNo + (Partition.IsSupportivePartition() ? 0 : (1 << 16)));
            KeyHash = Hash128to32(KeyHash, Offset);
        }

        bool operator == (const TKey& key) const {
            return TabletId == key.TabletId &&
                Partition.IsEqual(key.Partition) &&
                Offset == key.Offset &&
                PartNo == key.PartNo;
        }

        ui64 Hash() const noexcept {
            return KeyHash;
        }

    private:
        ui64 KeyHash;
    };

    static constexpr NKikimrServices::TActivity::EType ActorActivityType() {
        return NKikimrServices::TActivity::PERSQUEUE_CACHE_L2_ACTOR;
    }

    TPersQueueCacheL2(const TCacheL2Parameters& params, TIntrusivePtr<::NMonitoring::TDynamicCounters> countersGroup)
        : Cache(1_TB / MAX_BLOB_SIZE) // It's some "much bigger then we need" size here.
        , MaxSize(ClampMinSize(params.MaxSizeMB * 1_MB))
        , CurrentSize(0)
        , KeepTime(params.KeepTime)
        , RetentionTime(TDuration::Zero())
        , Counters(countersGroup)
    {}

    void Bootstrap(const TActorContext& ctx);

private:
    STFUNC(StateFunc)
    {
        TRACE_EVENT(NKikimrServices::PERSQUEUE);
        switch (ev->GetTypeRewrite()) {
            HFuncTraced(TEvents::TEvPoisonPill, Handle);
            HFuncTraced(TEvPqCache::TEvCacheL2Request, Handle);
            HFuncTraced(NMon::TEvHttpInfo, Handle);
        default:
            break;
        };
    }

    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx)
    {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void Handle(NMon::TEvHttpInfo::TPtr& ev, const TActorContext& ctx);

    void Handle(TEvPqCache::TEvCacheL2Request::TPtr& ev, const TActorContext& ctx);
    void SendResponses(const TActorContext& ctx, const THashMap<TKey, TCacheValue::TPtr>& evicted);

    void AddBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs,
                  THashMap<TKey, TCacheValue::TPtr>& outEvicted);
    void RemoveBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs);
    void TouchBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs, bool isHit = true);
    void RegretBlobs(const TActorContext& ctx, ui64 tabletId, const TVector<TCacheBlobL2>& blobs);

    static ui64 ClampMinSize(ui64 maxSize) {
        static const ui64 MIN_SIZE = 32_MB;
        return std::clamp(maxSize, MIN_SIZE, std::numeric_limits<ui64>::max());
    }

private:
    TLRUCache<TKey, TCacheValue::TPtr> Cache;
    ui64 MaxSize;
    ui64 CurrentSize;
    TDuration KeepTime;
    TDuration RetentionTime;
    TL2Counters Counters;

    TString HttpForm() const;
};

} // NPQ
} // NKikimr


template <>
struct THash<NKikimr::NPQ::TPersQueueCacheL2::TKey> {
    inline size_t operator() (const NKikimr::NPQ::TPersQueueCacheL2::TKey& key) const {
        return key.Hash();
    }
};
