#pragma once

#include "partition_id.h"

#include <ydb/library/actors/core/defs.h>
#include <ydb/library/actors/core/actor.h>

#include <atomic>

namespace NKikimr {
namespace NPQ {

inline TActorId MakePersQueueL2CacheID() {
    static_assert(TActorId::MaxServiceIDLength == 12, "Unexpected actor id length");
    const char x[12] = "pq_l2_cache";
    return TActorId(0, TStringBuf(x, 12));
}

struct TCacheL2Parameters {
    ui32 MaxSizeMB;
    TDuration KeepTime;
};

IActor* CreateNodePersQueueL2Cache(const TCacheL2Parameters&, TIntrusivePtr<::NMonitoring::TDynamicCounters>);

//

struct TCacheValue : TNonCopyable {
    using TPtr = std::shared_ptr<TCacheValue>;
    using TWeakPtr = std::weak_ptr<TCacheValue>;

    TCacheValue(TString value, TActorId owner, TInstant accessTime)
        : Value(value)
        , Owner(owner)
        , AccessTime(accessTime.TimeT())
        , AccessCount(0)
    {}

    TInstant GetAccessTime() const {
        return TInstant::Seconds(AccessTime);
    }

    void Touch(TInstant atime) {
        ++AccessCount;
        AccessTime = atime.TimeT();
    }

    ui32 GetAccessCount() const {
        return AccessCount;
    }

    TString GetValue() const {
        return Value;
    }

    size_t DataSize() const {
        return Value.size();
    }

    const TActorId& GetOwner() const {
        return Owner;
    }

private:
    const TString Value;
    const TActorId Owner;
    std::atomic<ui64> AccessTime;
    std::atomic<ui32> AccessCount;
};

struct TCacheBlobL2 {
    TPartitionId Partition;
    ui64 Offset;
    ui16 PartNo;
    TCacheValue::TPtr Value;
};

struct TCacheL2Request {
    ui64 TabletId;
    ui64 Cookie;
    TVector<TCacheBlobL2> RequestedBlobs;
    TVector<TCacheBlobL2> StoredBlobs;
    TVector<TCacheBlobL2> RemovedBlobs;
    TVector<TCacheBlobL2> ExpectedBlobs;
    TVector<TCacheBlobL2> MissedBlobs;

    explicit TCacheL2Request(ui64 tabletId)
        : TabletId(tabletId)
        , Cookie(0)
    {}
};

struct TCacheL2Response {
    ui64 Cookie = 0;
    ui64 TabletId; // Deprecated
    bool Overload = false;
    TVector<TCacheBlobL2> Removed;
};


struct TEvPqCache {
    enum EEv {
        EvCacheRequest = EventSpaceBegin(TKikimrEvents::ES_PQ_L2_CACHE),
        EvCacheResponse,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_PQ_L2_CACHE), "Unexpected TEvPqL2Cache event range");

    struct TEvCacheL2Request : public TEventLocal<TEvCacheL2Request, EvCacheRequest> {
        THolder<TCacheL2Request> Data;

        explicit TEvCacheL2Request(TAutoPtr<TCacheL2Request> data)
            : Data(data)
        {}
    };

    struct TEvCacheL2Response : public TEventLocal<TEvCacheL2Response, EvCacheResponse> {
        THolder<TCacheL2Response> Data;

        TEvCacheL2Response(TAutoPtr<TCacheL2Response> data)
            : Data(data)
        {}
    };
};

} // NPQ
} // NKikimr
