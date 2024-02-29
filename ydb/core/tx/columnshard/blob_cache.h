#pragma once

#include "blob.h"

#include <ydb/core/tx/ctor_logger.h>
#include <ydb/core/base/logoblob.h>
#include <ydb/core/base/events.h>
#include <ydb/core/base/blobstorage.h>

#include <library/cpp/monlib/dynamic_counters/counters.h>
#include <ydb/library/actors/core/actorid.h>
#include <ydb/library/actors/core/event_local.h>

#include <util/generic/vector.h>

namespace NKikimr::NBlobCache {

using NOlap::TUnifiedBlobId;
using NOlap::TBlobRange;

using TLogThis = TCtorLogger<NKikimrServices::BLOB_CACHE>;

struct TReadBlobRangeOptions {
    bool CacheAfterRead;
    bool IsBackgroud;
    bool WithDeadline = true;

    TString ToString() const {
        return TStringBuilder() << "cache: " << (ui32)CacheAfterRead
            << " background: " << (ui32)IsBackgroud
            << " dedlined: " << (ui32)WithDeadline;
    }
};

struct TEvBlobCache {
    enum EEv {
        EvReadBlobRange = EventSpaceBegin(TKikimrEvents::ES_BLOB_CACHE),
        EvReadBlobRangeBatch,
        EvReadBlobRangeResult,
        EvCacheBlobRange,
        EvForgetBlob,

        EvEnd
    };

    static_assert(EvEnd < EventSpaceEnd(TKikimrEvents::ES_BLOB_CACHE), "Unexpected TEvBlobCache event range");

    struct TEvReadBlobRange : public NActors::TEventLocal<TEvReadBlobRange, EvReadBlobRange> {
        TBlobRange BlobRange;
        TReadBlobRangeOptions ReadOptions;

        explicit TEvReadBlobRange(const TBlobRange& blobRange, TReadBlobRangeOptions&& opts)
            : BlobRange(blobRange)
            , ReadOptions(std::move(opts))
        {}
    };

    // Read a batch of ranges from the same DS group
    // This is usefull to save IOPs when reading multiple columns from the same blob
    struct TEvReadBlobRangeBatch : public NActors::TEventLocal<TEvReadBlobRangeBatch, EvReadBlobRangeBatch> {
        std::vector<TBlobRange> BlobRanges;
        TReadBlobRangeOptions ReadOptions;

        explicit TEvReadBlobRangeBatch(std::vector<TBlobRange>&& blobRanges, TReadBlobRangeOptions&& opts)
            : BlobRanges(std::move(blobRanges))
            , ReadOptions(std::move(opts))
        {
        }
    };

    struct TEvReadBlobRangeResult : public NActors::TEventLocal<TEvReadBlobRangeResult, EvReadBlobRangeResult> {
        TBlobRange BlobRange;
        NKikimrProto::EReplyStatus Status;
        TString Data;
        const bool FromCache = false;
        const TInstant ConstructTime = Now();
        const TString DataSourceId;

        TEvReadBlobRangeResult(const TBlobRange& blobRange, NKikimrProto::EReplyStatus status, const TString& data, const bool fromCache = false, const TString& dataSourceId = Default<TString>())
            : BlobRange(blobRange)
            , Status(status)
            , Data(data)
            , FromCache(fromCache)
            , DataSourceId(dataSourceId)
        {}
    };

    // Put a blob range data into cache. This helps to reduce number of reads from disks done by indexing, compactions
    // and queries that read recent data
    struct TEvCacheBlobRange : public NActors::TEventLocal<TEvCacheBlobRange, EvCacheBlobRange> {
        TBlobRange BlobRange;
        TString Data;

        TEvCacheBlobRange(const TBlobRange& blobRange, const TString& data)
            : BlobRange(blobRange)
            , Data(data)
        {}
    };

    // Notify the cache that this blob will not be requested any more
    // (e.g. when it was deleted after indexing or compaction)
    struct TEvForgetBlob : public NActors::TEventLocal<TEvForgetBlob, EvForgetBlob> {
        TUnifiedBlobId BlobId;

        explicit TEvForgetBlob(const TUnifiedBlobId& blobId)
            : BlobId(blobId)
        {}
    };
};

inline
NActors::TActorId MakeBlobCacheServiceId() {
    static_assert(TActorId::MaxServiceIDLength == 12, "Unexpected actor id length");
    const char x[12] = "blob_cache";
    return TActorId(0, TStringBuf(x, 12));
}

NActors::IActor* CreateBlobCache(ui64 maxBytes, TIntrusivePtr<::NMonitoring::TDynamicCounters>);

// Explicitly add and remove data from cache. This is usefull for newly written data that is likely to be read by
// indexing, compaction and user queries and for the data that has been compacted and will not be read again.
void AddRangeToCache(const TBlobRange& blobRange, const TString& data);
void ForgetBlob(const TUnifiedBlobId& blobId);

}
