#pragma once

#include "blob.h"
#include "pq_l2_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/events/internal.h>

namespace NKikimr {
namespace NPQ {

    struct TBlobId {
        TPartitionId Partition;
        ui64 Offset;
        ui16 PartNo;
        ui32 Count; // have to be unique for {Partition, Offset, partNo}
        ui16 InternalPartsCount; // have to be unique for {Partition, Offset, partNo}

        TBlobId(const TPartitionId& partition, ui64 offset, ui16 partNo, ui32 count, ui16 internalPartsCount)
            : Partition(partition)
            , Offset(offset)
            , PartNo(partNo)
            , Count(count)
            , InternalPartsCount(internalPartsCount)
        {
        }

        bool operator == (const TBlobId& r) const {
            return Partition.IsEqual(r.Partition) && Offset == r.Offset && PartNo == r.PartNo;
        }

        ui64 Hash() const {
            return Hash128to32((ui64(Partition.InternalPartitionId) << 17) + (Partition.IsSupportivePartition() ? 0 : (1 << 16)) + PartNo, Offset);
        }
    };
}}

template <>
struct THash<NKikimr::NPQ::TBlobId> {
    inline size_t operator() (const NKikimr::NPQ::TBlobId& key) const {
        return key.Hash();
    }
};

namespace NKikimr {
namespace NPQ {

    struct TKvRequest {
        enum ERequestType {
            TypeRead,
            TypeWrite
        };

        ERequestType Type;
        TActorId Sender;
        ui64 CookiePQ;
        TPartitionId Partition;
        ui32 MetadataWritesCount;
        TVector<TRequestedBlob> Blobs;

        TKvRequest(ERequestType type, TActorId sender, ui64 cookie, const TPartitionId& partition)
        : Type(type)
        , Sender(sender)
        , CookiePQ(cookie)
        , Partition(partition)
        , MetadataWritesCount(0)
        {}

        TBlobId GetBlobId(ui32 pos) const { return TBlobId(Partition, Blobs[pos].Offset, Blobs[pos].PartNo, Blobs[pos].Count, Blobs[pos].InternalPartsCount); }

        THolder<TEvKeyValue::TEvRequest> MakeKvRequest() const
        {
            THolder<TEvKeyValue::TEvRequest> request = MakeHolder<TEvKeyValue::TEvRequest>();
            for (auto& blob : Blobs) {
                if (blob.Value.empty()) {
                    // add reading command
                    TKey key(TKeyPrefix::TypeData, Partition, blob.Offset, blob.PartNo, blob.Count, blob.InternalPartsCount);
                    auto read = request->Record.AddCmdRead();
                    read->SetKey(key.Data(), key.Size());
                }
            }
            return request;
        }

        /// @note We should return blobs of size ~25 Mb. It's about 3 well-filled blobs.
        THolder<TEvPQ::TEvBlobResponse> MakePQResponse(const TActorContext& ctx, TErrorInfo error = TErrorInfo())
        {
            static const ui64 MAX_RESPONSE_SIZE = 24_MB;

            ui64 size = 0;
            ui32 cropped = 0;
            for (ui32 i = 0; i < Blobs.size(); ++i) {
                TRequestedBlob& blob = Blobs[i];
                if (blob.Value.size())
                    Verify(blob);
                size += blob.Value.size();
                if (size > MAX_RESPONSE_SIZE) {
                    ++cropped;
                    blob.Value.clear();
                }
            }

            if (cropped) {
                LOG_WARN_S(ctx, NKikimrServices::PERSQUEUE, "Cropped PQ response. Tablet: " << Sender
                    << "cookie " << CookiePQ << " partition " << Partition << " size " << size
                    << ". Cropped " << cropped << " blobs of " << Blobs.size());
            }

            return MakeHolder<TEvPQ::TEvBlobResponse>(CookiePQ, std::move(Blobs), error);
        }

        void Verify(const TRequestedBlob& blob) const {
            TKey key(TKeyPrefix::TypeData, TPartitionId(0), blob.Offset, blob.PartNo, blob.Count, blob.InternalPartsCount, false);
            Y_ABORT_UNLESS(blob.Value.size() == blob.Size);
            TClientBlob::CheckBlob(key, blob.Value);
        }
    };

    // TODO: better interface
    class TCacheEvictionStrategy {
    public:
        virtual ~TCacheEvictionStrategy()
        {}

        virtual void SaveHeadBlob(const TBlobId& blob) = 0;
        virtual void SaveUserOffset(TString client, const TPartitionId& partition, ui64 offset) = 0;
        virtual TDeque<TBlobId> BlobsToTouch() const = 0;
    };

    class TCacheStrategyKeepUsed : public TCacheEvictionStrategy {
    public:
        TCacheStrategyKeepUsed(ui64 size, ui64 maxBlobSize)
            : HeadBlobsCount(size / maxBlobSize)
        {}

        virtual ~TCacheStrategyKeepUsed()
        {}

        virtual void SaveHeadBlob(const TBlobId& blob) override
        {
            Head.push_back(blob);
            if (Head.size() > HeadBlobsCount)
                Head.pop_front();
        }

        virtual void SaveUserOffset(TString client, const TPartitionId& partition, ui64 offset) override
        {
            Y_UNUSED(client);
            Y_UNUSED(partition);
            Y_UNUSED(offset);
#if 0 // TODO: prevent unlimited memory growth
            UserOffset[std::make_pair(partition, client)] = offset;
#endif
        }

        virtual TDeque<TBlobId> BlobsToTouch() const override
        {
            return Head;
        }

    private:
        ui32 HeadBlobsCount;
        TDeque<TBlobId> Head;
    };

    /// Intablet (L1) cache logic
    class TIntabletCache {
    public:
        struct TValueL1 {
            enum ESource : ui32 {
                SourcePrefetch,
                SourceHead
            };

            ui32 DataSize;
            ESource Source;

            TValueL1(TCacheValue::TPtr blob, ui64 size, ESource source)
                : DataSize(size)
                , Source(source)
                , Blob(blob)
            {}

            TValueL1()
                : DataSize(0)
                , Source(SourcePrefetch)
            {}

            const TCacheValue::TPtr GetBlob() const { return Blob.lock(); }

        private:
            TCacheValue::TWeakPtr Blob;
        };

        using TMapType = THashMap<TBlobId, TValueL1>;

        struct TCounters {
            ui64 SizeBytes = 0;
            ui64 CachedOnRead = 0;
            ui64 CachedOnWrite = 0;

            void Clear() {
                SizeBytes = 0;
                CachedOnRead = 0;
                CachedOnWrite = 0;
            }

            void Inc(const TValueL1& value) {
                SizeBytes += value.DataSize;
                switch (value.Source) {
                case TValueL1::SourcePrefetch:
                    ++CachedOnRead;
                    break;
                case TValueL1::SourceHead:
                    ++CachedOnWrite;
                    break;
                }
            }

            void Dec(const TValueL1& value) {
                SizeBytes -= value.DataSize;
                switch (value.Source) {
                case TValueL1::SourcePrefetch:
                    --CachedOnRead;
                    break;
                case TValueL1::SourceHead:
                    --CachedOnWrite;
                    break;
                }
            }
        };

        explicit TIntabletCache(ui64 tabletId)
            : TabletId(tabletId)
            , L1Strategy(nullptr)
        {
        }

        const TMapType& CachedMap() const { return Cache; }
        ui64 GetSize() const { return Cache.size(); }
        const TCounters& GetCounters() const { return Counters; }

        void SetUserOffset(const TActorContext& ctx, TString client, const TPartitionId& partition, ui64 offset)
        {
            if (L1Strategy) {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Setting reader offset. User: "
                    << client << " Partition: " << partition << " offset " << offset);
                L1Strategy->SaveUserOffset(client, partition, offset);
            }
        }

        /// @return count of cached blobs
        ui32 RequestBlobs(const TActorContext& ctx, TKvRequest& kvReq)
        {
            ui32 fromCache = GetBlobs(ctx, kvReq);

            THolder<TCacheL2Request> reqData = MakeHolder<TCacheL2Request>(TabletId);

            for (const auto& blob : kvReq.Blobs) {
                // Touching blobs in L2. We don't need data here
                TCacheBlobL2 key = {kvReq.Partition, blob.Offset, blob.PartNo, nullptr};
                if (blob.Cached)
                    reqData->RequestedBlobs.push_back(key);
                else
                    reqData->MissedBlobs.push_back(key);
            }

            THolder<TEvPqCache::TEvCacheL2Request> l2Request = MakeHolder<TEvPqCache::TEvCacheL2Request>(reqData.Release());
            ctx.Send(MakePersQueueL2CacheID(), l2Request.Release()); // -> L2
            return fromCache;
        }

        void SaveHeadBlobs(const TActorContext& ctx, const TKvRequest& kvReq)
        {
            THolder<TCacheL2Request> reqData = MakeHolder<TCacheL2Request>(TabletId);

            for (const TRequestedBlob& reqBlob : kvReq.Blobs) {
                TBlobId blob(kvReq.Partition, reqBlob.Offset, reqBlob.PartNo, reqBlob.Count, reqBlob.InternalPartsCount);
                { // there could be a new blob with same id (for big messages)
                    if (RemoveExists(ctx, blob)) {
                        TCacheBlobL2 removed = {kvReq.Partition, reqBlob.Offset, reqBlob.PartNo, nullptr};
                        reqData->RemovedBlobs.push_back(removed);
                    }
                }

                TCacheValue::TPtr cached(new TCacheValue(reqBlob.Value, ctx.SelfID, TAppData::TimeProvider->Now()));
                TValueL1 valL1(cached, cached->DataSize(), TValueL1::SourceHead);
                Cache[blob] = valL1; // weak
                Counters.Inc(valL1);
                if (L1Strategy)
                    L1Strategy->SaveHeadBlob(blob);

                TCacheBlobL2 blobL2 = {kvReq.Partition, reqBlob.Offset, reqBlob.PartNo, cached};
                reqData->StoredBlobs.push_back(blobL2);

                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Caching head blob in L1. Partition "
                    << blob.Partition << " offset " << blob.Offset << " count " << blob.Count
                    << " size " << reqBlob.Value.size() << " actorID " << ctx.SelfID);
            }

            THolder<TEvPqCache::TEvCacheL2Request> l2Request = MakeHolder<TEvPqCache::TEvCacheL2Request>(reqData.Release());
            ctx.Send(MakePersQueueL2CacheID(), l2Request.Release()); // -> L2
        }

        void SavePrefetchBlobs(const TActorContext& ctx, const TKvRequest& kvReq, const TVector<bool>& store)
        {
            Y_ABORT_UNLESS(store.size() == kvReq.Blobs.size());

            THolder<TCacheL2Request> reqData = MakeHolder<TCacheL2Request>(TabletId);

            bool haveSome = false;
            for (ui32 i = 0; i < kvReq.Blobs.size(); ++i) {
                if (!store[i])
                    continue;

                const TRequestedBlob& reqBlob = kvReq.Blobs[i];
                TBlobId blob(kvReq.Partition, reqBlob.Offset, reqBlob.PartNo, reqBlob.Count, reqBlob.InternalPartsCount);
                {
                    TValueL1 value;
                    if (CheckExists(ctx, blob, value)) {
                        Y_ABORT_UNLESS(value.Source == TValueL1::SourceHead);
                        continue;
                    }
                }

                TCacheValue::TPtr cached(new TCacheValue(reqBlob.Value, ctx.SelfID, TAppData::TimeProvider->Now()));
                TValueL1 valL1(cached, cached->DataSize(), TValueL1::SourcePrefetch);
                Cache[blob] = valL1; // weak
                Counters.Inc(valL1);

                TCacheBlobL2 blobL2 = {kvReq.Partition, reqBlob.Offset, reqBlob.PartNo, cached};
                reqData->StoredBlobs.push_back(blobL2);

                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Prefetched blob in L1. Partition "
                    << blob.Partition << " offset " << blob.Offset << " count " << blob.Count
                    << " size " << reqBlob.Value.size()  << " actorID " << ctx.SelfID);
                haveSome = true;
            }

            if (haveSome) {
                THolder<TEvPqCache::TEvCacheL2Request> l2Request = MakeHolder<TEvPqCache::TEvCacheL2Request>(reqData.Release());
                ctx.Send(MakePersQueueL2CacheID(), l2Request.Release()); // -> L2
            }
        }

        void RemoveEvictedBlob(const TActorContext& ctx, const TBlobId& blob, TCacheValue::TPtr value)
        {
            auto it = Cache.find(blob);
            if (it == Cache.end()) {
                LOG_ERROR_S(ctx, NKikimrServices::PERSQUEUE, "Can't evict. No such blob in L1. Partition "
                    << blob.Partition << " offset " << blob.Offset << " size " << value->DataSize()
                    << " cause it's been evicted from L2. Actual L1 size: " << Cache.size());
                return;
            }

            auto sp = it->second.GetBlob();
            Y_ABORT_UNLESS(sp.get() == value.get(),
                "Evicting strange blob. Partition %d offset %ld partNo %d size %ld. L1 ptr %p vs L2 ptr %p",
                blob.Partition.InternalPartitionId, blob.Offset, blob.PartNo, value->DataSize(), sp.get(), value.get());

            RemoveBlob(it);

            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Erasing blob in L1. Partition "
                << blob.Partition << " offset " << blob.Offset << " size " << value->DataSize()
                << " cause it's been evicted from L2. Actual L1 size: " << Cache.size());
        }

        void Touch(const TActorContext& ctx)
        {
            RemoveEvicted();

            if (L1Strategy) {
                THolder<TCacheL2Request> reqData = MakeHolder<TCacheL2Request>(TabletId);

                TDeque<TBlobId> needTouch = L1Strategy->BlobsToTouch();
                PrepareTouch(ctx, reqData, needTouch);

                THolder<TEvPqCache::TEvCacheL2Request> l2Request = MakeHolder<TEvPqCache::TEvCacheL2Request>(reqData.Release());
                ctx.Send(MakePersQueueL2CacheID(), l2Request.Release()); // -> L2
            }
        }

    private:
        void PrepareTouch(const TActorContext& ctx, THolder<TCacheL2Request>& reqData, const TDeque<TBlobId>& used)
        {
            for (auto& blob : used) {
                TCacheBlobL2 blobL2 = {blob.Partition, blob.Offset, blob.PartNo, nullptr};
                reqData->ExpectedBlobs.push_back(blobL2);

                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Touching blob. Partition "
                    << blob.Partition << " offset " << blob.Offset << " count " << blob.Count);
            }
        }

        TCacheValue::TPtr GetValue(const TActorContext& ctx, const TBlobId& blobId)
        {
            const auto it = Cache.find(blobId);
            if (it == Cache.end()) {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "No blob in L1. Partition "
                    << blobId.Partition << " offset " << blobId.Offset << " actorID " << ctx.SelfID);
                return nullptr;
            }

            TCacheValue::TPtr data = it->second.GetBlob();
            if (!data) {
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Evicted blob in L1. Partition "
                    << blobId.Partition << " offset " << blobId.Offset << " actorID " << ctx.SelfID);
                RemoveBlob(it);
                return nullptr;
            }

            Y_ABORT_UNLESS(data->DataSize() == it->second.DataSize, "Mismatch L1-L2 blob sizes");

            const TBlobId& blob = it->first;
            LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Got data from cache. Partition "
                << blob.Partition << " offset " << blob.Offset << " count " << blob.Count
                << " source " << (ui32)it->second.Source << " size " << data->DataSize()
                << " accessed " << data->GetAccessCount() << " times before, last time " << data->GetAccessTime());

            return data;
        }

        ui32 GetBlobs(const TActorContext& ctx, TKvRequest& kvReq)
        {
            ui32 numCached = 0;
            for (auto& blob : kvReq.Blobs) {
                if (blob.Cached) {
                    ++numCached;
                    continue;
                }
                TBlobId blobId(kvReq.Partition, blob.Offset, blob.PartNo, blob.Count, blob.InternalPartsCount);
                TCacheValue::TPtr cached = GetValue(ctx, blobId);
                if (cached) {
                    ++numCached;
                    blob.Value = cached->GetValue();
                    blob.Cached = true;
                    Y_ABORT_UNLESS(blob.Value.size(), "Got empty blob from cache");
                }
            }
            return numCached;
        }

        void RemoveEvicted()
        {
            TMapType actual;
            for (const auto& c: Cache) {
                auto ptr = c.second.GetBlob();
                if (ptr) {
                    actual[c.first] = c.second;
                } else {
                    Counters.Dec(c.second);
                }
            }
            Cache.swap(actual);
        }

    private:
        ui64 TabletId;
        TMapType Cache;
        TCounters Counters;
        THolder<TCacheEvictionStrategy> L1Strategy;

        void RemoveBlob(const TMapType::iterator& it)
        {
            Counters.Dec(it->second);
            Cache.erase(it);
        }

        bool CheckExists(const TActorContext& ctx, const TBlobId& blob, TValueL1& out, bool remove = false)
        {
            auto it = Cache.find(blob);
            if (it != Cache.end()) {
                out = it->second;
                Y_ABORT_UNLESS(out.GetBlob(), "Duplicate blob in L1 with no data");
                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Duplicate blob in L1. "
                    << "Partition " << blob.Partition << " offset " << blob.Offset << " count " << blob.Count
                    << " size " << out.DataSize << " actorID " << ctx.SelfID
                    << " is actual " << (bool)out.GetBlob());
                if (remove)
                    RemoveBlob(it);
                return true;
            }
            return false;
        }

        bool RemoveExists(const TActorContext& ctx, const TBlobId& blob)
        {
            TValueL1 value;
            return CheckExists(ctx, blob, value, true);
        }
    };

} //NPQ
} //NKikimr
