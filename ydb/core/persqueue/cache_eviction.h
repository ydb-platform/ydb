#pragma once

#include "blob.h"
#include "pq_l2_service.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/persqueue/events/internal.h>
#include <ydb/core/persqueue/map_subrange.h>

namespace NKikimr::NPQ {
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

        bool operator<(const TBlobId& r) const {
            auto makeTuple = [](const TBlobId& v) {
                return std::make_tuple(v.Partition, v.Offset, v.PartNo, v.Count, v.InternalPartsCount);
            };

            return makeTuple(*this) < makeTuple(r);
        }

        bool operator==(const TBlobId& r) const {
            auto makeTuple = [](const TBlobId& v) {
                return std::make_tuple(v.Partition, v.Offset, v.PartNo, v.Count, v.InternalPartsCount);
            };

            return makeTuple(*this) == makeTuple(r);
        }

        ui64 Hash() const {
            ui64 hash = Hash128to32((ui64(Partition.InternalPartitionId) << 17) + (Partition.IsSupportivePartition() ? 0 : (1 << 16)) + PartNo, Offset);
            hash = Hash128to32(hash, Count);
            hash = Hash128to32(hash, InternalPartsCount);
            return hash;
        }
    };
}

template <>
struct THash<NKikimr::NPQ::TBlobId> {
    inline size_t operator() (const NKikimr::NPQ::TBlobId& key) const {
        return key.Hash();
    }
};

namespace NKikimr::NPQ {
    inline TBlobId MakeBlobId(const TPartitionId& partitionId, const TRequestedBlob& blob) {
        return {partitionId, blob.Offset, blob.PartNo, blob.Count, blob.InternalPartsCount};
    }

    struct TKvRequest {
        enum ERequestType {
            TypeRead,
            TypeWrite
        };

        struct TDeleteBlobRange {
            TString Begin;
            bool IncludeBegin;
            TString End;
            bool IncludeEnd;
        };

        struct TRenameBlob {
            TString From;
            TString To;
        };

        ERequestType Type;
        TActorId Sender;
        ui64 CookiePQ;
        TPartitionId Partition;
        ui32 MetadataWritesCount;
        TVector<TRequestedBlob> Blobs;
        TVector<TDeleteBlobRange> DeletedBlobs;
        TVector<TRenameBlob> RenamedBlobs;

        TKvRequest(ERequestType type, TActorId sender, ui64 cookie, const TPartitionId& partition)
        : Type(type)
        , Sender(sender)
        , CookiePQ(cookie)
        , Partition(partition)
        , MetadataWritesCount(0)
        {}

        TBlobId GetBlobId(ui32 pos) const {
            return NPQ::MakeBlobId(Partition, Blobs[pos]);
        }

        THolder<TEvKeyValue::TEvRequest> MakeKvRequest() const
        {
            auto request = MakeHolder<TEvKeyValue::TEvRequest>();
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

            const TCacheValue::TPtr GetBlob() const { return Blob; }

        private:
            TCacheValue::TPtr Blob;
        };

        using TMapType = TMap<TBlobId, TValueL1>;

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

        /// @return count of cached blobs
        ui32 RequestBlobs(const TActorContext& ctx, TKvRequest& kvReq)
        {
            ui32 fromCache = GetBlobs(ctx, kvReq);

            auto reqData = MakeHolder<TCacheL2Request>(TabletId);

            for (const auto& blob : kvReq.Blobs) {
                // Touching blobs in L2. We don't need data here
                auto& blobs = blob.Cached ? reqData->RequestedBlobs : reqData->MissedBlobs;
                blobs.emplace_back(kvReq.Partition, blob.Offset, blob.PartNo, blob.Count, blob.InternalPartsCount, nullptr);
            }

            auto l2Request = MakeHolder<TEvPqCache::TEvCacheL2Request>(reqData.Release());
            ctx.Send(MakePersQueueL2CacheID(), l2Request.Release()); // -> L2
            return fromCache;
        }

        void SaveHeadBlobs(const TActorContext& ctx, const TKvRequest& kvReq)
        {
            auto reqData = MakeHolder<TCacheL2Request>(TabletId);

            DeleteBlobs(kvReq, *reqData, ctx);
            RenameBlobs(kvReq, *reqData, ctx);
            SaveBlobs(kvReq, *reqData, ctx);

            auto l2Request = MakeHolder<TEvPqCache::TEvCacheL2Request>(reqData.Release());
            ctx.Send(MakePersQueueL2CacheID(), l2Request.Release()); // -> L2
        }

        void SaveBlobs(const TKvRequest& kvReq, TCacheL2Request& reqData, const TActorContext& ctx)
        {
            for (const TRequestedBlob& reqBlob : kvReq.Blobs) {
                TBlobId blob = NPQ::MakeBlobId(kvReq.Partition, reqBlob);

                // there could be a new blob with same id (for big messages)
                if (RemoveExists(ctx, blob)) {
                    reqData.RemovedBlobs.emplace_back(kvReq.Partition, reqBlob.Offset, reqBlob.PartNo, reqBlob.Count, reqBlob.InternalPartsCount, nullptr);
                }

                auto cached = std::make_shared<TCacheValue>(reqBlob.Value, ctx.SelfID, TAppData::TimeProvider->Now());
                TValueL1 valL1(cached, cached->DataSize(), TValueL1::SourceHead);
                Cache[blob] = valL1; // weak
                Counters.Inc(valL1);
                if (L1Strategy)
                    L1Strategy->SaveHeadBlob(blob);

                reqData.StoredBlobs.emplace_back(kvReq.Partition, reqBlob.Offset, reqBlob.PartNo, blob.Count, blob.InternalPartsCount, cached);

                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Caching head blob in L1. Partition "
                    << blob.Partition << " offset " << blob.Offset << " count " << blob.Count
                    << " size " << reqBlob.Value.size() << " actorID " << ctx.SelfID);
            }
        }

        static TBlobId MakeBlobId(const TString& s)
        {
            if (s.length() == TKeyPrefix::MarkPosition()) {
                TPartitionId partitionId;
                partitionId.OriginalPartitionId = FromString<ui32>(s.data() + 1, 10);
                partitionId.InternalPartitionId = partitionId.OriginalPartitionId;
                return {partitionId, 0, 0, 0, 0};
            } else {
                TKey key(s);
                return {key.GetPartition(), key.GetOffset(), key.GetPartNo(), key.GetCount(), key.GetInternalPartsCount()};
            }
        }

        void RenameBlobs(const TKvRequest& kvReq, TCacheL2Request& reqData, const TActorContext& ctx)
        {
            for (const auto& [oldKey, newKey] : kvReq.RenamedBlobs) {
                TBlobId oldBlob = MakeBlobId(oldKey);
                TBlobId newBlob = MakeBlobId(newKey);
                if (RenameExists(ctx, oldBlob, newBlob)) {
                    reqData.RenamedBlobs.emplace_back(std::piecewise_construct,
                                                      std::make_tuple(oldBlob.Partition, oldBlob.Offset, oldBlob.PartNo, oldBlob.Count, oldBlob.InternalPartsCount, nullptr),
                                                      std::make_tuple(newBlob.Partition, newBlob.Offset, newBlob.PartNo, newBlob.Count, newBlob.InternalPartsCount, nullptr));

                    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Renaming head blob in L1. Old partition "
                                << oldBlob.Partition << " old offset " << oldBlob.Offset << " old count " << oldBlob.Count
                                << " new partition " << newBlob.Partition << " new offset " << newBlob.Offset << " new count " << newBlob.Count
                                << " actorID " << ctx.SelfID);
                }
            }
        }

        void DeleteBlobs(const TKvRequest& kvReq, TCacheL2Request& reqData, const TActorContext& ctx)
        {
            for (const auto& range : kvReq.DeletedBlobs) {
                auto [lowerBound, upperBound] = MapSubrange(Cache,
                                                            MakeBlobId(range.Begin), range.IncludeBegin,
                                                            MakeBlobId(range.End), range.IncludeEnd);

                for (auto i = lowerBound; i != upperBound; ++i) {
                    const auto& [blob, value] = *i;

                    reqData.RemovedBlobs.emplace_back(blob.Partition, blob.Offset, blob.PartNo, blob.Count, blob.InternalPartsCount, nullptr);
                    Counters.Dec(value);

                    LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Deleting head blob in L1. Partition "
                                << blob.Partition << " offset " << blob.Offset << " count " << blob.Count
                                << " actorID " << ctx.SelfID);
                }

                Cache.erase(lowerBound, upperBound);
            }
        }

        void SavePrefetchBlobs(const TActorContext& ctx, const TKvRequest& kvReq, const TVector<bool>& store)
        {
            Y_ABORT_UNLESS(store.size() == kvReq.Blobs.size());

            auto reqData = MakeHolder<TCacheL2Request>(TabletId);

            bool haveSome = false;
            for (ui32 i = 0; i < kvReq.Blobs.size(); ++i) {
                if (!store[i])
                    continue;

                const TRequestedBlob& reqBlob = kvReq.Blobs[i];
                TBlobId blob = NPQ::MakeBlobId(kvReq.Partition, reqBlob);
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

                reqData->StoredBlobs.emplace_back(kvReq.Partition, reqBlob.Offset, reqBlob.PartNo, reqBlob.Count, reqBlob.InternalPartsCount, cached);

                LOG_DEBUG_S(ctx, NKikimrServices::PERSQUEUE, "Prefetched blob in L1. Partition "
                    << blob.Partition << " offset " << blob.Offset << " count " << blob.Count
                    << " size " << reqBlob.Value.size()  << " actorID " << ctx.SelfID);
                haveSome = true;
            }

            if (haveSome) {
                auto l2Request = MakeHolder<TEvPqCache::TEvCacheL2Request>(reqData.Release());
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
                auto reqData = MakeHolder<TCacheL2Request>(TabletId);

                TDeque<TBlobId> needTouch = L1Strategy->BlobsToTouch();
                PrepareTouch(ctx, reqData, needTouch);

                auto l2Request = MakeHolder<TEvPqCache::TEvCacheL2Request>(reqData.Release());
                ctx.Send(MakePersQueueL2CacheID(), l2Request.Release()); // -> L2
            }
        }

    private:
        void PrepareTouch(const TActorContext& ctx, THolder<TCacheL2Request>& reqData, const TDeque<TBlobId>& used)
        {
            for (auto& blob : used) {
                reqData->ExpectedBlobs.emplace_back(blob.Partition, blob.Offset, blob.PartNo, blob.Count, blob.InternalPartsCount, nullptr);

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
                TBlobId blobId = NPQ::MakeBlobId(kvReq.Partition, blob);
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

        bool RenameExists(const TActorContext& ctx, const TBlobId& oldBlob, const TBlobId& newBlob)
        {
            Y_UNUSED(ctx);

            auto it = Cache.find(oldBlob);
            if (it == Cache.end()) {
                return false;
            }

            TValueL1 value = it->second;
            Cache.erase(it);

            Cache[newBlob] = value;
            Counters.Inc(value);
            if (L1Strategy)
                L1Strategy->SaveHeadBlob(newBlob);

            return true;
        }
    };

} // NKikimr::NPQ
