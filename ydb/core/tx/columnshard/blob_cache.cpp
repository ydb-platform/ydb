#include "blob_cache.h"
#include "columnshard.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/tablet_pipe.h>

#include <library/cpp/actors/core/actor.h>
#include <library/cpp/actors/core/hfunc.h>
#include <library/cpp/cache/cache.h>

#include <util/string/vector.h>
#include <tuple>

namespace NKikimr::NBlobCache {

using namespace NActors;

class TBlobCache: public TActorBootstrapped<TBlobCache> {
private:
    struct TReadInfo {
        bool Cache;                 // Put in cache after read?
        TList<TActorId> Waiting;    // List of readers

        TReadInfo()
            : Cache(true)
        {}
    };

    struct TReadItem : public TReadBlobRangeOptions {
        enum class EReadVariant {
            FAST = 0,
            DEFAULT,
            DEFAULT_NO_DEADLINE,
        };

        TBlobRange BlobRange;

        TReadItem(const TReadBlobRangeOptions& opts, const TBlobRange& blobRange)
            : TReadBlobRangeOptions(opts)
            , BlobRange(blobRange)
        {
            Y_VERIFY(blobRange.BlobId.IsValid());
        }

        bool PromoteInCache() const {
            return CacheAfterRead;
        }

        static NKikimrBlobStorage::EGetHandleClass ReadClass(EReadVariant readVar) {
            return (readVar == EReadVariant::FAST)
                ? NKikimrBlobStorage::FastRead
                : NKikimrBlobStorage::AsyncRead;
        }

        EReadVariant ReadVariant() const {
            return IsBackgroud
                ? (WithDeadline ? EReadVariant::DEFAULT : EReadVariant::DEFAULT_NO_DEADLINE)
                : EReadVariant::FAST;
        }

        // Blobs with same tagret can be read in a single request
        // (e.g. DS blobs from the same tablet residing on the same DS group, or 2 small blobs from the same tablet)
        std::tuple<ui64, ui32, EReadVariant> BlobSource() const {
            const TUnifiedBlobId& blobId = BlobRange.BlobId;

            Y_VERIFY(blobId.IsValid());

            if (blobId.IsDsBlob()) {
                // Tablet & group restriction
                return {blobId.GetTabletId(), blobId.GetDsGroup(), ReadVariant()};
            } else if (blobId.IsSmallBlob()) {
                // Tablet restriction, no group restrictions
                return {blobId.GetTabletId(), 0, ReadVariant()};
            }

            return {0, 0, EReadVariant::FAST};
        }
    };

    static constexpr i64 MAX_IN_FLIGHT_BYTES = 250ll << 20;
    static constexpr i64 MAX_REQUEST_BYTES = 8ll << 20;
    static constexpr TDuration DEFAULT_READ_DEADLINE = TDuration::Seconds(30);
    static constexpr TDuration FAST_READ_DEADLINE = TDuration::Seconds(10);

    TLRUCache<TBlobRange, TString> Cache;
    THashMap<TUnifiedBlobId, THashSet<TBlobRange>> CachedRanges;   // List of cached ranges by blob id
                                                            // It is used to remove all blob ranges from cache when
                                                            // it gets a notification that a blob has been deleted
    TControlWrapper MaxCacheDataSize;
    TControlWrapper MaxCacheExternalDataSize; // Cache size for extern (i.e. S3) blobs
    TControlWrapper MaxInFlightDataSize;
    i64 CacheDataSize;              // Current size of all blobs in cache
    ui64 ReadCookie;
    THashMap<ui64, std::vector<TBlobRange>> CookieToRange;  // All in-flight requests
    THashMap<TBlobRange, TReadInfo> OutstandingReads;   // All in-flight and enqueued reads
    TDeque<TReadItem> ReadQueue;    // Reads that are waiting to be sent
                                    // TODO: Consider making per-group queues
    i64 InFlightDataSize;           // Current size of all in-flight blobs

    THashMap<ui64, TActorId> ShardPipes;    // TabletId -> PipeClient for small blob read requests
    THashMap<ui64, THashSet<ui64>> InFlightTabletRequests;  // TabletId -> list to read cookies

    using TCounterPtr = ::NMonitoring::TDynamicCounters::TCounterPtr;
    const TCounterPtr SizeBytes;
    const TCounterPtr SizeBlobs;
    const TCounterPtr Hits;
    const TCounterPtr Misses;
    const TCounterPtr Evictions;
    const TCounterPtr Adds;
    const TCounterPtr Forgets;
    const TCounterPtr HitsBytes;
    const TCounterPtr EvictedBytes;
    const TCounterPtr ReadBytes;
    const TCounterPtr AddBytes;
    const TCounterPtr ForgetBytes;
    const TCounterPtr SizeBytesInFlight;
    const TCounterPtr SizeBlobsInFlight;
    const TCounterPtr ReadRequests;
    const TCounterPtr ReadsInQueue;

public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BLOB_CACHE_ACTOR;
    }

public:
    explicit TBlobCache(ui64 maxSize, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : TActorBootstrapped<TBlobCache>()
        , Cache(SIZE_MAX)
        , MaxCacheDataSize(maxSize, 0, 1ull << 40)
        , MaxCacheExternalDataSize(0, 0, 1ull << 40)
        , MaxInFlightDataSize(Min<i64>(MaxCacheDataSize, MAX_IN_FLIGHT_BYTES), 0, 10ull << 30)
        , CacheDataSize(0)
        , ReadCookie(1)
        , InFlightDataSize(0)
        , SizeBytes(counters->GetCounter("SizeBytes"))
        , SizeBlobs(counters->GetCounter("SizeBlobs"))
        , Hits(counters->GetCounter("Hits", true))
        , Misses(counters->GetCounter("Misses", true))
        , Evictions(counters->GetCounter("Evictions", true))
        , Adds(counters->GetCounter("Adds", true))
        , Forgets(counters->GetCounter("Forgets", true))
        , HitsBytes(counters->GetCounter("HitsBytes", true))
        , EvictedBytes(counters->GetCounter("EvictedBytes", true))
        , ReadBytes(counters->GetCounter("ReadBytes", true))
        , AddBytes(counters->GetCounter("AddBytes", true))
        , ForgetBytes(counters->GetCounter("ForgetBytes", true))
        , SizeBytesInFlight(counters->GetCounter("SizeBytesInFlight"))
        , SizeBlobsInFlight(counters->GetCounter("SizeBlobsInFlight"))
        , ReadRequests(counters->GetCounter("ReadRequests", true))
        , ReadsInQueue(counters->GetCounter("ReadsInQueue"))
    {}

    void Bootstrap(const TActorContext& ctx) {
        auto& icb = AppData(ctx)->Icb;
        icb->RegisterSharedControl(MaxCacheDataSize, "BlobCache.MaxCacheDataSize");
        icb->RegisterSharedControl(MaxCacheExternalDataSize, "BlobCache.MaxCacheExternalDataSize");
        icb->RegisterSharedControl(MaxInFlightDataSize, "BlobCache.MaxInFlightDataSize");
        Become(&TBlobCache::StateFunc);
        ScheduleWakeup();
    }

private:
    STFUNC(StateFunc) {
        switch (ev->GetTypeRewrite()) {
            HFunc(TEvents::TEvPoisonPill, Handle);
            HFunc(TEvents::TEvWakeup, Handle);
            HFunc(TEvBlobCache::TEvReadBlobRange, Handle);
            HFunc(TEvBlobCache::TEvReadBlobRangeBatch, Handle);
            HFunc(TEvBlobCache::TEvCacheBlobRange, Handle);
            HFunc(TEvBlobCache::TEvForgetBlob, Handle);
            HFunc(TEvBlobStorage::TEvGetResult, Handle);
            HFunc(TEvTabletPipe::TEvClientConnected, Handle);
            HFunc(TEvTabletPipe::TEvClientDestroyed, Handle);
            HFunc(TEvColumnShard::TEvReadBlobRangesResult, Handle);
        default:
            LOG_S_WARN("Unhandled event type: " << ev->GetTypeRewrite()
                       << " event: " << ev->ToString());
            ctx.Send(IEventHandle::ForwardOnNondelivery(ev, TEvents::TEvUndelivered::ReasonActorUnknown));
            break;
        };
    }

    void ScheduleWakeup() {
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Evict(ctx);         // Max cache size might have changed
        ScheduleWakeup();
    }

    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void Handle(TEvBlobCache::TEvReadBlobRange::TPtr& ev, const TActorContext& ctx) {
        const TBlobRange& blobRange = ev->Get()->BlobRange;
        const bool promote = ev->Get()->ReadOptions.CacheAfterRead;
        const bool fallback = ev->Get()->ReadOptions.Fallback;

        LOG_S_DEBUG("Read request: " << blobRange << " cache: " << (ui32)promote << " fallback: " << (ui32)fallback);

        TReadItem readItem(ev->Get()->ReadOptions, blobRange);
        HandleSingleRangeRead(std::move(readItem), ev->Sender, ctx);

        MakeReadRequests(ctx);
    }

    void HandleSingleRangeRead(TReadItem&& readItem, const TActorId& sender, const TActorContext& ctx) {
        const TBlobRange& blobRange = readItem.BlobRange;

        // Is in cache?
        auto it = readItem.PromoteInCache() ? Cache.Find(blobRange) : Cache.FindWithoutPromote(blobRange);
        if (it != Cache.End()) {
            Hits->Inc();
            HitsBytes->Add(blobRange.Size);
            return SendResult(sender, blobRange, NKikimrProto::OK, it.Value(), ctx);
        }

        Misses->Inc();

        // Disable promoting reads for external blobs if MaxCacheExternalDataSize is zero. But keep promoting hits.
        // For now MaxCacheExternalDataSize is just a disabled/enabled flag. TODO: real MaxCacheExternalDataSize
        if (readItem.Fallback && MaxCacheExternalDataSize == 0) {
            readItem.CacheAfterRead = false;
        }

        // Is outstanding?
        auto readIt = OutstandingReads.find(blobRange);
        if (readIt != OutstandingReads.end()) {
            readIt->second.Waiting.push_back(sender);
            readIt->second.Cache |= readItem.PromoteInCache();
            return;
        }

        EnqueueRead(std::move(readItem), sender);
    }

    void Handle(TEvBlobCache::TEvReadBlobRangeBatch::TPtr& ev, const TActorContext& ctx) {
        const auto& ranges = ev->Get()->BlobRanges;
        LOG_S_DEBUG("Batch read request: " << JoinStrings(ranges.begin(), ranges.end(), " "));

        for (const auto& blobRange : ranges) {
            TReadItem readItem(ev->Get()->ReadOptions, blobRange);
            HandleSingleRangeRead(std::move(readItem), ev->Sender, ctx);
        }

        MakeReadRequests(ctx);
    }

    void Handle(TEvBlobCache::TEvCacheBlobRange::TPtr& ev, const TActorContext& ctx) {
        const auto& blobRange = ev->Get()->BlobRange;
        const auto& data = ev->Get()->Data;

        if (blobRange.Size != data.size()) {
            LOG_S_ERROR("Trying to add invalid data for range: " << blobRange << " size: " << data.size());
            return;
        }

        Adds->Inc();

        if (OutstandingReads.count(blobRange)) {
            // Don't bother if there is already a read request for this range
            return;
        }

        LOG_S_DEBUG("Adding range: " << blobRange);

        AddBytes->Add(blobRange.Size);

        InsertIntoCache(blobRange, data);

        Evict(ctx);
    }

    void Handle(TEvBlobCache::TEvForgetBlob::TPtr& ev, const TActorContext&) {
        const TUnifiedBlobId& blobId = ev->Get()->BlobId;

        LOG_S_DEBUG("Forgetting blob: " << blobId);

        Forgets->Inc();

        auto blobIdIt = CachedRanges.find(blobId);
        if (blobIdIt == CachedRanges.end()) {
            return;
        }

        // Remove all ranges of this blob that are present in cache
        for (const auto& blobRange: blobIdIt->second) {
            auto rangeIt = Cache.FindWithoutPromote(blobRange);
            if (rangeIt == Cache.End()) {
                continue;
            }

            Cache.Erase(rangeIt);
            CacheDataSize -= blobRange.Size;
            SizeBytes->Sub(blobRange.Size);
            SizeBlobs->Dec();
            ForgetBytes->Add(blobRange.Size);
        }

        CachedRanges.erase(blobIdIt);
    }

    void EnqueueRead(TReadItem&& readItem, const TActorId& sender) {
        const auto& blobRange = readItem.BlobRange;
        TReadInfo& blobInfo = OutstandingReads[blobRange];
        blobInfo.Waiting.push_back(sender);
        blobInfo.Cache = readItem.PromoteInCache();

        LOG_S_DEBUG("Enqueue read range: " << blobRange);

        ReadQueue.emplace_back(std::move(readItem));
        ReadsInQueue->Set(ReadQueue.size());
    }

    void SendBatchReadRequest(const std::vector<TBlobRange>& blobRanges,
        TReadItem::EReadVariant readVariant, const ui64 cookie, const TActorContext& ctx)
    {
        Y_VERIFY(!blobRanges.empty());

        if (blobRanges.front().BlobId.IsSmallBlob()) {
            SendBatchReadRequestToTablet(blobRanges, cookie, ctx);
        } else {
            SendBatchReadRequestToDS(blobRanges, readVariant, cookie, ctx);
        }
    }

    void SendBatchReadRequestToDS(const std::vector<TBlobRange>& blobRanges,
        TReadItem::EReadVariant readVariant, const ui64 cookie, const TActorContext& ctx)
    {
        const ui32 dsGroup = blobRanges.front().BlobId.GetDsGroup();

        LOG_S_DEBUG("Sending read from DS: group: " << dsGroup
            << " ranges: " << JoinStrings(blobRanges.begin(), blobRanges.end(), " ")
            << " cookie: " << cookie);

        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queires(new TEvBlobStorage::TEvGet::TQuery[blobRanges.size()]);
        for (size_t i = 0; i < blobRanges.size(); ++i) {
            Y_VERIFY(dsGroup == blobRanges[i].BlobId.GetDsGroup());
            queires[i].Set(blobRanges[i].BlobId.GetLogoBlobId(), blobRanges[i].Offset, blobRanges[i].Size);
        }

        NKikimrBlobStorage::EGetHandleClass readClass = TReadItem::ReadClass(readVariant);
        TInstant deadline = ReadDeadline(readVariant);
        SendToBSProxy(ctx,
                dsGroup,
                new TEvBlobStorage::TEvGet(queires, blobRanges.size(), deadline, readClass, false),
                cookie);

        ReadRequests->Inc();
    }

    static TInstant ReadDeadline(TReadItem::EReadVariant variant) {
        if (variant == TReadItem::EReadVariant::FAST) {
            return TAppData::TimeProvider->Now() + FAST_READ_DEADLINE;
        } else if (variant == TReadItem::EReadVariant::DEFAULT) {
            return TAppData::TimeProvider->Now() + DEFAULT_READ_DEADLINE;
        }
        return TInstant::Max(); // EReadVariant::DEFAULT_NO_DEADLINE
    }

    void MakeReadRequests(const TActorContext& ctx) {
        THashMap<std::tuple<ui64, ui32, TReadItem::EReadVariant>, std::vector<TBlobRange>> groupedBlobRanges;
        THashMap<TUnifiedBlobId, std::vector<TBlobRange>> fallbackRanges;

        while (!ReadQueue.empty()) {
            const auto& readItem = ReadQueue.front();
            const TBlobRange& blobRange = readItem.BlobRange;

            if (readItem.Fallback) {
                // For now it's always possible to add external read cause we must not block ReadQueue by such reads
                // TODO: separate ReadQueue and InFlightDataSize for external reads
                fallbackRanges[blobRange.BlobId].push_back(blobRange);
            } else {
                // NOTE: if queue is not empty, at least 1 in-flight request is allowed
                if (InFlightDataSize && InFlightDataSize >= MaxInFlightDataSize) {
                    break;
                }
                InFlightDataSize += blobRange.Size;

                auto blobSrc = readItem.BlobSource();
                groupedBlobRanges[blobSrc].push_back(blobRange);
            }

            SizeBytesInFlight->Add(blobRange.Size);
            SizeBlobsInFlight->Inc();

            ReadQueue.pop_front();
        }

        ReadsInQueue->Set(ReadQueue.size());

        // We might need to free some space to accomodate the results of new reads
        Evict(ctx);

        ui64 cookie = ++ReadCookie;

        for (auto& [target, rangesGroup] : groupedBlobRanges) {
            ui64 requestSize = 0;
            TReadItem::EReadVariant readVariant = std::get<2>(target);

            for (auto& blobRange : rangesGroup) {
                if (requestSize && (requestSize + blobRange.Size > MAX_REQUEST_BYTES)) {
                    SendBatchReadRequest(CookieToRange[cookie], readVariant, cookie, ctx);
                    cookie = ++ReadCookie;
                    requestSize = 0;
                }

                requestSize += blobRange.Size;
                CookieToRange[cookie].emplace_back(std::move(blobRange));
            }
            if (requestSize) {
                SendBatchReadRequest(CookieToRange[cookie], readVariant, cookie, ctx);
                cookie = ++ReadCookie;
                requestSize = 0;
            }
        }

        for (auto& [blobId, ranges] : fallbackRanges) {
            Y_VERIFY(blobId.IsDsBlob());
            Y_VERIFY(!ranges.empty());

            cookie = ++ReadCookie;
            CookieToRange[cookie] = std::move(ranges);
            SendBatchReadRequestToTablet(CookieToRange[cookie], cookie, ctx);
        }
    }

    void SendResult(const TActorId& to, const TBlobRange& blobRange, NKikimrProto::EReplyStatus status,
                    const TString& data, const TActorContext& ctx) {
        LOG_S_DEBUG("Send result: " << blobRange << " to: " << to << " status: " << status);

        ctx.Send(to, new TEvBlobCache::TEvReadBlobRangeResult(blobRange, status, data));
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr& ev, const TActorContext& ctx) {
        const ui64 readCookie = ev->Cookie;

        if (ev->Get()->ResponseSz < 1) {
            Y_FAIL("Unexpected reply from blobstorage");
        }

        if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
            LOG_S_WARN("Read failed: " << ev->Get()->ToString());
        }

        auto cookieIt = CookieToRange.find(readCookie);
        if (cookieIt == CookieToRange.end()) {
            // This shouldn't happen
            LOG_S_CRIT("Unknown read result cookie: " << readCookie);
            return;
        }

        std::vector<TBlobRange> blobRanges = std::move(cookieIt->second);
        CookieToRange.erase(readCookie);

        Y_VERIFY(blobRanges.size() == ev->Get()->ResponseSz, "Mismatched number of results for read request!");

        for (size_t i = 0; i < ev->Get()->ResponseSz; ++i) {
            const auto& res = ev->Get()->Responses[i];
            ProcessSingleRangeResult(blobRanges[i], readCookie, res.Status, res.Buffer, ctx);
        }

        MakeReadRequests(ctx);
    }

    void ProcessSingleRangeResult(const TBlobRange& blobRange, const ui64 readCookie,
        ui32 status, const TString& data, const TActorContext& ctx)
    {
        auto readIt = OutstandingReads.find(blobRange);
        if (readIt == OutstandingReads.end()) {
            // This shouldn't happen
            LOG_S_CRIT("Unknown read result key: " << blobRange << " cookie: " << readCookie);
            return;
        }

        SizeBytesInFlight->Sub(blobRange.Size);
        SizeBlobsInFlight->Dec();
        InFlightDataSize -= blobRange.Size;

        Y_VERIFY(Cache.Find(blobRange) == Cache.End(),
            "Range %s must not be already in cache", blobRange.ToString().c_str());

        if (status == NKikimrProto::EReplyStatus::OK) {
            Y_VERIFY(blobRange.Size == data.size(),
                "Read %s, size %" PRISZT, blobRange.ToString().c_str(), data.size());
            ReadBytes->Add(blobRange.Size);

            if (readIt->second.Cache) {
                InsertIntoCache(blobRange, data);
            }
        } else {
            LOG_S_WARN("Read failed for range: " << blobRange
                << " status: " << NKikimrProto::EReplyStatus_Name(status));
        }

        // Send results to all waiters
        for (const auto& to : readIt->second.Waiting) {
            SendResult(to, blobRange, (NKikimrProto::EReplyStatus)status, data, ctx);
        }

        OutstandingReads.erase(readIt);
    }

    void SendBatchReadRequestToTablet(const std::vector<TBlobRange>& blobRanges,
        const ui64 cookie, const TActorContext& ctx)
    {
        Y_VERIFY(!blobRanges.empty());
        ui64 tabletId = blobRanges.front().BlobId.GetTabletId();

        LOG_S_DEBUG("Sending read from Tablet: " << tabletId
            << " ranges: " << JoinStrings(blobRanges.begin(), blobRanges.end(), " ")
            << " cookie: " << cookie);

        if (!ShardPipes.contains(tabletId)) {
            NTabletPipe::TClientConfig clientConfig;
            clientConfig.AllowFollower = false;
            clientConfig.CheckAliveness = true;
            clientConfig.RetryPolicy = {
                .RetryLimitCount = 10,
                .MinRetryTime = TDuration::MilliSeconds(5),
            };
            ShardPipes[tabletId] = ctx.Register(NTabletPipe::CreateClient(ctx.SelfID, tabletId, clientConfig));
        }

        auto ev = std::make_unique<TEvColumnShard::TEvReadBlobRanges>(blobRanges);

        InFlightTabletRequests[tabletId].insert(cookie);
        NTabletPipe::SendData(ctx, ShardPipes[tabletId], ev.release(), cookie);
    }

    // Frogets the pipe to the tablet and fails all in-flight requests to it
    void DestroyPipe(ui64 tabletId, const TActorContext& ctx) {
        ShardPipes.erase(tabletId);
        // Send errors for in-flight requests
        auto cookies = std::move(InFlightTabletRequests[tabletId]);
        InFlightTabletRequests.erase(tabletId);
        for (ui64 readCookie : cookies) {
            auto cookieIt = CookieToRange.find(readCookie);
            if (cookieIt == CookieToRange.end()) {
                // This might only happen in case fo race between response and pipe close
                LOG_S_INFO("Unknown read result cookie: " << readCookie);
                return;
            }

            std::vector<TBlobRange> blobRanges = std::move(cookieIt->second);
            CookieToRange.erase(readCookie);

            for (size_t i = 0; i < blobRanges.size(); ++i) {
                Y_VERIFY(blobRanges[i].BlobId.GetTabletId() == tabletId);
                ProcessSingleRangeResult(blobRanges[i], readCookie, NKikimrProto::EReplyStatus::NOTREADY, {}, ctx);
            }
        }

        MakeReadRequests(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected* msg = ev->Get();
        const ui64 tabletId = msg->TabletId;
        Y_VERIFY(tabletId != 0);
        if (msg->Status == NKikimrProto::OK) {
            LOG_S_DEBUG("Pipe connected to tablet: " << tabletId);
        } else {
            LOG_S_DEBUG("Pipe connection to tablet: " << tabletId << " failed with status: " << msg->Status);
            DestroyPipe(tabletId, ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        const ui64 tabletId = ev->Get()->TabletId;
        Y_VERIFY(tabletId != 0);

        LOG_S_DEBUG("Closed pipe connection to tablet: " << tabletId);
        DestroyPipe(tabletId, ctx);
    }

    void Handle(TEvColumnShard::TEvReadBlobRangesResult::TPtr& ev, const TActorContext& ctx) {
        ui64 tabletId = ev->Get()->Record.GetTabletId();
        ui64 readCookie = ev->Cookie;
        LOG_S_DEBUG("Got read result from tablet: " << tabletId);

        InFlightTabletRequests[tabletId].erase(readCookie);

        auto cookieIt = CookieToRange.find(readCookie);
        if (cookieIt == CookieToRange.end()) {
            // This might only happen in case fo race between response and pipe close
            LOG_S_INFO("Unknown read result cookie: " << readCookie);
            return;
        }

        std::vector<TBlobRange> blobRanges = std::move(cookieIt->second);
        CookieToRange.erase(readCookie);

        const auto& record = ev->Get()->Record;

        Y_VERIFY(blobRanges.size() == record.ResultsSize(), "Mismatched number of results for read request!");

        for (size_t i = 0; i < record.ResultsSize(); ++i) {
            const auto& res = record.GetResults(i);

            Y_VERIFY(blobRanges[i].BlobId.ToStringNew() == res.GetBlobRange().GetBlobId());
            Y_VERIFY(blobRanges[i].Offset == res.GetBlobRange().GetOffset());
            Y_VERIFY(blobRanges[i].Size == res.GetBlobRange().GetSize());
            ProcessSingleRangeResult(blobRanges[i], readCookie, res.GetStatus(), res.GetData(), ctx);
        }

        MakeReadRequests(ctx);
    }

    void InsertIntoCache(const TBlobRange& blobRange, TString data) {
        CacheDataSize += blobRange.Size;
        SizeBytes->Add(blobRange.Size);
        SizeBlobs->Inc();

        // Shrink the buffer if it has to much extra capacity
        if (data.capacity() > data.size() * 1.1) {
            data = TString(data.begin(), data.end());
        }

        Cache.Insert(blobRange, data);
        CachedRanges[blobRange.BlobId].insert(blobRange);
    }

    void Evict(const TActorContext&) {
        while (CacheDataSize + InFlightDataSize > MaxCacheDataSize) {
            auto it = Cache.FindOldest();
            if (it == Cache.End()) {
                break;
            }

            LOG_S_DEBUG("Evict: " << it.Key() << ";CacheDataSize:" << CacheDataSize << ";InFlightDataSize:" << (i64)InFlightDataSize << ";MaxCacheDataSize:" << (i64)MaxCacheDataSize);

            {
                // Remove the range from list of ranges by blob id
                auto blobIdIt = CachedRanges.find(it.Key().BlobId);
                if (blobIdIt != CachedRanges.end()) {
                    blobIdIt->second.erase(it.Key());
                    if (blobIdIt->second.empty()) {
                        CachedRanges.erase(blobIdIt);
                    }
                }
            }

            Evictions->Inc();
            EvictedBytes->Add(it.Key().Size);

            CacheDataSize -= it.Key().Size;
            Cache.Erase(it);

            SizeBytes->Set(CacheDataSize);
            SizeBlobs->Set(Cache.Size());
        }
    }
};

NActors::IActor* CreateBlobCache(ui64 maxBytes, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TBlobCache(maxBytes, counters);
}

void AddRangeToCache(const TBlobRange& blobRange, const TString& data) {
    TlsActivationContext->Send(
        new IEventHandleFat(MakeBlobCacheServiceId(), TActorId(), new TEvBlobCache::TEvCacheBlobRange(blobRange, data)));
}

void ForgetBlob(const TUnifiedBlobId& blobId) {
    TlsActivationContext->Send(
        new IEventHandleFat(MakeBlobCacheServiceId(), TActorId(), new TEvBlobCache::TEvForgetBlob(blobId)));
}

}
