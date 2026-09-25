#include "blob_cache.h"
#include "columnshard.h"

#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/memory_controller_iface.h>
#include <ydb/core/base/tablet_pipe.h>
#include <ydb/core/protos/config.pb.h>

#include <ydb/library/actors/core/actor.h>
#include <ydb/library/actors/core/hfunc.h>

#include <library/cpp/cache/cache.h>
#include <util/generic/set.h>
#include <util/generic/vector.h>
#include <util/string/vector.h>

#include <optional>
#include <tuple>

#define YDB_LOG_THIS_FILE_COMPONENT NKikimrServices::BLOB_CACHE

namespace NKikimr::NBlobCache {
namespace {

using namespace NActors;

class TBlobCache: public TActorBootstrapped<TBlobCache> {
private:
    struct TReadInfo {
        /// List of readers.
        TList<TActorId> Waiting;
        /// Put in cache after read.
        bool Cache{ false };
    };

    struct TReadItem: public TReadBlobRangeOptions {
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
            Y_ABORT_UNLESS(blobRange.BlobId.IsValid());
        }

        bool PromoteInCache() const {
            return CacheAfterRead;
        }

        static NKikimrBlobStorage::EGetHandleClass ReadClass(EReadVariant readVar) {
            return (readVar == EReadVariant::FAST) ? NKikimrBlobStorage::FastRead : NKikimrBlobStorage::AsyncRead;
        }

        EReadVariant ReadVariant() const {
            return IsBackgroud ? (WithDeadline ? EReadVariant::DEFAULT : EReadVariant::DEFAULT_NO_DEADLINE) : EReadVariant::FAST;
        }

        // Blobs with same tagret can be read in a single request
        // (e.g. DS blobs from the same tablet residing on the same DS group, or 2 small blobs from the same tablet)
        std::tuple<ui64, ui32, EReadVariant> BlobSource() const {
            const TUnifiedBlobId& blobId = BlobRange.BlobId;
            Y_ABORT_UNLESS(blobId.IsValid());
            return { blobId.GetTabletId(), blobId.GetDsGroup(), ReadVariant() };
        }
    };

    /// Hash TBlobRange by BlobId only.
    struct BlobRangeHash {
        size_t operator()(const TBlobRange& range) const {
            return range.BlobId.Hash();
        }

        size_t operator()(const TUnifiedBlobId& id) const {
            return id.Hash();
        }
    };

    /// Compares TBlobRange by BlobId only.
    struct BlobRangeEqual {
        bool operator()(const TBlobRange& a, const TBlobRange& b) const {
            return a.BlobId == b.BlobId;
        }

        bool operator()(const TBlobRange& a, const TUnifiedBlobId& id) const {
            return a.BlobId == id;
        }
    };

    struct TCacheEntry {
        TString Data;
        TInstant StickyUntil;
    };

    TLRUCache<TBlobRange, TCacheEntry> Cache;
    TLRUCache<TBlobRange, char> Evictable;
    TSet<std::pair<TInstant, TBlobRange>> StickyExpiry;
    /// List of cached ranges by blob id.
    /// It is used to remove all blob ranges from cache when
    /// it gets a notification that a blob has been deleted.
    THashMultiSet<TBlobRange, BlobRangeHash, BlobRangeEqual> CachedRanges;

    TControlWrapper MaxCacheDataSize;
    const bool UseMaxCacheDataSizeFromConfig;
    TControlWrapper MaxInFlightDataSize;
    ui64 WriteProtectDurationMs;
    ui64 MaxRequestBytes;
    TDuration ReadDeadlineDuration;
    i64 CacheDataSize;   // Current size of all blobs in cache
    i64 StickyDataSize;
    ui64 ReadCookie;
    THashMap<ui64, std::vector<TBlobRange>> CookieToRange;   // All in-flight requests
    THashMap<TBlobRange, TReadInfo> OutstandingReads;   // All in-flight and enqueued reads
    TDeque<TReadItem> ReadQueue;   // Reads that are waiting to be sent
        // TODO: Consider making per-group queues
    i64 InFlightDataSize;   // Current size of all in-flight blobs

    THashMap<ui64, TActorId> ShardPipes;   // TabletId -> PipeClient for small blob read requests
    THashMap<ui64, THashSet<ui64>> InFlightTabletRequests;   // TabletId -> list to read cookies

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
    const TCounterPtr ReadRangeFailedBytes;
    const TCounterPtr ReadRangeFailedCount;
    const TCounterPtr ReadSimpleFailedBytes;
    const TCounterPtr ReadSimpleFailedCount;
    const TCounterPtr AddBytes;
    const TCounterPtr ForgetBytes;
    const TCounterPtr SizeBytesInFlight;
    const TCounterPtr SizeBlobsInFlight;
    const TCounterPtr ReadRequests;
    const TCounterPtr ReadsInQueue;
    const TCounterPtr MaxSizeBytes;
    const TCounterPtr StickyBytes;
    const TCounterPtr StickyBlobs;
    const TCounterPtr StickyHits;
    const TCounterPtr StickyHitsBytes;
    const TCounterPtr StickyEvictions;
    const TCounterPtr StickyEvictedBytes;

    TIntrusivePtr<NMemory::IMemoryConsumer> MemoryConsumer;

public:
    static constexpr auto ActorActivityType() {
        return NKikimrServices::TActivity::BLOB_CACHE_ACTOR;
    }

public:
    explicit TBlobCache(const TBlobCacheSettings& settings, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters)
        : TActorBootstrapped<TBlobCache>()
        , Cache(SIZE_MAX)
        , Evictable(SIZE_MAX)
        , MaxCacheDataSize(settings.MaxCacheDataSize, 0, 1ull << 40)
        , UseMaxCacheDataSizeFromConfig(settings.MaxCacheDataSizeFromConfig)
        , MaxInFlightDataSize(settings.MaxInFlightBytes, 0, 10ull << 30)
        , WriteProtectDurationMs(settings.WriteProtectDurationMs)
        , MaxRequestBytes(settings.MaxRequestBytes)
        , ReadDeadlineDuration(TDuration::MilliSeconds(settings.ReadDeadlineMs))
        , CacheDataSize(0)
        , StickyDataSize(0)
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
        , ReadRangeFailedBytes(counters->GetCounter("ReadRangeFailedBytes", true))
        , ReadRangeFailedCount(counters->GetCounter("ReadRangeFailedCount", true))
        , ReadSimpleFailedBytes(counters->GetCounter("ReadSimpleFailedBytes", true))
        , ReadSimpleFailedCount(counters->GetCounter("ReadSimpleFailedCount", true))
        , AddBytes(counters->GetCounter("AddBytes", true))
        , ForgetBytes(counters->GetCounter("ForgetBytes", true))
        , SizeBytesInFlight(counters->GetCounter("SizeBytesInFlight"))
        , SizeBlobsInFlight(counters->GetCounter("SizeBlobsInFlight"))
        , ReadRequests(counters->GetCounter("ReadRequests", true))
        , ReadsInQueue(counters->GetCounter("ReadsInQueue"))
        , MaxSizeBytes(counters->GetCounter("MaxSizeBytes"))
        , StickyBytes(counters->GetCounter("StickyBytes"))
        , StickyBlobs(counters->GetCounter("StickyBlobs"))
        , StickyHits(counters->GetCounter("StickyHits", true))
        , StickyHitsBytes(counters->GetCounter("StickyHitsBytes", true))
        , StickyEvictions(counters->GetCounter("StickyEvictions", true))
        , StickyEvictedBytes(counters->GetCounter("StickyEvictedBytes", true))
    {
    }

    void Bootstrap(const TActorContext& ctx) {
        auto& icb = AppData(ctx)->Icb;
        TControlBoard::RegisterSharedControl(MaxCacheDataSize, icb->BlobCache.MaxCacheDataSize);
        TControlBoard::RegisterSharedControl(MaxInFlightDataSize, icb->BlobCache.MaxInFlightDataSize);

        LOG_S_NOTICE("MaxCacheDataSize: " << (i64)MaxCacheDataSize << " InFlightDataSize: " << (i64)InFlightDataSize);

        MaxSizeBytes->Set((i64)MaxCacheDataSize);

        Send(NMemory::MakeMemoryControllerId(), new NMemory::TEvConsumerRegister(NMemory::EMemoryConsumerKind::ColumnTablesBlobCache));

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
            HFunc(NMemory::TEvConsumerRegistered, Handle);
            HFunc(NMemory::TEvConsumerLimit, Handle);
            default:
                LOG_S_WARN("Unhandled event type: " << ev->GetTypeRewrite() << " event: " << ev->ToString());
                Send(IEventHandle::ForwardOnNondelivery(std::move(ev), TEvents::TEvUndelivered::ReasonActorUnknown));
                break;
        };
    }

    void ScheduleWakeup() {
        Schedule(TDuration::MilliSeconds(100), new TEvents::TEvWakeup());
    }

    void Handle(TEvents::TEvWakeup::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Evict(ctx);   // Max cache size might have changed
        ScheduleWakeup();
    }

    void Handle(TEvents::TEvPoisonPill::TPtr& ev, const TActorContext& ctx) {
        Y_UNUSED(ev);
        Die(ctx);
    }

    void Handle(TEvBlobCache::TEvReadBlobRange::TPtr& ev, const TActorContext& ctx) {
        const TBlobRange& blobRange = ev->Get()->BlobRange;
        const bool promote = (i64)MaxCacheDataSize && ev->Get()->ReadOptions.CacheAfterRead;

        LOG_S_DEBUG("Read request: " << blobRange << " cache: " << (ui32)promote << " sender:" << ev->Sender);

        if (!HandleSingleRangeRead(TReadItem(ev->Get()->ReadOptions, blobRange), ev->Sender, ctx)) {
            MakeReadRequests(ctx);
        }
    }

    bool HandleSingleRangeRead(TReadItem readItem, const TActorId& sender, const TActorContext& ctx) {
        const TBlobRange& blobRange = readItem.BlobRange;
        YDB_LOG_DEBUG("",
            {"ask", blobRange});

        if (SendCached(sender, blobRange, readItem.PromoteInCache(), ctx)) {
            return true;
        }

        LOG_S_DEBUG("Miss cache: " << blobRange << " sender:" << sender);
        Misses->Inc();

        // Update set of outstanding requests.
        TReadInfo& blobInfo = OutstandingReads[blobRange];
        const bool inserted = blobInfo.Waiting.empty();

        blobInfo.Waiting.push_back(sender);
        blobInfo.Cache |= readItem.PromoteInCache();

        if (inserted) {
            LOG_S_DEBUG("Enqueue read range: " << blobRange);

            ReadQueue.emplace_back(std::move(readItem));
            ReadsInQueue->Set(ReadQueue.size());
            // The requested range just put into a read queue.
            // Extra work should be done to process the queue.
            return false;
        } else {
            // The requested range was already scheduled for read.
            return true;
        }
    }

    bool SendCached(const TActorId& sender, const TBlobRange& blobRange, const bool promote, const TActorContext& ctx) {
        auto exactIt = promote ? Cache.Find(blobRange) : Cache.FindWithoutPromote(blobRange);
        if (exactIt != Cache.End()) {
            SendCachedHit(sender, blobRange, exactIt.Value(), blobRange, promote, ctx);
            return true;
        }

        const auto covering = FindCoveringRange(blobRange);
        if (!covering) {
            return false;
        }
        auto coveringIt = promote ? Cache.Find(*covering) : Cache.FindWithoutPromote(*covering);
        if (coveringIt == Cache.End()) {
            return false;
        }
        SendCachedHit(sender, blobRange, coveringIt.Value(), *covering, promote, ctx);
        return true;
    }

    void SendCachedHit(const TActorId& sender, const TBlobRange& requested, const TCacheEntry& entry, const TBlobRange& covering,
        const bool promote, const TActorContext& ctx) {
        Y_ABORT_UNLESS(entry.Data.size() == covering.Size, "Cached %s, size %" PRISZT, covering.ToString().c_str(), entry.Data.size());
        const TString data = entry.Data.substr(requested.Offset - covering.Offset, requested.Size);
        Hits->Inc();
        HitsBytes->Add(requested.Size);
        if (entry.StickyUntil) {
            StickyHits->Inc();
            StickyHitsBytes->Add(requested.Size);
        } else if (promote) {
            Evictable.Find(covering);
        }
        SendResult(sender, requested, NKikimrProto::OK, data, {}, ctx, true);
    }

    std::optional<TBlobRange> FindCoveringRange(const TBlobRange& blobRange) const {
        const auto [begin, end] = CachedRanges.equal_range(blobRange);
        for (auto it = begin; it != end; ++it) {
            if (it->Offset <= blobRange.Offset &&
                static_cast<ui64>(it->Offset) + it->Size >= static_cast<ui64>(blobRange.Offset) + blobRange.Size) {
                return *it;
            }
        }
        return std::nullopt;
    }

    void Handle(TEvBlobCache::TEvReadBlobRangeBatch::TPtr& ev, const TActorContext& ctx) {
        const auto& ranges = ev->Get()->BlobRanges;
        LOG_S_DEBUG("Batch read request: " << JoinStrings(ranges.begin(), ranges.end(), " "));

        auto& readOptions = ev->Get()->ReadOptions;
        readOptions.CacheAfterRead = (i64)MaxCacheDataSize && readOptions.CacheAfterRead;

        for (const auto& blobRange : ranges) {
            HandleSingleRangeRead(TReadItem(readOptions, blobRange), ev->Sender, ctx);
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

        if (OutstandingReads.contains(blobRange)) {
            // Don't bother if there is already a read request for this range
            return;
        }

        LOG_S_DEBUG("Adding range: " << blobRange);

        AddBytes->Add(blobRange.Size);

        InsertIntoCache(blobRange, data, ev->Get()->Sticky);

        Evict(ctx);
    }

    void Handle(TEvBlobCache::TEvForgetBlob::TPtr& ev, const TActorContext&) {
        const TUnifiedBlobId& blobId = ev->Get()->BlobId;

        LOG_S_INFO("Forgetting blob: " << blobId);

        Forgets->Inc();

        const auto [begin, end] = CachedRanges.equal_range(blobId);
        if (begin == end) {
            return;
        }

        TVector<TBlobRange> ranges(begin, end);
        for (const auto& range : ranges) {
            ForgetBytes->Add(range.Size);
            EraseCachedRange(range);
        }

        UpdateConsumption();
    }

    void Handle(NMemory::TEvConsumerRegistered::TPtr& ev, const TActorContext&) {
        MemoryConsumer = std::move(ev->Get()->Consumer);
    }

    void Handle(NMemory::TEvConsumerLimit::TPtr& ev, const TActorContext&) {
        if (UseMaxCacheDataSizeFromConfig) {
            return;
        }

        const i64 newMaxCacheDataSize = ev->Get()->LimitBytes;
        if (newMaxCacheDataSize == (i64)MaxCacheDataSize) {
            return;
        }

        LOG_S_DEBUG("Updating max cache data size: " << newMaxCacheDataSize);

        MaxCacheDataSize = newMaxCacheDataSize;

        MaxSizeBytes->Set((i64)MaxCacheDataSize);
    }

    void UpdateConsumption() {
        if (!MemoryConsumer) {
            return;
        }

        MemoryConsumer->SetConsumption(CacheDataSize);
    }

    void SendBatchReadRequestToDS(const std::vector<TBlobRange>& blobRanges, const ui64 cookie, ui32 dsGroup,
        TReadItem::EReadVariant readVariant, const TActorContext& ctx) {
        LOG_S_DEBUG("Sending read from BlobCache: group: " << dsGroup << " ranges: " << JoinStrings(blobRanges.begin(), blobRanges.end(), " ")
                                                           << " cookie: " << cookie);

        TArrayHolder<TEvBlobStorage::TEvGet::TQuery> queires(new TEvBlobStorage::TEvGet::TQuery[blobRanges.size()]);
        for (size_t i = 0; i < blobRanges.size(); ++i) {
            Y_ABORT_UNLESS(dsGroup == blobRanges[i].BlobId.GetDsGroup());
            queires[i].Set(blobRanges[i].BlobId.GetLogoBlobId(), blobRanges[i].Offset, blobRanges[i].Size);
        }

        NKikimrBlobStorage::EGetHandleClass readClass = TReadItem::ReadClass(readVariant);
        TInstant deadline = ReadDeadline(readVariant);
        SendToBSProxy(ctx, dsGroup, new TEvBlobStorage::TEvGet(queires, blobRanges.size(), deadline, readClass, false), cookie);

        ReadRequests->Inc();
    }

    static TInstant ReadDeadlineForVariant(TReadItem::EReadVariant variant, const TDuration readDeadline) {
        if (variant == TReadItem::EReadVariant::DEFAULT) {
            return TAppData::TimeProvider->Now() + readDeadline;
        }
        // We want to wait for data anyway in this case. This behaviour is similar to datashard
        return TInstant::Max();   // EReadVariant::DEFAULT_NO_DEADLINE || EReadVariant::FAST
    }

    TInstant ReadDeadline(TReadItem::EReadVariant variant) const {
        return ReadDeadlineForVariant(variant, ReadDeadlineDuration);
    }

    void MakeReadRequests(const TActorContext& ctx) {
        THashMap<std::tuple<ui64, ui32, TReadItem::EReadVariant>, std::vector<TBlobRange>> groupedBlobRanges;

        while (!ReadQueue.empty()) {
            const auto& readItem = ReadQueue.front();
            const TBlobRange& blobRange = readItem.BlobRange;

            // NOTE: if queue is not empty, at least 1 in-flight request is allowed
            if (InFlightDataSize && InFlightDataSize >= MaxInFlightDataSize) {
                break;
            }
            InFlightDataSize += blobRange.Size;
            SizeBytesInFlight->Add(blobRange.Size);
            SizeBlobsInFlight->Inc();

            auto blobSrc = readItem.BlobSource();
            groupedBlobRanges[blobSrc].push_back(blobRange);

            ReadQueue.pop_front();
        }

        ReadsInQueue->Set(ReadQueue.size());

        // We might need to free some space to accommodate the results of new reads
        Evict(ctx);

        ui64 cookie = ++ReadCookie;

        // TODO: fix small blobs mix with dsGroup == 0 (it could be zero in tests)
        for (auto& [target, rangesGroup] : groupedBlobRanges) {
            ui64 requestSize = 0;
            ui32 dsGroup = std::get<1>(target);
            TReadItem::EReadVariant readVariant = std::get<2>(target);

            std::vector<ui64> dsReads;

            for (auto& blobRange : rangesGroup) {
                if (requestSize && (requestSize + blobRange.Size > MaxRequestBytes)) {
                    dsReads.push_back(cookie);
                    cookie = ++ReadCookie;
                    requestSize = 0;
                }

                requestSize += blobRange.Size;
                CookieToRange[cookie].emplace_back(std::move(blobRange));
            }
            if (requestSize) {
                dsReads.push_back(cookie);
                cookie = ++ReadCookie;
                requestSize = 0;
            }

            for (ui64 cookie : dsReads) {
                SendBatchReadRequestToDS(CookieToRange[cookie], cookie, dsGroup, readVariant, ctx);
            }
        }
    }

    void SendResult(const TActorId& to, const TBlobRange& blobRange, NKikimrProto::EReplyStatus status, const TString& data,
        const TString& detailedError, const TActorContext& ctx, const bool fromCache = false) {
        LOG_S_DEBUG("Send result: " << blobRange << " to: " << to << " status: " << status);

        ctx.Send(to, new TEvBlobCache::TEvReadBlobRangeResult(blobRange, status, data, detailedError, fromCache));
    }

    void Handle(TEvBlobStorage::TEvGetResult::TPtr& ev, const TActorContext& ctx) {
        const ui64 readCookie = ev->Cookie;

        if (ev->Get()->ResponseSz < 1) {
            Y_ABORT("Unexpected reply from blobstorage");
        }

        TString detailedError;
        if (ev->Get()->Status != NKikimrProto::EReplyStatus::OK) {
            detailedError = ev->Get()->ToString();
            YDB_LOG_WARN("",
                {"fail", ev->Get()->ToString()});
            ReadSimpleFailedBytes->Add(ev->Get()->ResponseSz);
            ReadSimpleFailedCount->Add(1);
        } else {
            YDB_LOG_DEBUG("",
                {"success", ev->Get()->ToString()});
        }

        auto cookieIt = CookieToRange.find(readCookie);
        if (cookieIt == CookieToRange.end()) {
            // This shouldn't happen
            LOG_S_CRIT("Unknown read result cookie: " << readCookie);
            return;
        }

        std::vector<TBlobRange> blobRanges = std::move(cookieIt->second);
        CookieToRange.erase(readCookie);

        Y_ABORT_UNLESS(blobRanges.size() == ev->Get()->ResponseSz, "Mismatched number of results for read request!");

        for (size_t i = 0; i < ev->Get()->ResponseSz; ++i) {
            const auto& res = ev->Get()->Responses[i];
            ProcessSingleRangeResult(blobRanges[i], readCookie, res.Status, res.Buffer.ConvertToString(), detailedError, ctx);
        }

        MakeReadRequests(ctx);
    }

    void ProcessSingleRangeResult(const TBlobRange& blobRange, const ui64 readCookie, ui32 status, const TString& data,
        const TString& detailedError, const TActorContext& ctx) noexcept {
        YDB_LOG_DEBUG("",
            {"processSingleRangeResult", blobRange});
        auto readIt = OutstandingReads.find(blobRange);
        if (readIt == OutstandingReads.end()) {
            // This shouldn't happen
            LOG_S_CRIT("Unknown read result key: " << blobRange << " cookie: " << readCookie);
            return;
        }

        SizeBytesInFlight->Sub(blobRange.Size);
        SizeBlobsInFlight->Dec();
        InFlightDataSize -= blobRange.Size;

        // The same range can already be in the cache when the DS reply arrives. Serve those bytes
        // and do not insert the DS payload over the existing entry.
        auto cached = Cache.FindWithoutPromote(blobRange);
        if (cached != Cache.End()) {
            for (const auto& to : readIt->second.Waiting) {
                SendCachedHit(to, blobRange, cached.Value(), blobRange, false, ctx);
            }
            OutstandingReads.erase(readIt);
            return;
        }

        if (status == NKikimrProto::EReplyStatus::OK) {
            Y_ABORT_UNLESS(blobRange.Size == data.size(), "Read %s, size %" PRISZT, blobRange.ToString().c_str(), data.size());
            ReadBytes->Add(blobRange.Size);

            if (readIt->second.Cache) {
                InsertIntoCache(blobRange, data, false);
            }
        } else {
            LOG_S_WARN("Read failed for range: " << blobRange << " status: " << NKikimrProto::EReplyStatus_Name(status));
            ReadRangeFailedBytes->Add(blobRange.Size);
            ReadRangeFailedCount->Add(1);
        }

        YDB_LOG_DEBUG("",
            {"processSingleRangeResult", blobRange},
            {"sendReplies", readIt->second.Waiting.size()});
        // Send results to all waiters
        for (const auto& to : readIt->second.Waiting) {
            SendResult(to, blobRange, (NKikimrProto::EReplyStatus)status, data, detailedError, ctx);
        }

        OutstandingReads.erase(readIt);
    }

    // Forgets the pipe to the tablet and fails all in-flight requests to it
    void DestroyPipe(ui64 tabletId, const TActorContext& ctx) {
        ShardPipes.erase(tabletId);
        // Send errors for in-flight requests
        auto cookies = std::move(InFlightTabletRequests[tabletId]);
        InFlightTabletRequests.erase(tabletId);
        for (ui64 readCookie : cookies) {
            auto cookieIt = CookieToRange.find(readCookie);
            if (cookieIt == CookieToRange.end()) {
                // This might only happen in case fo race between response and pipe close
                LOG_S_NOTICE("Unknown read result cookie: " << readCookie);
                return;
            }

            std::vector<TBlobRange> blobRanges = std::move(cookieIt->second);
            CookieToRange.erase(readCookie);

            for (size_t i = 0; i < blobRanges.size(); ++i) {
                Y_ABORT_UNLESS(blobRanges[i].BlobId.GetTabletId() == tabletId);
                ProcessSingleRangeResult(blobRanges[i], readCookie, NKikimrProto::EReplyStatus::NOTREADY, {}, {}, ctx);
            }
        }

        MakeReadRequests(ctx);
    }

    void Handle(TEvTabletPipe::TEvClientConnected::TPtr& ev, const TActorContext& ctx) {
        TEvTabletPipe::TEvClientConnected* msg = ev->Get();
        const ui64 tabletId = msg->TabletId;
        Y_ABORT_UNLESS(tabletId != 0);
        if (msg->Status == NKikimrProto::OK) {
            LOG_S_DEBUG("Pipe connected to tablet: " << tabletId);
        } else {
            LOG_S_DEBUG("Pipe connection to tablet: " << tabletId << " failed with status: " << msg->Status);
            DestroyPipe(tabletId, ctx);
        }
    }

    void Handle(TEvTabletPipe::TEvClientDestroyed::TPtr& ev, const TActorContext& ctx) {
        const ui64 tabletId = ev->Get()->TabletId;
        Y_ABORT_UNLESS(tabletId != 0);

        LOG_S_DEBUG("Closed pipe connection to tablet: " << tabletId);
        DestroyPipe(tabletId, ctx);
    }

    void InsertIntoCache(const TBlobRange& blobRange, TString data, const bool sticky) {
        // Shrink the buffer if it has to much extra capacity
        if (data.capacity() > data.size() * 1.1) {
            data = TString(data.begin(), data.end());
        }
        YDB_LOG_DEBUG("",
            {"insertCache", blobRange},
            {"sticky", sticky});

        const TInstant now = TAppData::TimeProvider->Now();
        const TDuration protectFor = TDuration::MilliSeconds(WriteProtectDurationMs);
        const TInstant stickyUntil = (sticky && protectFor) ? (now + protectFor) : TInstant::Zero();

        // A sticky (re)insert refreshes StickyUntil and therefore moves the blob to the newest end of
        // StickyExpiry. Emergency eviction drops the earliest StickyUntil, so a later read does not
        // keep an older write. An unprotected insert over an existing entry is a no-op.
        auto existing = stickyUntil ? Cache.Find(blobRange) : Cache.FindWithoutPromote(blobRange);
        if (existing != Cache.End()) {
            if (stickyUntil) {
                PromoteToSticky(blobRange, *existing, stickyUntil);
            }
            return;
        }

        TCacheEntry entry;
        entry.Data = std::move(data);
        entry.StickyUntil = stickyUntil;
        if (Cache.Insert(blobRange, entry)) {
            CachedRanges.insert(blobRange);
            CacheDataSize += blobRange.Size;
            SizeBytes->Add(blobRange.Size);
            SizeBlobs->Inc();
            if (stickyUntil) {
                StickyExpiry.emplace(stickyUntil, blobRange);
                StickyDataSize += blobRange.Size;
                StickyBytes->Set(StickyDataSize);
                StickyBlobs->Inc();
            } else {
                Evictable.Insert(blobRange, 0);
            }
        }

        UpdateConsumption();
    }

    void PromoteToSticky(const TBlobRange& blobRange, TCacheEntry& entry, const TInstant stickyUntil) {
        if (entry.StickyUntil) {
            StickyExpiry.erase({ entry.StickyUntil, blobRange });
        } else {
            auto eit = Evictable.FindWithoutPromote(blobRange);
            if (eit != Evictable.End()) {
                Evictable.Erase(eit);
            }
            StickyDataSize += blobRange.Size;
            StickyBytes->Set(StickyDataSize);
            StickyBlobs->Inc();
        }
        entry.StickyUntil = stickyUntil;
        StickyExpiry.emplace(stickyUntil, blobRange);
    }

    void EraseCachedRange(const TBlobRange& blobRange) {
        auto it = Cache.FindWithoutPromote(blobRange);
        if (it == Cache.End()) {
            CachedRanges.erase(blobRange);
            return;
        }

        if (it.Value().StickyUntil) {
            StickyExpiry.erase({ it.Value().StickyUntil, blobRange });
            StickyDataSize -= blobRange.Size;
            StickyBytes->Set(StickyDataSize);
            StickyBlobs->Dec();
        } else {
            auto eit = Evictable.FindWithoutPromote(blobRange);
            if (eit != Evictable.End()) {
                Evictable.Erase(eit);
            }
        }

        CacheDataSize -= blobRange.Size;
        SizeBytes->Sub(blobRange.Size);
        SizeBlobs->Dec();
        Cache.Erase(it);
        const auto [begin, end] = CachedRanges.equal_range(blobRange);
        for (auto cachedIt = begin; cachedIt != end; ++cachedIt) {
            if (*cachedIt == blobRange) {
                CachedRanges.erase(cachedIt);
                break;
            }
        }
    }

    void GraduateExpired() {
        const TInstant now = TAppData::TimeProvider->Now();
        while (!StickyExpiry.empty() && StickyExpiry.begin()->first <= now) {
            const auto [until, range] = *StickyExpiry.begin();
            StickyExpiry.erase(StickyExpiry.begin());
            auto it = Cache.FindWithoutPromote(range);
            if (it == Cache.End() || it.Value().StickyUntil != until) {
                continue;
            }
            it->StickyUntil = TInstant::Zero();
            Evictable.Insert(range, 0);
            StickyDataSize -= range.Size;
            StickyBytes->Set(StickyDataSize);
            StickyBlobs->Dec();
        }
    }

    void Evict(const TActorContext&) {
        GraduateExpired();
        while (CacheDataSize + InFlightDataSize > MaxCacheDataSize) {
            TBlobRange victim;
            bool stickyVictim = false;
            auto eit = Evictable.FindOldest();
            if (eit != Evictable.End()) {
                victim = eit.Key();
                Evictable.Erase(eit);
            } else if (!StickyExpiry.empty()) {
                // Only sticky blobs are left. Drop the earliest insert (StickyUntil), not the least
                // recently read one: a promoting read must not save an older write.
                victim = StickyExpiry.begin()->second;
                stickyVictim = true;
            } else {
                break;
            }

            LOG_S_DEBUG("Evict: " << victim << " CacheDataSize: " << CacheDataSize << " InFlightDataSize: " << (i64)InFlightDataSize
                                  << " MaxCacheDataSize: " << (i64)MaxCacheDataSize << " sticky: " << stickyVictim);

            Evictions->Inc();
            EvictedBytes->Add(victim.Size);
            if (stickyVictim) {
                StickyEvictions->Inc();
                StickyEvictedBytes->Add(victim.Size);
            }
            EraseCachedRange(victim);

            SizeBytes->Set(CacheDataSize);
            SizeBlobs->Set(Cache.Size());
        }

        UpdateConsumption();
    }
};

}   // namespace

TBlobCacheSettings TBlobCacheSettings::FromProto(const NKikimrConfig::TBlobCacheConfig& cfg) {
    TBlobCacheSettings settings;
    settings.MaxCacheDataSize = cfg.GetMaxSizeBytes();
    settings.MaxInFlightBytes = cfg.GetMaxInFlightBytes();
    settings.MaxRequestBytes = cfg.GetMaxRequestBytes();
    settings.ReadDeadlineMs = cfg.GetReadDeadlineMs();
    settings.WriteProtectDurationMs = cfg.GetWriteProtectDurationMs();
    settings.MaxCacheDataSizeFromConfig = cfg.HasMaxSizeBytes();
    return settings;
}

NActors::IActor* CreateBlobCache(const TBlobCacheSettings& settings, TIntrusivePtr<::NMonitoring::TDynamicCounters> counters) {
    return new TBlobCache(settings, counters);
}

}   // namespace NKikimr::NBlobCache
