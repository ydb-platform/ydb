#include <ydb/core/base/appdata.h>
#include <ydb/core/base/blobstorage.h>
#include <ydb/core/base/counters.h>
#include <ydb/core/tablet_flat/flat_bio_events.h>
#include <ydb/core/tablet_flat/flat_bio_stats.h>
#include <ydb/core/tablet_flat/flat_sausage_packet.h>
#include <ydb/core/tablet_flat/flat_sausage_writer.h>
#include <ydb/core/tablet_flat/shared_cache_counters.h>
#include <ydb/core/tablet_flat/shared_cache_events.h>
#include <ydb/core/tablet_flat/shared_sausagecache.h>
#include <ydb/core/testlib/actors/test_runtime.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <array>
#include <string>

namespace {

using namespace NKikimr;
using namespace NActors;
using namespace NKikimr::NPageCollection;
using namespace NKikimr::NSharedCache;
using namespace NKikimr::NTabletFlatExecutor;

constexpr size_t MaxCollections = 3;
constexpr size_t MaxPagesPerCollection = 8;
constexpr size_t MaxOps = 96;
constexpr size_t MaxPageBytes = 64;
constexpr ui64 PageGroup = 51001;

TString ConsumeBytes(FuzzedDataProvider& fdp, size_t maxSize = MaxPageBytes) {
    const size_t len = fdp.ConsumeIntegralInRange<size_t>(1, Min(maxSize, fdp.remaining_bytes() + 1));
    std::string bytes = fdp.ConsumeBytesAsString(len);
    if (bytes.size() < len) {
        bytes.resize(len, '\0');
    }
    return TString(bytes.data(), bytes.size());
}

struct TCollectionFixture {
    TIntrusiveConstPtr<IPageCollection> PageCollection;
    THashMap<TLogoBlobID, TString> Blobs;

    static TCollectionFixture Make(FuzzedDataProvider& fdp, ui64 salt) {
        const ui8 channel = 1;
        const std::array<TSlot, 1> slots = {{TSlot(channel, PageGroup)}};
        TCookieAllocator cookieAllocator(
            100 + salt,
            (ui64(200 + salt) << 32) | (300 + salt),
            {1, 100000},
            slots);

        TCollectionFixture fixture;
        TWriter writer(cookieAllocator, channel, fdp.ConsumeIntegralInRange<ui32>(1, 48));

        const size_t total = fdp.ConsumeIntegralInRange<size_t>(1, MaxPagesPerCollection);
        for (size_t page = 0; page < total; ++page) {
            TString body = ConsumeBytes(fdp);
            const ui32 type = fdp.ConsumeBool()
                ? ui32(NTable::NPage::EPage::Opaque)
                : ui32(NTable::NPage::EPage::Undef);
            const ui32 pageId = writer.AddPage(body, type);
            if (fdp.ConsumeBool()) {
                writer.AddInplace(pageId, ConsumeBytes(fdp, 12));
            }
        }

        TSharedData meta = writer.Finish(true);
        for (auto& glob : writer.Grab()) {
            fixture.Blobs.emplace(glob.GId.Logo, std::move(glob.Data));
        }

        const ui32 metaBlobLimit = fdp.ConsumeIntegralInRange<ui32>(1, 128);
        TLargeGlobId metaId = cookieAllocator.Do(channel, meta.size(), metaBlobLimit);
        fixture.PageCollection = new TPageCollection(metaId, meta);
        Y_ABORT_UNLESS(fixture.PageCollection->Total() == total);
        return fixture;
    }
};

struct TFetchRequest {
    TActorId Sender;
    TActorId Recipient;
    ui64 Cookie = 0;
    ui32 Group = 0;
    TVector<TEvBlobStorage::TEvGet::TQuery> Queries;
};

struct TClientActor : public TActorBootstrapped<TClientActor> {
    TClientActor(TVector<NSharedCache::TEvResult::TPtr>& results, TVector<NSharedCache::TEvUpdated::TPtr>& updates)
        : Results(results)
        , Updates(updates)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(NSharedCache::TEvResult, Handle);
            hFunc(NSharedCache::TEvUpdated, Handle);
            hFunc(NBlockIO::TEvStat, Handle);
            default:
                break;
        }
    }

    void Handle(NSharedCache::TEvResult::TPtr& ev) {
        Results.push_back(ev);
    }

    void Handle(NSharedCache::TEvUpdated::TPtr& ev) {
        Updates.push_back(ev);
    }

    void Handle(NBlockIO::TEvStat::TPtr&) {
    }

    TVector<NSharedCache::TEvResult::TPtr>& Results;
    TVector<NSharedCache::TEvUpdated::TPtr>& Updates;
};

struct TBlobStorageActor : public TActorBootstrapped<TBlobStorageActor> {
    explicit TBlobStorageActor(TVector<TFetchRequest>& pendingFetches)
        : PendingFetches(pendingFetches)
    {}

    void Bootstrap() {
        Become(&TThis::StateWork);
    }

    STFUNC(StateWork) {
        switch (ev->GetTypeRewrite()) {
            hFunc(TEvBlobStorage::TEvGet, Handle);
            default:
                break;
        }
    }

    void Handle(TEvBlobStorage::TEvGet::TPtr& ev) {
        TFetchRequest fetch;
        fetch.Sender = ev->Sender;
        fetch.Recipient = ev->Recipient;
        fetch.Cookie = ev->Cookie;
        fetch.Group = PageGroup;
        fetch.Queries.reserve(ev->Get()->QuerySize);
        for (ui32 i = 0; i < ev->Get()->QuerySize; ++i) {
            fetch.Queries.push_back(ev->Get()->Queries[i]);
        }
        PendingFetches.push_back(std::move(fetch));
    }

    TVector<TFetchRequest>& PendingFetches;
};

TTestActorRuntime::TEgg MakeEgg() {
    return {new TAppData(0, 0, 0, 0, {}, nullptr, nullptr, nullptr, nullptr), nullptr, nullptr, {}, {}};
}

class TActorHarness {
public:
    explicit TActorHarness(FuzzedDataProvider& fdp) {
        Runtime.Initialize(MakeEgg());

        auto config = DefaultConfig(fdp);
        BlobStorageActor = Runtime.Register(new TBlobStorageActor(PendingFetches));
        Runtime.RegisterService(MakeBlobStorageProxyID(PageGroup), BlobStorageActor);
        CacheActor = Runtime.Register(CreateSharedPageCache(config, Runtime.GetDynamicCounters()));
        ClientA = Runtime.Register(new TClientActor(Results, Updates));
        ClientB = Runtime.Register(new TClientActor(Results, Updates));

        Dispatch();
    }

    void AddCollection(TCollectionFixture fixture) {
        for (const auto& [blobId, body] : fixture.Blobs) {
            BlobBodies[blobId] = body;
        }
        Collections.push_back(std::move(fixture));
    }

    void Request(FuzzedDataProvider& fdp) {
        if (Collections.empty()) {
            return;
        }

        auto& fixture = PickCollection(fdp);
        TVector<TPageId> pages;
        const ui32 total = fixture.PageCollection->Total();
        const size_t count = fdp.ConsumeIntegralInRange<size_t>(1, Min<size_t>(total, 5));
        pages.reserve(count);
        for (size_t i = 0; i < count; ++i) {
            pages.push_back(fdp.ConsumeIntegralInRange<TPageId>(0, total - 1));
        }

        auto priority = PickPriority(fdp);
        const ui64 cookie = ++RequestCookie;
        Send(PickClient(fdp), new TEvRequest(priority, fixture.PageCollection, std::move(pages), cookie), cookie);
        Dispatch();
        CheckCounters();
    }

    void Attach(FuzzedDataProvider& fdp) {
        if (Collections.empty()) {
            return;
        }

        auto mode = fdp.ConsumeBool() ? NTable::NPage::ECacheMode::TryKeepInMemory : NTable::NPage::ECacheMode::Regular;
        Send(PickClient(fdp), new TEvAttach(PickCollection(fdp).PageCollection, mode));
        Dispatch();
        CheckCounters();
    }

    void Detach(FuzzedDataProvider& fdp) {
        if (Collections.empty()) {
            return;
        }

        Send(PickClient(fdp), new TEvDetach(PickCollection(fdp).PageCollection->Label()));
        Dispatch();
        CheckCounters();
    }

    void Unregister(FuzzedDataProvider& fdp) {
        Send(PickClient(fdp), new TEvUnregister());
        Dispatch();
        CheckCounters();
    }

    void Sync(FuzzedDataProvider& fdp) {
        if (Collections.empty()) {
            return;
        }

        THashMap<TLogoBlobID, THashSet<TPageId>> pages;
        auto& fixture = PickCollection(fdp);
        const ui32 total = fixture.PageCollection->Total();
        const size_t count = fdp.ConsumeIntegralInRange<size_t>(1, Min<size_t>(total, 4));
        auto& set = pages[fixture.PageCollection->Label()];
        for (size_t i = 0; i < count; ++i) {
            set.insert(fdp.ConsumeIntegralInRange<TPageId>(0, total - 1));
        }

        Send(PickClient(fdp), new TEvSync(std::move(pages)));
        Dispatch();
        CheckCounters();
    }

    void SetLimit(FuzzedDataProvider& fdp) {
        const ui64 limit = fdp.ConsumeIntegralInRange<ui64>(0, 4096);
        Send(ClientA, new NMemory::TEvConsumerLimit(limit));
        Dispatch();
        CheckCounters();
    }

    void Wakeup() {
        Send(ClientA, new TKikimrEvents::TEvWakeup(static_cast<ui64>(EWakeupTag::DoGCManual)));
        Dispatch();
        CheckCounters();
    }

    void ReplyToBlobStorage(FuzzedDataProvider& fdp) {
        if (PendingFetches.empty()) {
            return;
        }

        const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, PendingFetches.size() - 1);
        TFetchRequest fetch = PendingFetches[index];
        PendingFetches.erase(PendingFetches.begin() + index);

        const bool failWholeRequest = fdp.ConsumeIntegralInRange<ui8>(0, 15) == 0;
        auto* result = new TEvBlobStorage::TEvGetResult(
            failWholeRequest ? NKikimrProto::ERROR : NKikimrProto::OK,
            fetch.Queries.size(),
            TGroupId::FromValue(fetch.Group ? fetch.Group : PageGroup));

        for (size_t i = 0; i < fetch.Queries.size(); ++i) {
            const auto& query = fetch.Queries[i];
            auto& response = result->Responses[i];
            response.Id = query.Id;
            response.Shift = query.Shift;
            response.RequestedSize = query.Size;

            auto* body = BlobBodies.FindPtr(query.Id);
            if (!body || failWholeRequest) {
                response.Status = failWholeRequest ? NKikimrProto::ERROR : NKikimrProto::NODATA;
                continue;
            }

            response.Status = NKikimrProto::OK;
            const size_t shift = Min<size_t>(query.Shift, body->size());
            const size_t size = Min<size_t>(query.Size ? query.Size : Max<ui32>(), body->size() - shift);
            TString slice = TString::Uninitialized(size);
            memcpy(slice.Detach(), body->data() + shift, size);
            response.Buffer = TRope(std::move(slice));
        }

        Runtime.Send(new IEventHandle(fetch.Sender, fetch.Recipient, result, 0, fetch.Cookie), 0, true);
        Dispatch();
        CheckResults();
        CheckCounters();
    }

    void Finish(FuzzedDataProvider& fdp) {
        while (!PendingFetches.empty() && fdp.ConsumeBool()) {
            ReplyToBlobStorage(fdp);
        }
        Send(ClientA, new TEvents::TEvPoison);
        Dispatch();
    }

private:
    static TSharedCacheConfig DefaultConfig(FuzzedDataProvider& fdp) {
        TSharedCacheConfig config;
        config.SetMemoryLimit(fdp.ConsumeIntegralInRange<ui64>(512, 4096));
        config.SetAsyncQueueInFlyLimit(fdp.ConsumeIntegralInRange<ui64>(1, 256));
        config.SetScanQueueInFlyLimit(fdp.ConsumeIntegralInRange<ui64>(1, 256));
        config.SetInMemoryInFlyLimit(fdp.ConsumeIntegralInRange<ui64>(1, 1024));
        config.SetMaxLimitDecreaseStepBytes(fdp.ConsumeIntegralInRange<ui64>(1, 1024));
        return config;
    }

    TCollectionFixture& PickCollection(FuzzedDataProvider& fdp) {
        return Collections[fdp.ConsumeIntegralInRange<size_t>(0, Collections.size() - 1)];
    }

    TActorId PickClient(FuzzedDataProvider& fdp) const {
        return fdp.ConsumeBool() ? ClientA : ClientB;
    }

    static NBlockIO::EPriority PickPriority(FuzzedDataProvider& fdp) {
        switch (fdp.ConsumeIntegralInRange<ui8>(0, 3)) {
            case 0:
                return NBlockIO::EPriority::Fast;
            case 1:
                return NBlockIO::EPriority::Bkgr;
            case 2:
                return NBlockIO::EPriority::Bulk;
            default:
                return NBlockIO::EPriority::Low;
        }
    }

    void Send(const TActorId& sender, IEventBase* ev, ui64 cookie = 0) {
        Runtime.Send(new IEventHandle(CacheActor, sender, ev, 0, cookie), 0, true);
    }

    void Dispatch() {
        TDispatchOptions options;
        options.FinalEvents.emplace_back([](IEventHandle&) { return false; });
        Runtime.DispatchEvents(options, TDuration::MilliSeconds(5));
    }

    void CheckResults() {
        for (auto& result : Results) {
            if (result->Get()->Status == NKikimrProto::OK) {
                for (const auto& page : result->Get()->Pages) {
                    Y_ABORT_UNLESS(page.Page);
                }
            }
        }
        Results.clear();
        Updates.clear();
    }

    void CheckCounters() {
        TSharedPageCacheCounters counters(GetServiceCounters(Runtime.GetDynamicCounters(), "tablets")->GetSubgroup("type", "S_CACHE"));
        Y_ABORT_UNLESS(static_cast<ui64>(counters.PageCollections->Val()) <= Collections.size());
        Y_ABORT_UNLESS(static_cast<ui64>(counters.Owners->Val()) <= 2);
        Y_ABORT_UNLESS(static_cast<ui64>(counters.PageCollectionOwners->Val()) <= Collections.size() * 2);
        Y_ABORT_UNLESS(counters.SucceedRequests->Val() + counters.FailedRequests->Val() <= counters.RequestedPages->Val());
        Y_ABORT_UNLESS(static_cast<ui64>(counters.LoadInFlyBytes->Val()) <= 1_MB);
        Y_ABORT_UNLESS(static_cast<ui64>(counters.PendingRequests->Val()) <= MaxOps * 2);
    }

private:
    TTestActorRuntime Runtime;
    TActorId BlobStorageActor;
    TActorId CacheActor;
    TActorId ClientA;
    TActorId ClientB;
    TVector<TCollectionFixture> Collections;
    THashMap<TLogoBlobID, TString> BlobBodies;
    TVector<TFetchRequest> PendingFetches;
    TVector<NSharedCache::TEvResult::TPtr> Results;
    TVector<NSharedCache::TEvUpdated::TPtr> Updates;
    ui64 RequestCookie = 0;
};

void FuzzActorCache(FuzzedDataProvider& fdp) {
    TActorHarness harness(fdp);

    const size_t collections = fdp.ConsumeIntegralInRange<size_t>(1, MaxCollections);
    for (size_t i = 0; i < collections; ++i) {
        harness.AddCollection(TCollectionFixture::Make(fdp, i + 1));
    }

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(1, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<ui8>(0, 7)) {
            case 0:
            case 1:
                harness.Request(fdp);
                break;
            case 2:
                harness.ReplyToBlobStorage(fdp);
                break;
            case 3:
                harness.Attach(fdp);
                break;
            case 4:
                harness.Detach(fdp);
                break;
            case 5:
                harness.Unregister(fdp);
                break;
            case 6:
                harness.Sync(fdp);
                break;
            default:
                fdp.ConsumeBool() ? harness.Wakeup() : harness.SetLimit(fdp);
                break;
        }
    }

    harness.Finish(fdp);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    if (size > 16 * 1024) {
        return 0;
    }
    FuzzedDataProvider fdp(data, size);
    FuzzActorCache(fdp);
    return 0;
}
