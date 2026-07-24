#include <ydb/core/tablet_flat/flat_sausage_packet.h>
#include <ydb/core/tablet_flat/flat_sausage_writer.h>
#include <ydb/core/tablet_flat/flat_sausagecache.h>
#include <ydb/core/tablet_flat/shared_cache_s3fifo.h>
#include <ydb/core/tablet_flat/shared_cache_pages.h>
#include <ydb/core/tablet_flat/shared_page.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/algorithm.h>
#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/digest/multi.h>
#include <util/system/yassert.h>

#include <array>
#include <string>

namespace {

using namespace NKikimr;
using namespace NKikimr::NPageCollection;
using namespace NKikimr::NSharedCache;
using namespace NKikimr::NTabletFlatExecutor;

constexpr size_t MaxCollections = 3;
constexpr size_t MaxPagesPerCollection = 8;
constexpr size_t MaxOps = 192;
constexpr size_t MaxPageBytes = 96;
constexpr ui64 MaxCacheLimit = 2048;

TString ConsumeBytes(FuzzedDataProvider& fdp, size_t maxSize = MaxPageBytes) {
    const size_t len = fdp.ConsumeIntegralInRange<size_t>(1, Min(maxSize, fdp.remaining_bytes() + 1));
    std::string bytes = fdp.ConsumeBytesAsString(len);
    if (bytes.size() < len) {
        bytes.resize(len, '\0');
    }
    return TString(bytes.data(), bytes.size());
}

TSharedData CopyData(TStringBuf bytes) {
    return TSharedData::Copy(bytes.data(), bytes.size());
}

struct TCollectionFixture {
    TIntrusiveConstPtr<IPageCollection> PageCollection;
    TVector<TString> PageBodies;
    TVector<TString> BlobBodies;
    TVector<TGlobId> BlobIds;

    static TCollectionFixture Make(FuzzedDataProvider& fdp, ui64 salt) {
        const ui8 channel = 1;
        const ui32 group = 1000 + static_cast<ui32>(salt % 1000);
        const std::array<TSlot, 1> slots = {{TSlot(channel, group)}};
        TCookieAllocator cookieAllocator(
            10 + salt,
            (ui64(20 + salt) << 32) | (30 + salt),
            {1, 100000},
            slots);

        TCollectionFixture fixture;

        const ui32 maxBlobSize = fdp.ConsumeIntegralInRange<ui32>(1, 64);
        TWriter writer(cookieAllocator, channel, maxBlobSize);

        const size_t total = fdp.ConsumeIntegralInRange<size_t>(1, MaxPagesPerCollection);
        fixture.PageBodies.reserve(total);
        for (size_t page = 0; page < total; ++page) {
            TString body = ConsumeBytes(fdp);
            fixture.PageBodies.push_back(body);
            const ui32 type = fdp.ConsumeBool()
                ? ui32(NTable::NPage::EPage::Opaque)
                : ui32(NTable::NPage::EPage::Undef);
            const ui32 pageId = writer.AddPage(body, type);
            if (fdp.ConsumeBool()) {
                TString inplace = ConsumeBytes(fdp, 16);
                writer.AddInplace(pageId, inplace);
            }
        }

        TSharedData meta = writer.Finish(true);
        for (auto& glob : writer.Grab()) {
            fixture.BlobIds.push_back(glob.GId);
            fixture.BlobBodies.push_back(std::move(glob.Data));
        }

        const ui32 metaBlobLimit = fdp.ConsumeIntegralInRange<ui32>(1, 128);
        TLargeGlobId metaId = cookieAllocator.Do(channel, meta.size(), metaBlobLimit);
        fixture.PageCollection = new TPageCollection(metaId, meta);
        fixture.CheckLifecycle();
        return fixture;
    }

    TString ReadPageThroughBounds(TPageId pageId) const {
        const TBorder bounds = PageCollection->Bounds(pageId);
        Y_ABORT_UNLESS(bounds);
        Y_ABORT_UNLESS(bounds.Bytes == PageCollection->Page(pageId).Size);
        Y_ABORT_UNLESS(bounds.Lo.Blob <= bounds.Up.Blob);
        Y_ABORT_UNLESS(bounds.Up.Blob < BlobBodies.size());

        TString body;
        body.reserve(bounds.Bytes);
        for (ui32 blob = bounds.Lo.Blob; blob <= bounds.Up.Blob; ++blob) {
            const TGlobId glob = PageCollection->Glob(blob);
            Y_ABORT_UNLESS(glob == BlobIds[blob]);
            const TString& stored = BlobBodies[blob];
            const ui32 begin = blob == bounds.Lo.Blob ? bounds.Lo.Skip : 0;
            const ui32 end = blob == bounds.Up.Blob ? bounds.Up.Skip : stored.size();
            Y_ABORT_UNLESS(begin <= end);
            Y_ABORT_UNLESS(end <= stored.size());
            body.append(stored.data() + begin, end - begin);
        }

        Y_ABORT_UNLESS(body.size() == bounds.Bytes);
        return body;
    }

    bool VerifyPage(TPageId pageId, const TString& body) const {
        return PageCollection->Verify(pageId, TArrayRef<const char>(body.data(), body.size()));
    }

    void CheckLifecycle() const {
        Y_ABORT_UNLESS(PageCollection);
        Y_ABORT_UNLESS(PageCollection->Label());
        Y_ABORT_UNLESS(PageCollection->Total() == PageBodies.size());
        for (TPageId pageId = 0; pageId < PageBodies.size(); ++pageId) {
            const TInfo info = PageCollection->Page(pageId);
            Y_ABORT_UNLESS(info);
            Y_ABORT_UNLESS(info.Size == PageBodies[pageId].size());
            const TString body = ReadPageThroughBounds(pageId);
            Y_ABORT_UNLESS(body == PageBodies[pageId]);
            Y_ABORT_UNLESS(VerifyPage(pageId, body));
            TString corrupted = body;
            corrupted[0] = char(corrupted[0]) ^ char(0x5a);
            Y_UNUSED(VerifyPage(pageId, corrupted));
        }
    }
};

struct TSharedPageTraits {
    struct TPageKey {
        const void* Collection = nullptr;
        TPageId PageId = 0;
    };

    static ui64 GetSize(const TPage* page) {
        return sizeof(TPage) + page->Size;
    }

    static TPageKey GetKey(const TPage* page) {
        return {page->Collection, page->PageId};
    }

    static size_t GetHash(const TPageKey& key) {
        return MultiHash(reinterpret_cast<size_t>(key.Collection), key.PageId);
    }

    static TString ToString(const TPageKey& key) {
        return TStringBuilder() << "Collection: " << key.Collection << " PageId: " << key.PageId;
    }

    static TString GetKeyToString(const TPage* page) {
        return ToString(GetKey(page));
    }

    static ES3FIFOPageLocation GetLocation(const TPage* page) {
        return page->Location;
    }

    static void SetLocation(TPage* page, ES3FIFOPageLocation location) {
        page->Location = location;
    }

    static ui32 GetFrequency(const TPage* page) {
        return page->GetFrequency();
    }

    static void SetFrequency(TPage* page, ui32 frequency) {
        page->SetFrequency(frequency);
    }
};

struct TSharedCollectionSlot {
    TCollectionFixture Fixture;
    TPageMap<TIntrusivePtr<TPage>> Pages;
    THashSet<TPageId> DroppedPages;

    explicit TSharedCollectionSlot(TCollectionFixture fixture)
        : Fixture(std::move(fixture))
    {
        Pages.resize(Fixture.PageCollection->Total());
    }

    TLogoBlobID Id() const {
        return Fixture.PageCollection->Label();
    }

    void Reset(TCollectionFixture fixture) {
        Fixture = std::move(fixture);
        Pages = TPageMap<TIntrusivePtr<TPage>>();
        Pages.resize(Fixture.PageCollection->Total());
        DroppedPages.clear();
    }
};

struct TFakeSharedCache {
    explicit TFakeSharedCache(ui64 limit)
        : Cache(limit)
    {}

    TSharedPageRef Fetch(TSharedCollectionSlot& slot, TPageId pageId, FuzzedDataProvider& fdp) {
        TPage* page = EnsurePage(slot, pageId);

        switch (page->State) {
            case PageStateLoaded:
                page->IncrementFrequency();
                ++Hits;
                return TSharedPageRef::MakeUsed(page, SharedPages->GCList);

            case PageStateEvicted:
                ++Hits;
                if (page->Use()) {
                    page->State = PageStateLoaded;
                    InsertLoaded(page);
                    return TSharedPageRef::MakeUsed(page, SharedPages->GCList);
                }
                slot.Pages.erase(pageId);
                slot.DroppedPages.insert(pageId);
                return {};

            case PageStateNo:
                break;

            default:
                return {};
        }

        ++Misses;
        TString body = slot.Fixture.ReadPageThroughBounds(pageId);
        const ui32 ioMode = fdp.ConsumeIntegralInRange<ui32>(0, 9);
        if (ioMode == 0) {
            ++IoDrops;
            return {};
        }
        if (ioMode == 1) {
            body[0] = char(body[0]) ^ char(0xa5);
        }
        if (!slot.Fixture.VerifyPage(pageId, body)) {
            ++VerifyFailures;
            return {};
        }

        page->ProvideBody(CopyData(body));
        InsertLoaded(page);
        ++VerifiedLoads;
        return TSharedPageRef::MakeUsed(page, SharedPages->GCList);
    }

    void EvictNext() {
        if (Cache.GetSize() == 0) {
            return;
        }
        if (TPage* page = Cache.EvictNext()) {
            EvictLoaded(page);
        }
    }

    void EnsureLimits() {
        Evict(Cache.EnsureLimits());
    }

    void UpdateLimit(ui64 limit) {
        Cache.UpdateLimit(limit);
        EnsureLimits();
    }

    void ProcessGC() {
        while (auto raw = SharedPages->GCList->PopGC()) {
            auto* page = static_cast<TPage*>(raw.Get());
            if (page->State != PageStateEvicted || page->IsDropped()) {
                continue;
            }

            auto* slot = reinterpret_cast<TSharedCollectionSlot*>(page->Collection);
            if (page->GetFrequency() > 0 && page->Use()) {
                page->State = PageStateLoaded;
                InsertLoaded(page);
                continue;
            }

            if (page->IsGarbage()) {
                if (page->TryDrop()) {
                    slot->DroppedPages.insert(page->PageId);
                    slot->Pages.erase(page->PageId);
                    ++Dropped;
                }
            }
        }
    }

    void RetireCollection(TSharedCollectionSlot& slot) {
        for (auto& [pageId, page] : slot.Pages) {
            Y_UNUSED(pageId);
            if (page->Location != ES3FIFOPageLocation::None) {
                Cache.Erase(page.Get());
            }
            if (page->State == PageStateLoaded) {
                page->State = PageStateEvicted;
                if (page->UnUse()) {
                    SharedPages->GCList->PushGC(page.Get());
                }
            }
            RetiredPages.push_back(page);
        }
        slot.Pages.clear();
        slot.DroppedPages.clear();
        ProcessGC();
    }

    void Check(const TVector<THolder<TSharedCollectionSlot>>& slots) const {
        ui64 expectedCacheSize = 0;
        for (const auto& slot : slots) {
            for (const auto& [pageId, page] : slot->Pages) {
                Y_ABORT_UNLESS(pageId < slot->Fixture.PageCollection->Total());
                Y_ABORT_UNLESS(page->PageId == pageId);
                Y_ABORT_UNLESS(page->Size == slot->Fixture.PageCollection->Page(pageId).Size);
                Y_ABORT_UNLESS(page->GetFrequency() <= 3);
                if (page->Location != ES3FIFOPageLocation::None) {
                    Y_ABORT_UNLESS(page->State == PageStateLoaded);
                    expectedCacheSize += TSharedPageTraits::GetSize(page.Get());
                }
                if (page->State == PageStateEvicted) {
                    Y_ABORT_UNLESS(page->Location == ES3FIFOPageLocation::None);
                }
            }
        }
        Y_ABORT_UNLESS(Cache.GetSize() == expectedCacheSize);
        Y_UNUSED(Cache.Dump());
    }

    ui64 VerifiedLoads = 0;
    ui64 VerifyFailures = 0;
    ui64 IoDrops = 0;
    ui64 Hits = 0;
    ui64 Misses = 0;
    ui64 Dropped = 0;

private:
    TPage* EnsurePage(TSharedCollectionSlot& slot, TPageId pageId) {
        Y_ABORT_UNLESS(pageId < slot.Fixture.PageCollection->Total());
        if (TPage* page = slot.Pages[pageId].Get()) {
            return page;
        }

        auto page = MakeIntrusive<TPage>(
            pageId,
            slot.Fixture.PageCollection->Page(pageId).Size,
            reinterpret_cast<TCollection*>(&slot));
        TPage* raw = page.Get();
        Y_ABORT_UNLESS(slot.Pages.emplace(pageId, std::move(page)));
        return raw;
    }

    void InsertLoaded(TPage* page) {
        page->EnsureNoCacheFlags();
        Evict(Cache.Insert(page));
    }

    void Evict(TIntrusiveList<TPage>&& pages) {
        while (!pages.Empty()) {
            EvictLoaded(pages.PopFront());
        }
    }

    void EvictLoaded(TPage* page) {
        page->EnsureNoCacheFlags();
        Y_ABORT_UNLESS(page->State == PageStateLoaded);
        page->State = PageStateEvicted;
        if (page->UnUse()) {
            SharedPages->GCList->PushGC(page);
        }
    }

    TIntrusivePtr<TSharedCachePages> SharedPages = new TSharedCachePages;
    TS3FIFOCache<TPage, TSharedPageTraits> Cache;
    TVector<TIntrusivePtr<TPage>> RetiredPages;
};

struct TPrivateModel {
    TIntrusivePtr<TPrivatePageCache::TPageCollection> Collection;
    THashSet<TPageId> Pages;
    THashSet<TPageId> StickyPages;
    bool Registered = false;
    ECacheMode CacheMode = ECacheMode::Regular;

    void Reset(TIntrusiveConstPtr<IPageCollection> pageCollection) {
        Collection = MakeIntrusive<TPrivatePageCache::TPageCollection>(std::move(pageCollection));
        Pages.clear();
        StickyPages.clear();
        Registered = false;
        CacheMode = ECacheMode::Regular;
    }
};

void CheckPrivateCacheStats(const TPrivatePageCache& cache, const TVector<TPrivateModel>& models) {
    ui64 pageCollections = 0;
    ui64 sharedBodyBytes = 0;
    ui64 stickyBytes = 0;
    ui64 tryKeepInMemoryBytes = 0;

    for (const auto& model : models) {
        if (!model.Registered) {
            continue;
        }

        ++pageCollections;
        for (TPageId pageId : model.Pages) {
            sharedBodyBytes += model.Collection->GetPageSize(pageId);
            if (model.StickyPages.contains(pageId)) {
                stickyBytes += model.Collection->GetPageSize(pageId);
            }
        }
        if (model.CacheMode == ECacheMode::TryKeepInMemory) {
            tryKeepInMemoryBytes += model.Collection->PageCollection->BackingSize();
        }
    }

    const auto& stats = cache.GetStats();
    Y_ABORT_UNLESS(stats.PageCollections == pageCollections);
    Y_ABORT_UNLESS(stats.SharedBodyBytes == sharedBodyBytes);
    Y_ABORT_UNLESS(stats.StickyBytes == stickyBytes);
    Y_ABORT_UNLESS(stats.TryKeepInMemoryBytes == tryKeepInMemoryBytes);
}

void AddPrivatePage(
        TPrivatePageCache& privateCache,
        TPrivateModel& model,
        TPageId pageId,
        TSharedPageRef ref,
        bool sticky)
{
    Y_ABORT_UNLESS(ref);
    Y_ABORT_UNLESS(ref.IsUsed());

    if (sticky) {
        if (model.Registered) {
            privateCache.AddStickyPage(pageId, std::move(ref), model.Collection.Get());
        } else {
            model.Collection->AddStickyPage(pageId, std::move(ref));
        }
        model.StickyPages.insert(pageId);
    } else if (!model.Pages.contains(pageId)) {
        if (model.Registered) {
            privateCache.AddPage(pageId, std::move(ref), model.Collection.Get());
        } else {
            model.Collection->AddPage(pageId, std::move(ref));
        }
    }
    model.Pages.insert(pageId);
}

void SyncDroppedPages(TPrivatePageCache& privateCache, TPrivateModel& model, TSharedCollectionSlot& slot) {
    if (!model.Registered) {
        slot.DroppedPages.clear();
        return;
    }

    for (TPageId pageId : slot.DroppedPages) {
        if (model.Pages.contains(pageId) && !model.StickyPages.contains(pageId)) {
            privateCache.DropPage(pageId, model.Collection.Get());
            model.Pages.erase(pageId);
        }
    }
    slot.DroppedPages.clear();
}

void FuzzPageCollectionCache(FuzzedDataProvider& fdp) {
    TFakeSharedCache sharedCache(fdp.ConsumeIntegralInRange<ui64>(0, MaxCacheLimit));
    TPrivatePageCache privateCache;

    TVector<THolder<TSharedCollectionSlot>> sharedSlots;
    TVector<TPrivateModel> privateModels;
    sharedSlots.reserve(MaxCollections);
    privateModels.reserve(MaxCollections);
    for (size_t i = 0; i < MaxCollections; ++i) {
        auto fixture = TCollectionFixture::Make(fdp, i + 1);
        privateModels.emplace_back();
        privateModels.back().Reset(fixture.PageCollection);
        sharedSlots.push_back(MakeHolder<TSharedCollectionSlot>(std::move(fixture)));
    }

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t op = 0; op < ops; ++op) {
        const size_t slotIndex = fdp.ConsumeIntegralInRange<size_t>(0, sharedSlots.size() - 1);
        auto& sharedSlot = *sharedSlots[slotIndex];
        auto& privateModel = privateModels[slotIndex];
        const TPageId pageId = fdp.ConsumeIntegralInRange<TPageId>(0, sharedSlot.Fixture.PageCollection->Total() - 1);

        switch (fdp.ConsumeIntegralInRange<ui32>(0, 12)) {
            case 0:
            case 1: {
                TSharedPageRef ref = sharedCache.Fetch(sharedSlot, pageId, fdp);
                if (ref) {
                    if (fdp.ConsumeBool()) {
                        TPinnedPageRef pinned(ref);
                        Y_ABORT_UNLESS(pinned);
                    }
                    if (!privateModel.Pages.contains(pageId) || fdp.ConsumeBool()) {
                        AddPrivatePage(privateCache, privateModel, pageId, std::move(ref), false);
                    }
                }
                break;
            }

            case 2: {
                TSharedPageRef ref = sharedCache.Fetch(sharedSlot, pageId, fdp);
                if (ref) {
                    AddPrivatePage(privateCache, privateModel, pageId, std::move(ref), true);
                }
                break;
            }

            case 3:
                if (privateModel.Registered && privateModel.Pages.contains(pageId) && !privateModel.StickyPages.contains(pageId)) {
                    TSharedPageRef ref = privateCache.TryGetPage(pageId, privateModel.Collection.Get());
                    if (ref) {
                        Y_ABORT_UNLESS(ref.IsUsed());
                        if (fdp.ConsumeBool()) {
                            ref.IncrementFrequency();
                        }
                        if (fdp.ConsumeBool()) {
                            TPinnedPageRef pinned(ref);
                            Y_ABORT_UNLESS(pinned);
                        }
                        ref.UnUse();
                    } else {
                        Y_ABORT_UNLESS(!privateModel.StickyPages.contains(pageId));
                        privateModel.Pages.erase(pageId);
                    }
                }
                break;

            case 4:
                if (privateModel.Registered && privateModel.Pages.contains(pageId) && !privateModel.StickyPages.contains(pageId)) {
                    privateCache.DropPage(pageId, privateModel.Collection.Get());
                    privateModel.Pages.erase(pageId);
                }
                break;

            case 5:
                if (!privateModel.Registered) {
                    auto touches = privateCache.AddPageCollection(privateModel.Collection);
                    Y_ABORT_UNLESS(touches.size() == (privateModel.Pages.empty() ? 0 : 1));
                    if (!privateModel.Pages.empty()) {
                        Y_ABORT_UNLESS(touches.contains(privateModel.Collection->Id));
                        Y_ABORT_UNLESS(touches[privateModel.Collection->Id].size() == privateModel.Pages.size());
                    }
                    privateModel.Registered = true;
                }
                break;

            case 6:
                if (privateModel.Registered) {
                    privateCache.DropPageCollection(privateModel.Collection.Get());
                    privateModel.Pages.clear();
                    privateModel.StickyPages.clear();
                    privateModel.Registered = false;
                }
                break;

            case 7:
                if (privateModel.Registered) {
                    const ECacheMode nextMode = fdp.ConsumeBool() ? ECacheMode::Regular : ECacheMode::TryKeepInMemory;
                    const bool changed = privateCache.UpdateCacheMode(nextMode, privateModel.Collection.Get());
                    Y_ABORT_UNLESS(changed == (nextMode != privateModel.CacheMode));
                    privateModel.CacheMode = nextMode;
                }
                break;

            case 8:
                sharedCache.EvictNext();
                sharedCache.ProcessGC();
                break;

            case 9:
                sharedCache.UpdateLimit(fdp.ConsumeIntegralInRange<ui64>(0, MaxCacheLimit));
                sharedCache.ProcessGC();
                break;

            case 10:
                SyncDroppedPages(privateCache, privateModel, sharedSlot);
                break;

            case 11: {
                auto detached = privateCache.DetachPrivatePageCache();
                Y_ABORT_UNLESS(detached.size() == privateCache.GetStats().PageCollections);
                break;
            }

            default:
                if (privateModel.Registered) {
                    privateCache.DropPageCollection(privateModel.Collection.Get());
                }
                privateModel.Reset(sharedSlot.Fixture.PageCollection);
                sharedCache.RetireCollection(sharedSlot);
                sharedSlot.Reset(TCollectionFixture::Make(fdp, 100 + op * MaxCollections + slotIndex));
                privateModel.Reset(sharedSlot.Fixture.PageCollection);
                break;
        }

        sharedCache.ProcessGC();
        sharedSlot.Fixture.CheckLifecycle();
        sharedCache.Check(sharedSlots);
        CheckPrivateCacheStats(privateCache, privateModels);
    }

    for (size_t i = 0; i < sharedSlots.size(); ++i) {
        if (privateModels[i].Registered) {
            privateCache.DropPageCollection(privateModels[i].Collection.Get());
            privateModels[i].Registered = false;
            privateModels[i].Pages.clear();
            privateModels[i].StickyPages.clear();
            privateModels[i].CacheMode = ECacheMode::Regular;
        }
        sharedCache.RetireCollection(*sharedSlots[i]);
    }
    sharedCache.ProcessGC();
    CheckPrivateCacheStats(privateCache, privateModels);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    FuzzPageCollectionCache(fdp);
    return 0;
}
