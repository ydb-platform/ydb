#include <ydb/core/tablet_flat/flat_sausagecache.h>
#include <ydb/core/tablet_flat/shared_cache_s3fifo.h>
#include <ydb/core/tablet_flat/shared_handle.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <string>

namespace {

using namespace NKikimr;
using namespace NKikimr::NSharedCache;
using namespace NKikimr::NTabletFlatExecutor;

constexpr size_t MaxS3Pages = 32;
constexpr size_t MaxHandles = 8;
constexpr size_t MaxCollections = 4;
constexpr size_t MaxPagesPerCollection = 16;
constexpr size_t MaxRefs = 16;
constexpr size_t MaxPins = 16;
constexpr size_t MaxOps = 192;
constexpr size_t MaxBytes = 64;
constexpr ui64 MaxCacheLimit = 512;

TSharedData MakeData(FuzzedDataProvider& fdp, size_t maxSize = MaxBytes) {
    const size_t len = fdp.ConsumeIntegralInRange<size_t>(1, std::min(maxSize, fdp.remaining_bytes() + 1));
    std::string bytes = fdp.ConsumeBytesAsString(len);
    if (bytes.size() < len) {
        bytes.resize(len, '\0');
    }
    return TSharedData::Copy(bytes.data(), bytes.size());
}

struct TS3Page : public TIntrusiveListItem<TS3Page> {
    ui32 Id = 0;
    size_t Size = 1;
    ES3FIFOPageLocation Location = ES3FIFOPageLocation::None;
    std::atomic<ui32> Frequency{0};

    TS3Page(ui32 id, size_t size)
        : Id(id)
        , Size(size)
    {}

    ui32 GetFrequency() const noexcept {
        return Frequency.load(std::memory_order_relaxed);
    }

    void IncrementFrequency() noexcept {
        ui32 value = Frequency.load(std::memory_order_relaxed);
        if (value < 3) {
            Frequency.compare_exchange_weak(value, value + 1,
                std::memory_order_acq_rel, std::memory_order_relaxed);
        }
    }

    void SetFrequency(ui32 frequency) noexcept {
        Frequency.store(frequency, std::memory_order_release);
    }
};

struct TS3PageTraits {
    struct TPageKey {
        ui32 Id = 0;
    };

    static ui64 GetSize(const TS3Page* page) {
        return page->Size;
    }

    static TPageKey GetKey(const TS3Page* page) {
        return {page->Id};
    }

    static size_t GetHash(const TPageKey& key) {
        return key.Id;
    }

    static TString ToString(const TPageKey& key) {
        return ::ToString(key.Id);
    }

    static TString GetKeyToString(const TS3Page* page) {
        return ToString(GetKey(page));
    }

    static ES3FIFOPageLocation GetLocation(const TS3Page* page) {
        return page->Location;
    }

    static void SetLocation(TS3Page* page, ES3FIFOPageLocation location) {
        page->Location = location;
    }

    static ui32 GetFrequency(const TS3Page* page) {
        return page->GetFrequency();
    }

    static void SetFrequency(TS3Page* page, ui32 frequency) {
        page->SetFrequency(frequency);
    }
};

void CheckS3Evicted(const TIntrusiveList<TS3Page>& evicted) {
    for (const auto& page : evicted) {
        Y_ABORT_UNLESS(page.Location == ES3FIFOPageLocation::None);
        Y_ABORT_UNLESS(page.GetFrequency() == 0);
    }
}

void CheckS3Cache(const TS3FIFOCache<TS3Page, TS3PageTraits>& cache, const TVector<THolder<TS3Page>>& pages) {
    ui64 expectedSize = 0;
    size_t cachedPages = 0;

    for (const auto& page : pages) {
        Y_ABORT_UNLESS(page->GetFrequency() <= 3);
        switch (page->Location) {
            case ES3FIFOPageLocation::None:
                break;
            case ES3FIFOPageLocation::SmallQueue:
            case ES3FIFOPageLocation::MainQueue:
                expectedSize += page->Size;
                ++cachedPages;
                break;
            default:
                Y_ABORT_UNLESS(false);
        }
    }

    Y_ABORT_UNLESS(cache.GetSize() == expectedSize);
    if (cachedPages == 0) {
        Y_ABORT_UNLESS(cache.GetSize() == 0);
    }

    Y_UNUSED(cache.Dump());
}

void FuzzS3FIFO(FuzzedDataProvider& fdp) {
    const ui64 initialLimit = fdp.ConsumeIntegralInRange<ui64>(0, MaxCacheLimit);
    TS3FIFOCache<TS3Page, TS3PageTraits> cache(initialLimit);

    TVector<THolder<TS3Page>> pages;
    pages.reserve(MaxS3Pages);
    for (size_t i = 0; i < MaxS3Pages; ++i) {
        const ui32 salt = fdp.ConsumeIntegralInRange<ui32>(0, 7);
        const size_t pageSize = fdp.ConsumeIntegralInRange<size_t>(1, 64);
        pages.push_back(MakeHolder<TS3Page>(static_cast<ui32>(i * 8 + salt), pageSize));
    }

    ui64 lastEvictOpsCounter = cache.GetEvictOpsCounter();
    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        TS3Page& page = *pages[fdp.ConsumeIntegralInRange<size_t>(0, pages.size() - 1)];

        switch (fdp.ConsumeIntegralInRange<ui32>(0, 7)) {
            case 0:
                if (page.Location == ES3FIFOPageLocation::None) {
                    auto evicted = cache.Insert(&page);
                    CheckS3Evicted(evicted);
                } else {
                    page.IncrementFrequency();
                }
                break;

            case 1:
                if (page.Location == ES3FIFOPageLocation::None) {
                    auto evicted = cache.InsertUntouched(&page);
                    CheckS3Evicted(evicted);
                }
                break;

            case 2:
                if (page.Location != ES3FIFOPageLocation::None) {
                    cache.Erase(&page);
                    Y_ABORT_UNLESS(page.Location == ES3FIFOPageLocation::None);
                    Y_ABORT_UNLESS(page.GetFrequency() == 0);
                }
                break;

            case 3:
                if (cache.GetSize() > 0) {
                    const ui64 evictOpsBefore = cache.GetEvictOpsCounter();
                    TS3Page* evicted = cache.EvictNext();
                    Y_ABORT_UNLESS(evicted);
                    Y_ABORT_UNLESS(evicted->Location == ES3FIFOPageLocation::None);
                    Y_ABORT_UNLESS(evicted->GetFrequency() == 0);
                    Y_ABORT_UNLESS(cache.GetEvictOpsCounter() > evictOpsBefore);
                }
                break;

            case 4: {
                auto evicted = cache.EnsureLimits();
                CheckS3Evicted(evicted);
                Y_ABORT_UNLESS(cache.GetSize() <= cache.GetLimit());
                break;
            }

            case 5:
                cache.UpdateLimit(fdp.ConsumeIntegralInRange<ui64>(0, MaxCacheLimit));
                break;

            case 6:
                page.IncrementFrequency();
                break;

            default: {
                TS3FIFOGhostPageQueue<TS3PageTraits> ghosts;
                const size_t ghostOps = fdp.ConsumeIntegralInRange<size_t>(0, 16);
                THashSet<size_t> model;
                TVector<size_t> queue;
                for (size_t j = 0; j < ghostOps; ++j) {
                    TS3Page& ghostPage = *pages[fdp.ConsumeIntegralInRange<size_t>(0, pages.size() - 1)];
                    const size_t hash = TS3PageTraits::GetHash(TS3PageTraits::GetKey(&ghostPage));
                    const bool added = ghosts.Add(TS3PageTraits::GetKey(&ghostPage));
                    if (model.insert(hash).second) {
                        queue.push_back(hash);
                        Y_ABORT_UNLESS(added);
                    } else {
                        Y_ABORT_UNLESS(!added);
                    }
                    const size_t limit = fdp.ConsumeIntegralInRange<size_t>(0, MaxS3Pages);
                    ghosts.Limit(limit);
                    while (queue.size() > limit) {
                        model.erase(queue.front());
                        queue.erase(queue.begin());
                    }
                    for (const auto& candidate : pages) {
                        const bool expected = model.contains(TS3PageTraits::GetHash(TS3PageTraits::GetKey(candidate.Get())));
                        Y_ABORT_UNLESS(ghosts.Contains(TS3PageTraits::GetKey(candidate.Get())) == expected);
                    }
                    Y_UNUSED(ghosts.Dump());
                }
                break;
            }
        }

        Y_ABORT_UNLESS(cache.GetEvictOpsCounter() >= lastEvictOpsCounter);
        lastEvictOpsCounter = cache.GetEvictOpsCounter();
        CheckS3Cache(cache, pages);
    }
}

class TTestHandle : public TSharedPageHandle {
public:
    explicit TTestHandle(TSharedData data) {
        Initialize(std::move(data));
    }
};

struct THandleSlot {
    TIntrusivePtr<TTestHandle> Handle;
    size_t ExpectedUses = 1;
    TVector<TSharedData> Pins;

    explicit THandleSlot(TSharedData data)
        : Handle(MakeIntrusive<TTestHandle>(std::move(data)))
    {}
};

void CheckHandleSlot(const THandleSlot& slot) {
    Y_ABORT_UNLESS(slot.Handle->IsInitialized());
    Y_ABORT_UNLESS(slot.Handle->UseCount() == slot.ExpectedUses);
    Y_ABORT_UNLESS(slot.Handle->PinCount() == slot.Pins.size());
    Y_ABORT_UNLESS(slot.Handle->GetFrequency() <= 3);
}

void FuzzRawHandles(FuzzedDataProvider& fdp) {
    TVector<THolder<THandleSlot>> handles;
    handles.reserve(MaxHandles);
    for (size_t i = 0; i < MaxHandles; ++i) {
        handles.push_back(MakeHolder<THandleSlot>(MakeData(fdp)));
    }

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        THandleSlot& slot = *handles[fdp.ConsumeIntegralInRange<size_t>(0, handles.size() - 1)];
        auto* handle = slot.Handle.Get();

        switch (fdp.ConsumeIntegralInRange<ui32>(0, 8)) {
            case 0:
                if (handle->Use()) {
                    ++slot.ExpectedUses;
                } else {
                    Y_ABORT_UNLESS(handle->IsDropped());
                    Y_ABORT_UNLESS(slot.ExpectedUses == 0);
                }
                break;

            case 1:
                if (slot.ExpectedUses > 0) {
                    const bool becameGarbage = handle->UnUse();
                    --slot.ExpectedUses;
                    Y_ABORT_UNLESS(!becameGarbage || (slot.ExpectedUses == 0 && !handle->IsDropped()));
                    Y_ABORT_UNLESS(!becameGarbage || handle->IsGarbage());
                }
                break;

            case 2:
                if (!handle->IsDropped() || handle->PinCount() > 0) {
                    TSharedData pinned = handle->Pin();
                    Y_ABORT_UNLESS(pinned);
                    slot.Pins.push_back(std::move(pinned));
                }
                break;

            case 3:
                if (!slot.Pins.empty()) {
                    slot.Pins.pop_back();
                    handle->UnPin();
                }
                break;

            case 4:
                if (!handle->IsDropped()) {
                    const bool moved = handle->TryMove(MakeData(fdp));
                    Y_ABORT_UNLESS(moved == slot.Pins.empty());
                }
                break;

            case 5:
                if (handle->IsGarbage() && !handle->IsDropped()) {
                    TSharedData dropped = handle->TryDrop();
                    if (slot.ExpectedUses == 0) {
                        Y_ABORT_UNLESS(dropped);
                        Y_ABORT_UNLESS(handle->IsDropped());
                    } else {
                        Y_ABORT_UNLESS(!dropped);
                        Y_ABORT_UNLESS(!handle->IsGarbage());
                    }
                }
                break;

            case 6:
                handle->IncrementFrequency();
                break;

            case 7:
                handle->SetFrequency(fdp.ConsumeIntegralInRange<ui32>(0, 3));
                break;

            default:
                Y_UNUSED(handle->IsGarbage());
                Y_UNUSED(handle->IsDropped());
                break;
        }

        CheckHandleSlot(slot);
    }

    for (auto& slot : handles) {
        while (!slot->Pins.empty()) {
            slot->Pins.pop_back();
            slot->Handle->UnPin();
        }
        while (slot->ExpectedUses > 0) {
            slot->Handle->UnUse();
            --slot->ExpectedUses;
        }
    }
}

void FuzzSharedRefsAndGC(FuzzedDataProvider& fdp) {
    auto gc = MakeIntrusive<TSharedPageGCList>();
    auto handle = MakeIntrusive<TTestHandle>(MakeData(fdp));

    TVector<TSharedPageRef> refs;
    TVector<TPinnedPageRef> pins;
    refs.reserve(MaxRefs);
    pins.reserve(MaxPins);

    refs.push_back(TSharedPageRef::MakeUsed(handle, gc));
    Y_ABORT_UNLESS(handle->UseCount() == 2);

    bool releasedInitialUse = false;
    auto releaseInitialUse = [&]() {
        if (!releasedInitialUse) {
            if (handle->UnUse()) {
                gc->PushGC(handle.Get());
            }
            releasedInitialUse = true;
        }
    };

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<ui32>(0, 8)) {
            case 0:
                if (refs.size() < MaxRefs) {
                    refs.emplace_back(handle, gc);
                }
                break;

            case 1:
                if (refs.size() < MaxRefs && !refs.empty()) {
                    refs.push_back(refs[fdp.ConsumeIntegralInRange<size_t>(0, refs.size() - 1)]);
                }
                break;

            case 2:
                if (!refs.empty()) {
                    refs[fdp.ConsumeIntegralInRange<size_t>(0, refs.size() - 1)].Use();
                }
                break;

            case 3:
                if (!refs.empty()) {
                    refs[fdp.ConsumeIntegralInRange<size_t>(0, refs.size() - 1)].UnUse();
                }
                break;

            case 4:
                if (!refs.empty()) {
                    const size_t idx = fdp.ConsumeIntegralInRange<size_t>(0, refs.size() - 1);
                    if (refs[idx].IsUsed()) {
                        refs[idx].IncrementFrequency();
                    }
                }
                break;

            case 5:
                if (pins.size() < MaxPins && !refs.empty()) {
                    const size_t idx = fdp.ConsumeIntegralInRange<size_t>(0, refs.size() - 1);
                    if (refs[idx].IsUsed()) {
                        pins.emplace_back(refs[idx]);
                    }
                }
                break;

            case 6:
                if (!pins.empty()) {
                    pins.erase(pins.begin() + fdp.ConsumeIntegralInRange<size_t>(0, pins.size() - 1));
                }
                break;

            case 7:
                if (!refs.empty()) {
                    refs.erase(refs.begin() + fdp.ConsumeIntegralInRange<size_t>(0, refs.size() - 1));
                }
                break;

            default:
                releaseInitialUse();
                break;
        }

        Y_ABORT_UNLESS(handle->GetFrequency() <= 3);
        Y_ABORT_UNLESS(handle->PinCount() == pins.size());
        if (handle->UseCount() == 0) {
            Y_ABORT_UNLESS(handle->IsGarbage());
        }
    }

    pins.clear();
    releaseInitialUse();
    refs.clear();

    auto popped = gc->PopGC();
    Y_ABORT_UNLESS(popped == handle);
    Y_ABORT_UNLESS(gc->PopGC() == nullptr);

    TSharedPageRef privateRef = TSharedPageRef::MakePrivate(MakeData(fdp));
    Y_ABORT_UNLESS(privateRef);
    Y_ABORT_UNLESS(privateRef.IsUsed());
    TPinnedPageRef pinned(privateRef);
    Y_ABORT_UNLESS(pinned);
    privateRef.IncrementFrequency();
    Y_ABORT_UNLESS(privateRef.GetHandle()->GetFrequency() == 1);
}

struct TFakePageCollection : public NPageCollection::IPageCollection {
    TFakePageCollection(ui64 id, TVector<ui32> pageSizes)
        : Id(1, 1, id)
        , PageSizes(std::move(pageSizes))
    {}

    const TLogoBlobID& Label() const noexcept override {
        return Id;
    }

    ui32 Total() const noexcept override {
        return PageSizes.size();
    }

    NPageCollection::TInfo Page(ui32 page) const override {
        Y_ABORT_UNLESS(page < PageSizes.size());
        return {PageSizes[page], ui32(NTable::NPage::EPage::Undef)};
    }

    NPageCollection::TBorder Bounds(ui32) const override {
        Y_ABORT("Unexpected Bounds(...) call");
    }

    NPageCollection::TGlobId Glob(ui32) const override {
        Y_ABORT("Unexpected Glob(...) call");
    }

    bool Verify(ui32, TArrayRef<const char>) const override {
        Y_ABORT("Unexpected Verify(...) call");
    }

    size_t BackingSize() const noexcept override {
        size_t result = 0;
        for (ui32 size : PageSizes) {
            result += size;
        }
        return result;
    }

    TLogoBlobID Id;
    TVector<ui32> PageSizes;
};

struct TPrivateCollectionModel {
    TIntrusivePtr<TPrivatePageCache::TPageCollection> Collection;
    TVector<ui32> Sizes;
    THashSet<TPageId> Pages;
    THashSet<TPageId> StickyPages;
    bool Registered = false;
    ECacheMode CacheMode = ECacheMode::Regular;

    ui64 BackingSize() const {
        ui64 result = 0;
        for (ui32 size : Sizes) {
            result += size;
        }
        return result;
    }
};

TSharedPageRef MakePrivateCachePage(FuzzedDataProvider& fdp) {
    return TSharedPageRef::MakePrivate(MakeData(fdp));
}

void CheckPrivateCacheStats(const TPrivatePageCache& cache, const TVector<TPrivateCollectionModel>& models) {
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
            Y_ABORT_UNLESS(pageId < model.Sizes.size());
            sharedBodyBytes += model.Sizes[pageId];
            if (model.StickyPages.contains(pageId)) {
                stickyBytes += model.Sizes[pageId];
            }
        }

        if (model.CacheMode == ECacheMode::TryKeepInMemory) {
            tryKeepInMemoryBytes += model.BackingSize();
        }
    }

    const auto& stats = cache.GetStats();
    Y_ABORT_UNLESS(stats.PageCollections == pageCollections);
    Y_ABORT_UNLESS(stats.SharedBodyBytes == sharedBodyBytes);
    Y_ABORT_UNLESS(stats.StickyBytes == stickyBytes);
    Y_ABORT_UNLESS(stats.TryKeepInMemoryBytes == tryKeepInMemoryBytes);
}

void FuzzPrivatePageCache(FuzzedDataProvider& fdp) {
    TPrivatePageCache cache;
    TVector<TPrivateCollectionModel> models;
    models.reserve(MaxCollections);

    for (size_t i = 0; i < MaxCollections; ++i) {
        TVector<ui32> sizes;
        const size_t total = fdp.ConsumeIntegralInRange<size_t>(1, MaxPagesPerCollection);
        sizes.reserve(total);
        for (size_t page = 0; page < total; ++page) {
            sizes.push_back(fdp.ConsumeIntegralInRange<ui32>(1, 64));
        }

        TIntrusiveConstPtr<NPageCollection::IPageCollection> fake =
            new TFakePageCollection(i + 1, sizes);
        TPrivateCollectionModel model;
        model.Sizes = sizes;
        model.Collection = MakeIntrusive<TPrivatePageCache::TPageCollection>(std::move(fake));
        models.push_back(std::move(model));
    }

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        TPrivateCollectionModel& model = models[fdp.ConsumeIntegralInRange<size_t>(0, models.size() - 1)];
        const TPageId pageId = fdp.ConsumeIntegralInRange<TPageId>(0, model.Sizes.size() - 1);

        switch (fdp.ConsumeIntegralInRange<ui32>(0, 8)) {
            case 0:
                if (!model.Pages.contains(pageId)) {
                    if (model.Registered) {
                        cache.AddPage(pageId, MakePrivateCachePage(fdp), model.Collection.Get());
                    } else {
                        model.Collection->AddPage(pageId, MakePrivateCachePage(fdp));
                    }
                    model.Pages.insert(pageId);
                }
                break;

            case 1:
                if (!model.StickyPages.contains(pageId)) {
                    if (model.Registered) {
                        cache.AddStickyPage(pageId, MakePrivateCachePage(fdp), model.Collection.Get());
                    } else {
                        model.Collection->AddStickyPage(pageId, MakePrivateCachePage(fdp));
                    }
                    model.Pages.insert(pageId);
                    model.StickyPages.insert(pageId);
                }
                break;

            case 2:
                if (model.Registered && model.Pages.contains(pageId) && !model.StickyPages.contains(pageId)) {
                    cache.DropPage(pageId, model.Collection.Get());
                    model.Pages.erase(pageId);
                }
                break;

            case 3:
                if (!model.Registered) {
                    auto touches = cache.AddPageCollection(model.Collection);
                    Y_ABORT_UNLESS(touches.size() == (model.Pages.empty() ? 0 : 1));
                    model.Registered = true;
                }
                break;

            case 4:
                if (model.Registered) {
                    cache.DropPageCollection(model.Collection.Get());
                    model.Pages.clear();
                    model.StickyPages.clear();
                    model.Registered = false;
                }
                break;

            case 5:
                if (model.Registered && model.Pages.contains(pageId)) {
                    TSharedPageRef ref = cache.TryGetPage(pageId, model.Collection.Get());
                    if (ref) {
                        Y_ABORT_UNLESS(ref.IsUsed());
                        if (fdp.ConsumeBool()) {
                            TPinnedPageRef pinned(ref);
                            Y_ABORT_UNLESS(pinned);
                        }
                        ref.UnUse();
                    } else {
                        Y_ABORT_UNLESS(!model.StickyPages.contains(pageId));
                        model.Pages.erase(pageId);
                    }
                }
                break;

            case 6:
                if (model.Registered) {
                    const ECacheMode nextMode = fdp.ConsumeBool()
                        ? ECacheMode::Regular
                        : ECacheMode::TryKeepInMemory;
                    const bool changed = cache.UpdateCacheMode(nextMode, model.Collection.Get());
                    Y_ABORT_UNLESS(changed == (nextMode != model.CacheMode));
                    model.CacheMode = nextMode;
                }
                break;

            case 7:
                if (model.Registered) {
                    Y_ABORT_UNLESS(cache.GetPageCollection(model.Collection->Id) == model.Collection.Get());
                    Y_ABORT_UNLESS(cache.FindPageCollection(model.Collection->Id) == model.Collection.Get());
                }
                break;

            default: {
                auto detached = cache.DetachPrivatePageCache();
                Y_ABORT_UNLESS(detached.size() == cache.GetStats().PageCollections);
                break;
            }
        }

        CheckPrivateCacheStats(cache, models);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);

    switch (fdp.ConsumeIntegralInRange<ui32>(0, 3)) {
        case 0:
            FuzzS3FIFO(fdp);
            break;
        case 1:
            FuzzRawHandles(fdp);
            break;
        case 2:
            FuzzSharedRefsAndGC(fdp);
            break;
        default:
            FuzzPrivatePageCache(fdp);
            break;
    }

    return 0;
}
