#include <ydb/core/tablet_flat/flat_range_cache.h>
#include <ydb/core/tablet_flat/shared_cache_s3fifo.h>
#include <ydb/core/tablet_flat/shared_handle.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/generic/hash_set.h>
#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <atomic>
#include <cstring>
#include <limits>
#include <string>

namespace {

using namespace NKikimr;
using namespace NKikimr::NSharedCache;
using namespace NKikimr::NTable;

constexpr size_t MaxOps = 192;
constexpr size_t MaxRanges = 32;
constexpr ui64 MaxKey = 127;
constexpr size_t MaxPoolAllocs = 48;
constexpr size_t MaxS3Pages = 32;
constexpr size_t MaxHandles = 8;
constexpr size_t MaxRefs = 16;
constexpr size_t MaxPins = 16;
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

TVector<TCell> MakeKey(ui64 value) {
    return {TCell::Make(value)};
}

ui64 KeyValue(TArrayRef<const TCell> key) {
    Y_ABORT_UNLESS(key.size() == 1);
    return key[0].AsValue<ui64>();
}

TRowVersion MakeVersion(FuzzedDataProvider& fdp) {
    return TRowVersion(
        fdp.ConsumeIntegralInRange<ui64>(0, 32),
        fdp.ConsumeIntegralInRange<ui64>(0, 32));
}

struct TRangeModel {
    ui64 From = 0;
    ui64 To = 0;
    bool FromInclusive = true;
    bool ToInclusive = true;
    TRowVersion Version = TRowVersion::Min();
};

bool IsValidRange(const TRangeModel& range) {
    return range.From < range.To || (range.From == range.To && range.FromInclusive && range.ToInclusive);
}

bool IsBefore(const TRangeModel& left, const TRangeModel& right) {
    return left.To < right.From ||
        (left.To == right.From && (!left.ToInclusive || !right.FromInclusive));
}

bool Contains(const TRangeModel& range, ui64 key) {
    const bool insideLeft = range.From < key || (range.From == key && range.FromInclusive);
    const bool insideRight = key < range.To || (key == range.To && range.ToInclusive);
    return insideLeft && insideRight;
}

TRangeModel FromEntry(TKeyRangeCache::const_iterator it) {
    return {
        .From = KeyValue(it->FromKey),
        .To = KeyValue(it->ToKey),
        .FromInclusive = it->FromInclusive,
        .ToInclusive = it->ToInclusive,
        .Version = it->MaxVersion,
    };
}

TKeyRangeCache::const_iterator IteratorAt(const TKeyRangeCache& cache, size_t index) {
    auto it = cache.begin();
    while (index-- && it != cache.end()) {
        ++it;
    }
    return it;
}

TVector<TRangeModel> ReadModelFromCache(const TKeyRangeCache& cache) {
    TVector<TRangeModel> result;
    for (auto it = cache.begin(); it != cache.end(); ++it) {
        result.push_back(FromEntry(it));
    }
    return result;
}

bool CanInsert(const TVector<TRangeModel>& model, const TRangeModel& candidate) {
    if (!IsValidRange(candidate)) {
        return false;
    }

    for (const auto& range : model) {
        if (!IsBefore(range, candidate) && !IsBefore(candidate, range)) {
            return false;
        }
    }

    return true;
}

void SortModel(TVector<TRangeModel>& model) {
    std::sort(model.begin(), model.end(), [](const TRangeModel& left, const TRangeModel& right) {
        if (left.From != right.From) {
            return left.From < right.From;
        }
        return left.FromInclusive && !right.FromInclusive;
    });
}

void CheckRangeModel(const TVector<TRangeModel>& model) {
    for (size_t i = 0; i < model.size(); ++i) {
        Y_ABORT_UNLESS(IsValidRange(model[i]));
        if (i > 0) {
            Y_ABORT_UNLESS(IsBefore(model[i - 1], model[i]));
        }
    }
}

void CheckRangeCache(const TKeyRangeCache& cache, const TVector<TRangeModel>& model) {
    CheckRangeModel(model);
    Y_ABORT_UNLESS(cache.GetTotalUsed() <= cache.GetTotalAllocated());

    size_t count = 0;
    for (auto it = cache.begin(); it != cache.end(); ++it, ++count) {
        Y_ABORT_UNLESS(count < model.size());
        const auto actual = FromEntry(it);
        const auto& expected = model[count];
        Y_ABORT_UNLESS(actual.From == expected.From);
        Y_ABORT_UNLESS(actual.To == expected.To);
        Y_ABORT_UNLESS(actual.FromInclusive == expected.FromInclusive);
        Y_ABORT_UNLESS(actual.ToInclusive == expected.ToInclusive);
        Y_ABORT_UNLESS(actual.Version == expected.Version);
    }
    Y_ABORT_UNLESS(count == model.size());

    for (ui64 key = 0; key <= MaxKey; ++key) {
        TVector<TCell> keyCells = MakeKey(key);
        const bool expected = std::any_of(model.begin(), model.end(), [key](const TRangeModel& range) {
            return Contains(range, key);
        });
        auto [forwardIt, forwardFound] = cache.FindKey(keyCells);
        auto [reverseIt, reverseFound] = cache.FindKeyReverse(keyCells);
        Y_ABORT_UNLESS(forwardFound == expected);
        Y_ABORT_UNLESS(reverseFound == expected);
        if (forwardFound) {
            Y_ABORT_UNLESS(forwardIt != cache.end());
            Y_ABORT_UNLESS(Contains(FromEntry(forwardIt), key));
        }
        if (reverseFound) {
            Y_ABORT_UNLESS(reverseIt != cache.end());
            Y_ABORT_UNLESS(Contains(FromEntry(reverseIt), key));
        }
    }

    TStringStream dump;
    dump << cache.DumpRanges();
    Y_UNUSED(dump.Str());
}

TArrayRef<TCell> AllocateKey(TKeyRangeCache& cache, ui64 value) {
    TVector<TCell> key = MakeKey(value);
    return cache.AllocateKey(key);
}

void FuzzSpecialMemoryPool(FuzzedDataProvider& fdp) {
    TSpecialMemoryPool pool;
    struct TAlloc {
        void* Ptr = nullptr;
        size_t Len = 0;
        size_t Used = 0;
    };

    TVector<TAlloc> allocations;
    size_t expectedUsed = 0;

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<ui32>(0, 3)) {
            case 0:
                if (allocations.size() < MaxPoolAllocs) {
                    const size_t len = fdp.ConsumeIntegralInRange<size_t>(1, 256);
                    const size_t before = pool.TotalUsed();
                    void* ptr = pool.Allocate(len);
                    const size_t used = pool.TotalUsed() - before;
                    std::memset(ptr, 0xa5, len);
                    allocations.push_back({ptr, len, used});
                    expectedUsed += used;
                }
                break;

            case 1:
                pool.Reserve(fdp.ConsumeIntegralInRange<size_t>(1, 512));
                break;

            case 2:
                if (!allocations.empty()) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, allocations.size() - 1);
                    const auto alloc = allocations[index];
                    pool.Deallocate(alloc.Ptr, alloc.Len);
                    expectedUsed -= alloc.Used;
                    allocations.erase(allocations.begin() + index);
                }
                break;

            default:
                pool.Clear();
                allocations.clear();
                expectedUsed = 0;
                break;
        }

        Y_ABORT_UNLESS(pool.TotalUsed() == expectedUsed);
        Y_ABORT_UNLESS(pool.TotalAllocated() >= pool.TotalUsed());
    }
}

void FuzzKeyRangeCache(FuzzedDataProvider& fdp) {
    TVector<NScheme::TTypeInfoOrder> types;
    types.emplace_back(NScheme::TTypeInfo(NScheme::NTypeIds::Uint64));
    TVector<TCell> defaults(1);
    TIntrusiveConstPtr<TKeyCellDefaults> keyDefaults = TKeyCellDefaults::Make(types, defaults);

    TKeyRangeCacheConfig config;
    config.MinRows = fdp.ConsumeIntegralInRange<size_t>(1, 8);
    config.MaxBytes = fdp.ConsumeIntegralInRange<size_t>(256, 4096);
    auto gcList = MakeIntrusive<TKeyRangeCacheNeedGCList>();
    TKeyRangeCache cache(*keyDefaults, config, gcList);
    TVector<TRangeModel> model;

    TKeyRangeCache::TStats lastStats = cache.Stats();
    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        switch (fdp.ConsumeIntegralInRange<ui32>(0, 9)) {
            case 0: {
                if (model.size() >= MaxRanges) {
                    break;
                }

                ui64 a = fdp.ConsumeIntegralInRange<ui64>(0, MaxKey);
                ui64 b = fdp.ConsumeIntegralInRange<ui64>(0, MaxKey);
                if (a > b) {
                    std::swap(a, b);
                }
                TRangeModel candidate{
                    .From = a,
                    .To = b,
                    .FromInclusive = fdp.ConsumeBool(),
                    .ToInclusive = fdp.ConsumeBool(),
                    .Version = MakeVersion(fdp),
                };
                if (candidate.From == candidate.To) {
                    candidate.FromInclusive = true;
                    candidate.ToInclusive = true;
                }
                if (!CanInsert(model, candidate)) {
                    break;
                }

                TKeyRangeEntry entry(
                    AllocateKey(cache, candidate.From),
                    AllocateKey(cache, candidate.To),
                    candidate.FromInclusive,
                    candidate.ToInclusive,
                    candidate.Version);
                cache.Add(entry);
                model.push_back(candidate);
                SortModel(model);
                break;
            }

            case 1:
                if (!model.empty()) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
                    cache.Invalidate(IteratorAt(cache, index));
                    model.erase(model.begin() + index);
                }
                break;

            case 2:
                if (!model.empty()) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
                    const auto& range = model[index];
                    const ui64 key = fdp.ConsumeIntegralInRange<ui64>(range.From, range.To);
                    if (Contains(range, key)) {
                        TVector<TCell> keyCells = MakeKey(key);
                        cache.InvalidateKey(IteratorAt(cache, index), keyCells);
                        if (key == range.From && range.FromInclusive) {
                            model.erase(model.begin() + index);
                        } else {
                            model[index].To = key;
                            model[index].ToInclusive = false;
                        }
                    }
                }
                break;

            case 3:
                if (!model.empty()) {
                    cache.Touch(IteratorAt(cache, fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1)));
                }
                break;

            case 4:
                if (model.size() >= 2) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 2);
                    if (model[index].To == model[index + 1].From) {
                        const TRowVersion version = MakeVersion(fdp);
                        cache.Merge(IteratorAt(cache, index), IteratorAt(cache, index + 1), version);
                        model[index].To = model[index + 1].To;
                        model[index].ToInclusive = model[index + 1].ToInclusive;
                        model[index].Version = Max(Max(model[index].Version, model[index + 1].Version), version);
                        model.erase(model.begin() + index + 1);
                    }
                }
                break;

            case 5:
                if (!model.empty()) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
                    TRangeModel candidate = model[index];
                    candidate.From = fdp.ConsumeIntegralInRange<ui64>(0, candidate.From);
                    candidate.FromInclusive = fdp.ConsumeBool();
                    if (candidate.From == candidate.To) {
                        candidate.FromInclusive = true;
                        candidate.ToInclusive = true;
                    }
                    const bool leftOk = index == 0 || IsBefore(model[index - 1], candidate);
                    if (IsValidRange(candidate) && leftOk) {
                        const TRowVersion version = MakeVersion(fdp);
                        cache.ExtendLeft(IteratorAt(cache, index), AllocateKey(cache, candidate.From), candidate.FromInclusive, version);
                        model[index].From = candidate.From;
                        model[index].FromInclusive = candidate.FromInclusive;
                        model[index].Version = Max(model[index].Version, version);
                    }
                }
                break;

            case 6:
                if (!model.empty()) {
                    const size_t index = fdp.ConsumeIntegralInRange<size_t>(0, model.size() - 1);
                    TRangeModel candidate = model[index];
                    candidate.To = fdp.ConsumeIntegralInRange<ui64>(candidate.To, MaxKey);
                    candidate.ToInclusive = fdp.ConsumeBool();
                    if (candidate.From == candidate.To) {
                        candidate.FromInclusive = true;
                        candidate.ToInclusive = true;
                    }
                    const bool rightOk = index + 1 == model.size() || IsBefore(candidate, model[index + 1]);
                    if (IsValidRange(candidate) && rightOk) {
                        const TRowVersion version = MakeVersion(fdp);
                        cache.ExtendRight(IteratorAt(cache, index), AllocateKey(cache, candidate.To), candidate.ToInclusive, version);
                        model[index].To = candidate.To;
                        model[index].ToInclusive = candidate.ToInclusive;
                        model[index].Version = Max(model[index].Version, version);
                    }
                }
                break;

            case 7:
                cache.EvictOld();
                model = ReadModelFromCache(cache);
                break;

            case 8:
                cache.CollectGarbage();
                model = ReadModelFromCache(cache);
                break;

            default:
                gcList->RunGC();
                model = ReadModelFromCache(cache);
                break;
        }

        SortModel(model);
        Y_ABORT_UNLESS(cache.Stats().Allocations >= lastStats.Allocations);
        Y_ABORT_UNLESS(cache.Stats().Deallocations >= lastStats.Deallocations);
        Y_ABORT_UNLESS(cache.Stats().Evictions >= lastStats.Evictions);
        Y_ABORT_UNLESS(cache.Stats().GarbageCollections >= lastStats.GarbageCollections);
        lastStats = cache.Stats();
        CheckRangeCache(cache, model);
    }
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

    void SetFrequency(ui32 frequency) noexcept {
        Frequency.store(frequency, std::memory_order_release);
    }

    void IncrementFrequency() noexcept {
        ui32 value = Frequency.load(std::memory_order_relaxed);
        while (value < 3 && !Frequency.compare_exchange_weak(
            value, value + 1, std::memory_order_acq_rel, std::memory_order_relaxed))
        { }
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
    for (const auto& page : pages) {
        Y_ABORT_UNLESS(page->GetFrequency() <= 3);
        if (page->Location == ES3FIFOPageLocation::SmallQueue ||
                page->Location == ES3FIFOPageLocation::MainQueue) {
            expectedSize += page->Size;
        } else {
            Y_ABORT_UNLESS(page->Location == ES3FIFOPageLocation::None);
        }
    }

    Y_ABORT_UNLESS(cache.GetSize() == expectedSize);
    Y_UNUSED(cache.Dump());
}

void FuzzS3FIFO(FuzzedDataProvider& fdp) {
    TS3FIFOCache<TS3Page, TS3PageTraits> cache(fdp.ConsumeIntegralInRange<ui64>(0, MaxCacheLimit));
    TVector<THolder<TS3Page>> pages;
    for (size_t i = 0; i < MaxS3Pages; ++i) {
        pages.push_back(MakeHolder<TS3Page>(
            static_cast<ui32>(i),
            fdp.ConsumeIntegralInRange<size_t>(1, 64)));
    }

    ui64 lastEvictOpsCounter = cache.GetEvictOpsCounter();
    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        TS3Page& page = *pages[fdp.ConsumeIntegralInRange<size_t>(0, pages.size() - 1)];
        switch (fdp.ConsumeIntegralInRange<ui32>(0, 6)) {
            case 0:
                if (page.Location == ES3FIFOPageLocation::None) {
                    CheckS3Evicted(cache.Insert(&page));
                } else {
                    page.IncrementFrequency();
                }
                break;
            case 1:
                if (page.Location == ES3FIFOPageLocation::None) {
                    CheckS3Evicted(cache.InsertUntouched(&page));
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
                    const ui64 before = cache.GetEvictOpsCounter();
                    TS3Page* evicted = cache.EvictNext();
                    Y_ABORT_UNLESS(evicted);
                    Y_ABORT_UNLESS(evicted->Location == ES3FIFOPageLocation::None);
                    Y_ABORT_UNLESS(evicted->GetFrequency() == 0);
                    Y_ABORT_UNLESS(cache.GetEvictOpsCounter() > before);
                }
                break;
            case 4:
                CheckS3Evicted(cache.EnsureLimits());
                Y_ABORT_UNLESS(cache.GetSize() <= cache.GetLimit());
                break;
            case 5:
                cache.UpdateLimit(fdp.ConsumeIntegralInRange<ui64>(0, MaxCacheLimit));
                break;
            default:
                page.IncrementFrequency();
                break;
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
    for (size_t i = 0; i < MaxHandles; ++i) {
        handles.push_back(MakeHolder<THandleSlot>(MakeData(fdp)));
    }

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        THandleSlot& slot = *handles[fdp.ConsumeIntegralInRange<size_t>(0, handles.size() - 1)];
        TTestHandle* handle = slot.Handle.Get();
        switch (fdp.ConsumeIntegralInRange<ui32>(0, 7)) {
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
                    const bool wasGarbage = handle->IsGarbage();
                    const bool garbage = handle->UnUse();
                    --slot.ExpectedUses;
                    Y_ABORT_UNLESS(garbage == (slot.ExpectedUses == 0 && !handle->IsDropped() && !wasGarbage));
                }
                break;
            case 2:
                if (!handle->IsDropped() || handle->PinCount() > 0) {
                    slot.Pins.push_back(handle->Pin());
                    Y_ABORT_UNLESS(slot.Pins.back());
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
                    Y_ABORT_UNLESS(bool(dropped) == (slot.ExpectedUses == 0));
                }
                break;
            case 6:
                handle->IncrementFrequency();
                break;
            default:
                handle->SetFrequency(fdp.ConsumeIntegralInRange<ui32>(0, 3));
                break;
        }
        CheckHandleSlot(slot);
    }
}

void FuzzSharedRefsAndGC(FuzzedDataProvider& fdp) {
    auto gc = MakeIntrusive<TSharedPageGCList>();
    auto handle = MakeIntrusive<TTestHandle>(MakeData(fdp));
    TVector<TSharedPageRef> refs;
    TVector<TPinnedPageRef> pins;

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
        switch (fdp.ConsumeIntegralInRange<ui32>(0, 7)) {
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
                if (pins.size() < MaxPins && !refs.empty()) {
                    auto& ref = refs[fdp.ConsumeIntegralInRange<size_t>(0, refs.size() - 1)];
                    if (ref.IsUsed()) {
                        pins.emplace_back(ref);
                    }
                }
                break;
            case 5:
                if (!pins.empty()) {
                    pins.erase(pins.begin() + fdp.ConsumeIntegralInRange<size_t>(0, pins.size() - 1));
                }
                break;
            case 6:
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
    }

    pins.clear();
    releaseInitialUse();
    refs.clear();

    bool sawHandleInGC = false;
    while (auto popped = gc->PopGC()) {
        Y_ABORT_UNLESS(popped == handle);
        sawHandleInGC = true;
    }
    Y_ABORT_UNLESS(sawHandleInGC);
    Y_ABORT_UNLESS(handle->UseCount() == 0);
    Y_ABORT_UNLESS(handle->IsGarbage());
    Y_ABORT_UNLESS(gc->PopGC() == nullptr);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const uint8_t* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    switch (fdp.ConsumeIntegralInRange<ui32>(0, 4)) {
        case 0:
            FuzzSpecialMemoryPool(fdp);
            break;
        case 1:
            FuzzKeyRangeCache(fdp);
            break;
        case 2:
            FuzzS3FIFO(fdp);
            break;
        case 3:
            FuzzRawHandles(fdp);
            break;
        default:
            FuzzSharedRefsAndGC(fdp);
            break;
    }
    return 0;
}
