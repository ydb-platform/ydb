#include <ydb/core/util/cache.h>
#include <ydb/core/util/page_map.h>
#include <ydb/core/util/simple_cache.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <algorithm>

namespace {

class TInput {
public:
    TInput(const ui8* data, size_t size)
        : Data(data)
        , Size(size)
    {}

    bool Empty() const {
        return Pos == Size;
    }

    ui8 Byte(ui8 fallback = 0) {
        return Pos < Size ? Data[Pos++] : fallback;
    }

    ui32 Int(ui32 mod, ui32 fallback = 0) {
        ui32 value = fallback;
        for (size_t i = 0; i != 4 && Pos < Size; ++i) {
            value = (value << 8) | Data[Pos++];
        }
        return mod ? value % mod : value;
    }

private:
    const ui8* Data = nullptr;
    size_t Size = 0;
    size_t Pos = 0;
};

TString MakeValue(ui32 x) {
    const size_t size = 1 + x % 16;
    return TString(size, char('a' + x % 26));
}

struct TPageMapPage {
    ui32 PageId = 0;

    explicit TPageMapPage(ui32 pageId)
        : PageId(pageId)
    {}
};

void CheckPageMap(const NKikimr::TPageMap<THolder<TPageMapPage>>& map, const TVector<bool>& present) {
    Y_ABORT_UNLESS(map.size() == present.size());

    size_t used = 0;
    TVector<bool> iterated(present.size(), false);
    for (ui32 page = 0; page < present.size(); ++page) {
        const bool hasPage = bool(map[page]);
        Y_ABORT_UNLESS(hasPage == present[page]);
        if (hasPage) {
            Y_ABORT_UNLESS(map[page]->PageId == page);
            ++used;
        }
    }

    size_t iterCount = 0;
    for (const auto& item : map) {
        Y_ABORT_UNLESS(item.first < present.size());
        Y_ABORT_UNLESS(present[item.first]);
        Y_ABORT_UNLESS(!iterated[item.first]);
        Y_ABORT_UNLESS(item.second);
        Y_ABORT_UNLESS(item.second->PageId == item.first);
        iterated[item.first] = true;
        ++iterCount;
    }

    Y_ABORT_UNLESS(map.used() == used);
    Y_ABORT_UNLESS(iterCount == used);
}

void FuzzPageMap(TInput& in) {
    NKikimr::TPageMap<THolder<TPageMapPage>> map;
    TVector<bool> present;

    auto ensureSize = [&](size_t size) {
        if (size > map.size()) {
            map.resize(size);
            present.resize(size, false);
        }
    };

    ensureSize(1 + in.Int(128));

    for (size_t step = 0; step != 128 && !in.Empty(); ++step) {
        const ui32 op = in.Byte() % 8;
        if (op == 0) {
            ensureSize(map.size() + 1 + in.Int(256));
            CheckPageMap(map, present);
            continue;
        }

        if (map.size() == 0) {
            ensureSize(1);
        }

        const ui32 page = in.Int(map.size());
        switch (op) {
            case 1: {
                const bool expected = !present[page];
                const bool inserted = map.emplace(page, MakeHolder<TPageMapPage>(page));
                Y_ABORT_UNLESS(inserted == expected);
                present[page] = true;
                break;
            }
            case 2: {
                const bool erased = map.erase(page);
                Y_ABORT_UNLESS(erased == present[page]);
                present[page] = false;
                break;
            }
            case 3:
                Y_ABORT_UNLESS(bool(map[page]) == present[page]);
                if (present[page]) {
                    Y_ABORT_UNLESS(map[page]->PageId == page);
                }
                break;
            case 4:
                map.clear();
                std::fill(present.begin(), present.end(), false);
                break;
            default:
                CheckPageMap(map, present);
                break;
        }

        if ((step & 7) == 0) {
            CheckPageMap(map, present);
        }
    }
}

void FuzzCoreCaches(TInput& in) {
    using namespace NKikimr::NCache;

    TLruCache<ui32, TString> lru([](const ui32&, const TString& value) { return std::max<ui64>(1, value.size()); });
    TUnboundedCacheOnHash<ui32, TString> unbounded;
    TIntrusivePtr<T2QCacheConfig> config(new T2QCacheConfig);
    config->InSizeRatio = 0.10 + double(in.Byte() % 80) / 100.0;
    config->OutKeyRatio = 0.10 + double(in.Byte() % 80) / 100.0;
    T2QCache<ui32, TString> twoQ(config, [](const ui32&, const TString& value) { return std::max<ui64>(1, value.size()); });

    ui64 evictions = 0;
    ui64 keyEvictions = 0;
    ui64 maxSize = 16;
    auto overflow = [&](const ICache<ui32, TString>& cache) {
        return cache.GetUsedSize() >= maxSize;
    };
    auto onEvict = [&](const ui32&, TString&, ui64) {
        ++evictions;
    };
    auto onKeyEvict = [&](const ui32&) {
        ++keyEvictions;
    };

    lru.SetOverflowCallback(overflow);
    twoQ.SetOverflowCallback(overflow);
    lru.SetEvictionCallback(onEvict);
    twoQ.SetEvictionCallback(onEvict);
    twoQ.SetKeyEvictionCallback(onKeyEvict);

    for (size_t step = 0; step != 96 && !in.Empty(); ++step) {
        const ui32 op = in.Byte() % 11;
        const ui32 key = in.Int(32);
        const TString value = MakeValue(in.Int(64));
        TString* ptr = nullptr;

        switch (op) {
            case 0:
                (void)lru.Insert(key, value, ptr);
                (void)twoQ.Insert(key, value, ptr);
                (void)unbounded.Insert(key, value, ptr);
                break;
            case 1:
                (void)lru.Find(key, ptr);
                (void)twoQ.Find(key, ptr);
                (void)unbounded.Find(key, ptr);
                break;
            case 2:
                (void)lru.FindWithoutPromote(key, ptr);
                (void)twoQ.FindWithoutPromote(key, ptr);
                (void)unbounded.FindWithoutPromote(key, ptr);
                break;
            case 3:
                (void)lru.Erase(key);
                (void)twoQ.Erase(key);
                (void)unbounded.Erase(key);
                break;
            case 4:
                (void)lru.Pop();
                (void)twoQ.Pop();
                (void)unbounded.Pop();
                break;
            case 5:
                lru.PopWhileOverflow();
                twoQ.PopWhileOverflow();
                break;
            case 6:
                maxSize = 1 + in.Int(64);
                break;
            case 7:
                lru.Clear();
                twoQ.Clear();
                unbounded.Clear();
                break;
            case 8:
                lru.ClearStatistics();
                twoQ.ClearStatistics();
                unbounded.ClearStatistics();
                break;
            default:
                (void)lru.GetStatistics();
                (void)twoQ.GetStatistics();
                (void)unbounded.GetStatistics();
                break;
        }

        Y_ABORT_UNLESS(lru.GetCount() <= 64);
        Y_ABORT_UNLESS(twoQ.GetCount() <= 64);
        Y_ABORT_UNLESS(unbounded.GetCount() <= 32);
    }

    Y_UNUSED(evictions);
    Y_UNUSED(keyEvictions);
}

struct TReleaseAwareValue {
    ui32 Value = 0;
    bool Safe = true;

    bool IsSafeToRelease() const {
        return Safe;
    }
};

void FuzzSimpleCaches(TInput& in) {
    NKikimr::TSimpleCache<ui32, ui32> simple;
    NKikimr::TNotSoSimpleCache<ui32, TReleaseAwareValue> guarded;
    simple.MaxSize = 1 + in.Int(16);
    guarded.MaxSize = 1 + in.Int(16);

    for (size_t step = 0; step != 96 && !in.Empty(); ++step) {
        const ui32 op = in.Byte() % 6;
        const ui32 key = in.Int(32);
        switch (op) {
            case 0:
                simple.Update(key, in.Int(1000));
                guarded.Update(key, TReleaseAwareValue{in.Int(1000), bool(in.Byte() & 1)});
                break;
            case 1:
                (void)simple.FindPtr(key);
                (void)guarded.FindPtr(key);
                break;
            case 2:
                simple.Erase(key);
                guarded.Erase(key);
                break;
            case 3:
                simple.MaxSize = 1 + in.Int(16);
                guarded.MaxSize = 1 + in.Int(16);
                simple.Update(key, in.Int(1000));
                guarded.Update(key, TReleaseAwareValue{in.Int(1000), true});
                break;
            default:
                break;
        }
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput coreInput(data, size);
    FuzzCoreCaches(coreInput);

    TInput simpleInput(data, size);
    FuzzSimpleCaches(simpleInput);

    TInput pageMapInput(data, size);
    FuzzPageMap(pageMapInput);

    return 0;
}
