#include <library/cpp/cache/cache.h>
#include <library/cpp/cache/thread_safe_cache.h>

#include <util/generic/string.h>
#include <util/system/types.h>

#include <algorithm>
#include <array>
#include <atomic>
#include <condition_variable>
#include <memory>
#include <mutex>
#include <thread>

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

struct TValueSize {
    size_t operator()(const TString& value) const {
        return std::max<size_t>(1, value.size());
    }
};

struct TWeightByLength {
    static ui32 Weight(const TString& value) {
        return value.size();
    }
};

TString MakeValue(ui32 x) {
    const size_t size = 1 + x % 16;
    return TString(size, char('a' + x % 26));
}

void FuzzLibraryCaches(TInput& in) {
    TLRUCache<ui32, TString, TNoopDelete, TValueSize> lru(8, false, TValueSize());
    TLFUCache<ui32, TString, TNoopDelete, std::allocator<typename TLFUList<ui32, TString, TValueSize>::TItem>, TValueSize>
        lfu(8, false, TValueSize());
    TLWCache<ui32, TString, ui32, TWeightByLength> lw(8);

    for (size_t step = 0; step != 96 && !in.Empty(); ++step) {
        const ui32 op = in.Byte() % 12;
        const ui32 key = in.Int(32);
        const TString value = MakeValue(in.Int(64));

        switch (op) {
            case 0:
                lru.Insert(key, value);
                lfu.Insert(key, value);
                lw.Insert(key, value);
                break;
            case 1:
                lru.Update(key, value);
                lfu.Update(key, value);
                break;
            case 2:
                (void)lru.Find(key);
                (void)lfu.Find(key);
                (void)lw.Find(key);
                break;
            case 3:
                (void)lru.FindWithoutPromote(key);
                (void)lfu.FindWithoutPromote(key);
                (void)lw.FindWithoutPromote(key);
                break;
            case 4:
                if (auto it = lru.Find(key); it != lru.End()) {
                    lru.Erase(it);
                }
                if (auto it = lfu.Find(key); it != lfu.End()) {
                    lfu.Erase(it);
                }
                if (auto it = lw.Find(key); it != lw.End()) {
                    lw.Erase(it);
                }
                break;
            case 5: {
                TString tmp;
                (void)lru.PickOut(key, &tmp);
                (void)lfu.PickOut(key, &tmp);
                (void)lw.PickOut(key, &tmp);
                break;
            }
            case 6: {
                const size_t maxSize = 1 + in.Int(24);
                lru.SetMaxSize(maxSize);
                lfu.SetMaxSize(maxSize);
                lw.SetMaxSize(maxSize);
                break;
            }
            case 7:
                lru.Reserve(1 + in.Int(64));
                lfu.Reserve(1 + in.Int(64));
                lw.Reserve(1 + in.Int(64));
                break;
            case 8:
                if (!lru.Empty()) {
                    (void)lru.GetOldest();
                    (void)lru.FindOldest();
                }
                if (!lfu.Empty()) {
                    (void)lfu.GetLeastFrequentlyUsed();
                    (void)lfu.FindLeastFrequentlyUsed();
                }
                if (!lw.Empty()) {
                    (void)lw.GetLightest();
                    (void)lw.FindLightest();
                }
                break;
            case 9:
                lru.Clear();
                lfu.Clear();
                lw.Clear();
                break;
            default:
                Y_ABORT_UNLESS(lru.Size() <= std::max<size_t>(1, lru.GetMaxSize()));
                Y_ABORT_UNLESS(lfu.Size() <= std::max<size_t>(1, lfu.GetMaxSize()));
                Y_ABORT_UNLESS(lw.Size() <= std::max<size_t>(1, lw.GetMaxSize()));
                break;
        }
    }
}

struct TStringLengthSizeProvider {
    size_t operator()(const TString& value) const {
        return std::max<size_t>(1, value.size());
    }
};

template <class TCache>
class TThreadCacheCallbacks final : public TCache::ICallbacks {
public:
    typename TCache::ICallbacks::TKey GetKey(ui32 key) const override {
        return key % 16;
    }

    typename TCache::ICallbacks::TValue* CreateObject(ui32 key) const override {
        ++Creations;
        return new TString(MakeValue(key));
    }

    mutable std::atomic<ui32> Creations = 0;
};

struct TThreadCacheOp {
    ui32 Op = 0;
    ui32 Key = 0;
    TString Value;
    size_t MaxSize = 1;
};

template <class TCache>
void ApplyThreadCacheOp(TCache& cache, const TThreadCacheOp& op) {
    const ui32 key = op.Key % 16;
    switch (op.Op % 9) {
        case 0:
            (void)cache.Insert(key, MakeAtomicShared<TString>(op.Value));
            break;
        case 1:
            cache.Update(key, MakeAtomicShared<TString>(op.Value));
            break;
        case 2:
            (void)cache.Get(key);
            break;
        case 3:
            (void)cache.GetUnsafe(key);
            break;
        case 4:
            (void)cache.GetOrNull(key);
            break;
        case 5:
            cache.Erase(key);
            break;
        case 6:
            (void)cache.Contains(key);
            break;
        case 7:
            cache.SetMaxSize(op.MaxSize);
            break;
        default:
            cache.Clear();
            break;
    }

    Y_ABORT_UNLESS(cache.Size() <= 16);
    Y_ABORT_UNLESS(cache.TotalSize() <= 16 * 16);
}

template <class TCache>
void RunDeterministicThreadScript(TCache& cache, TInput& in) {
    constexpr size_t ThreadCount = 2;
    constexpr size_t Steps = 24;
    std::array<std::array<TThreadCacheOp, Steps>, ThreadCount> ops;
    for (auto& threadOps : ops) {
        for (auto& op : threadOps) {
            op.Op = in.Byte();
            op.Key = in.Int(16);
            op.Value = MakeValue(in.Int(64));
            op.MaxSize = 1 + in.Int(24);
        }
    }

    std::mutex mutex;
    std::condition_variable cv;
    size_t turn = 0;

    auto worker = [&](size_t workerId) {
        for (size_t step = 0; step != Steps; ++step) {
            const size_t myTurn = step * ThreadCount + workerId;
            {
                std::unique_lock<std::mutex> lock(mutex);
                cv.wait(lock, [&] { return turn == myTurn; });
            }

            ApplyThreadCacheOp(cache, ops[workerId][step]);

            {
                std::lock_guard<std::mutex> lock(mutex);
                ++turn;
            }
            cv.notify_all();
        }
    };

    std::thread first(worker, 0);
    std::thread second(worker, 1);
    first.join();
    second.join();
}

void FuzzThreadSafeCaches(TInput& in) {
    {
        using TCache = TThreadSafeLRUCache<ui32, TString, ui32>;
        TThreadCacheCallbacks<TCache> callbacks;
        TCache cache(callbacks, 1 + in.Int(8));
        RunDeterministicThreadScript(cache, in);
    }
    {
        using TCache = TThreadSafeLRUCacheWithSizeProvider<ui32, TString, TStringLengthSizeProvider, ui32>;
        TThreadCacheCallbacks<TCache> callbacks;
        TCache cache(callbacks, 1 + in.Int(32));
        RunDeterministicThreadScript(cache, in);
    }
    {
        using TCache = TThreadSafeLFUCacheWithSizeProvider<ui32, TString, TStringLengthSizeProvider, ui32>;
        TThreadCacheCallbacks<TCache> callbacks;
        TCache cache(callbacks, 1 + in.Int(32));
        RunDeterministicThreadScript(cache, in);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput libraryInput(data, size);
    FuzzLibraryCaches(libraryInput);

    TInput threadInput(data, size);
    FuzzThreadSafeCaches(threadInput);
    return 0;
}
