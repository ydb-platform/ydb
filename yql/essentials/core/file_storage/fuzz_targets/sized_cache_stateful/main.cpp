#include <yql/essentials/core/file_storage/sized_cache.h>

#include <util/generic/string.h>
#include <util/generic/vector.h>
#include <util/system/types.h>

#include <algorithm>
#include <array>
#include <utility>

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

TString SizedName(ui32 key) {
    return TString("k") + char('a' + key);
}

class TSizedCacheObj final : public NYql::TSizedCache::ICacheObj {
public:
    TSizedCacheObj(TString name, ui64 size, ui32 slot, std::array<ui32, 16>& dismissed)
        : Name_(std::move(name))
        , Size_(size)
        , Slot_(slot)
        , Dismissed_(dismissed)
    {}

    TString GetName() override {
        return Name_;
    }

    ui64 GetSize() override {
        return Size_;
    }

    void Dismiss() override {
        ++Dismissed_[Slot_];
    }

private:
    TString Name_;
    ui64 Size_ = 0;
    ui32 Slot_ = 0;
    std::array<ui32, 16>& Dismissed_;
};

struct TSizedModelEntry {
    bool Present = false;
    ui32 Locks = 0;
    ui64 Size = 0;
};

struct TSizedModel {
    std::array<TSizedModelEntry, 16> Entries;
    TVector<ui32> Order;
    size_t Count = 0;
    ui64 Occupied = 0;
    size_t MaxEntries = 1;
    ui64 MaxSize = 1;
    ui64 ExpectedDismisses = 0;

    void Promote(ui32 key) {
        Order.erase(std::remove(Order.begin(), Order.end(), key), Order.end());
        Order.push_back(key);
    }

    void Remove(ui32 key, bool dismiss) {
        auto& entry = Entries[key];
        Y_ABORT_UNLESS(entry.Present);
        entry.Present = false;
        entry.Locks = 0;
        Y_ABORT_UNLESS(Occupied >= entry.Size);
        Occupied -= entry.Size;
        entry.Size = 0;
        --Count;
        Order.erase(std::remove(Order.begin(), Order.end(), key), Order.end());
        if (dismiss) {
            ++ExpectedDismisses;
        }
    }

    bool OverLimit() const {
        return Count > MaxEntries || Occupied > MaxSize;
    }

    void EvictOldestIfNeeded() {
        while (OverLimit()) {
            if (Order.empty()) {
                break;
            }
            const ui32 oldest = Order.front();
            Y_ABORT_UNLESS(Entries[oldest].Present);
            if (Entries[oldest].Locks != 0) {
                break;
            }
            Remove(oldest, true);
        }
    }

    void Put(ui32 key, ui64 size, bool lock) {
        auto& entry = Entries[key];
        if (!entry.Present) {
            entry.Present = true;
            entry.Locks = lock ? 1 : 0;
            entry.Size = size;
            ++Count;
            Occupied += size;
            Order.push_back(key);
        } else {
            Promote(key);
            if (lock) {
                ++entry.Locks;
            }
        }
        EvictOldestIfNeeded();
    }

    void Release(ui32 key, bool remove) {
        auto& entry = Entries[key];
        Y_ABORT_UNLESS(entry.Present);
        Y_ABORT_UNLESS(entry.Locks > 0);
        --entry.Locks;
        if (!entry.Locks && (remove || OverLimit())) {
            Remove(key, true);
        }
    }

    void Get(ui32 key) {
        if (Entries[key].Present) {
            Promote(key);
        }
    }

    void Revoke(ui32 key) {
        if (Entries[key].Present) {
            Remove(key, false);
        }
    }
};

void CheckSizedCache(const NYql::TSizedCache& cache, const TSizedModel& model, const std::array<ui32, 16>& dismissed) {
    Y_ABORT_UNLESS(cache.GetCount() == model.Count);
    Y_ABORT_UNLESS(cache.GetOccupiedSize() == model.Occupied);

    ui64 actualDismisses = 0;
    for (ui32 key = 0; key < model.Entries.size(); ++key) {
        actualDismisses += dismissed[key];
        const auto locks = cache.GetLocks(SizedName(key));
        if (model.Entries[key].Present) {
            Y_ABORT_UNLESS(locks.Defined());
            Y_ABORT_UNLESS(*locks == model.Entries[key].Locks);
        } else {
            Y_ABORT_UNLESS(!locks.Defined());
        }
    }
    Y_ABORT_UNLESS(actualDismisses == model.ExpectedDismisses);
}

void FuzzSizedCache(TInput& in) {
    TSizedModel model;
    model.MaxEntries = 1 + in.Int(8);
    model.MaxSize = 1 + in.Int(64);
    NYql::TSizedCache cache(model.MaxEntries, model.MaxSize);
    std::array<ui32, 16> dismissed = {};

    for (size_t step = 0; step != 128 && !in.Empty(); ++step) {
        const ui32 op = in.Byte() % 7;
        const ui32 key = in.Int(16);
        switch (op) {
            case 0: {
                const ui64 size = 1 + in.Int(32);
                const bool lock = in.Byte() & 1;
                TIntrusivePtr<NYql::TSizedCache::ICacheObj> obj = new TSizedCacheObj(SizedName(key), size, key, dismissed);
                cache.Put(obj, lock);
                model.Put(key, size, lock);
                break;
            }
            case 1:
                if (model.Entries[key].Present && model.Entries[key].Locks) {
                    const bool remove = in.Byte() & 1;
                    cache.Release(SizedName(key), remove);
                    model.Release(key, remove);
                }
                break;
            case 2:
                (void)cache.Get(SizedName(key));
                model.Get(key);
                break;
            case 3:
                (void)cache.Revoke(SizedName(key));
                model.Revoke(key);
                break;
            case 4:
                (void)cache.GetLocks(SizedName(key));
                break;
            default:
                break;
        }

        CheckSizedCache(cache, model, dismissed);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput sizedCacheInput(data, size);
    FuzzSizedCache(sizedCacheInput);
    return 0;
}
