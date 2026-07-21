#include <library/cpp/containers/concurrent_hash/concurrent_hash.h>
#include <library/cpp/containers/dense_hash/dense_hash.h>
#include <library/cpp/yt/containers/expiring_set.h>
#include <library/cpp/yt/containers/sharded_set.h>

#include <util/datetime/base.h>
#include <util/generic/hash.h>
#include <util/generic/vector.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <unordered_map>
#include <unordered_set>
#include <vector>

namespace {

class TInput {
public:
    TInput(const ui8* data, size_t size)
        : Data_(data)
        , Size_(size)
    {
    }

    bool Empty() const {
        return Pos_ >= Size_;
    }

    ui8 Byte() {
        if (Empty()) {
            return 0;
        }
        return Data_[Pos_++];
    }

    ui32 Int() {
        ui32 value = 0;
        for (int i = 0; i < 4; ++i) {
            value = (value << 8) | Byte();
        }
        return value;
    }

private:
    const ui8* Data_;
    size_t Size_;
    size_t Pos_ = 0;
};

ui32 Key(TInput& input, ui32 modulo = 128) {
    return 1 + input.Int() % modulo;
}

ui32 Value(TInput& input) {
    return input.Int();
}

template <class TMap>
std::unordered_map<ui32, ui32> SnapshotDenseMap(const TMap& map) {
    std::unordered_map<ui32, ui32> snapshot;
    for (const auto& [key, value] : map) {
        Y_ABORT_UNLESS(key != 0);
        Y_ABORT_UNLESS(snapshot.emplace(key, value).second);
    }
    return snapshot;
}

void CheckDenseMap(const TDenseHash<ui32, ui32, THash<ui32>, 50, 3>& map, const std::unordered_map<ui32, ui32>& model) {
    Y_ABORT_UNLESS(map.Size() == model.size());
    Y_ABORT_UNLESS(map.Empty() == model.empty());
    Y_ABORT_UNLESS(map.size() == model.size());
    Y_ABORT_UNLESS(map.empty() == model.empty());

    const auto snapshot = SnapshotDenseMap(map);
    Y_ABORT_UNLESS(snapshot == model);

    for (ui32 key = 1; key <= 128; ++key) {
        const bool expected = model.contains(key);
        Y_ABORT_UNLESS(map.Has(key) == expected);
        Y_ABORT_UNLESS(map.contains(key) == expected);
        auto it = map.find(key);
        if (expected) {
            Y_ABORT_UNLESS(it != map.end());
            Y_ABORT_UNLESS(it->first == key);
            Y_ABORT_UNLESS(it->second == model.at(key));
            Y_ABORT_UNLESS(map.at(key) == model.at(key));
        } else {
            Y_ABORT_UNLESS(it == map.end());
        }
    }
}

std::unordered_set<ui32> SnapshotDenseSet(const TDenseHashSet<ui32, THash<ui32>, 50, 3>& set) {
    std::unordered_set<ui32> snapshot;
    for (ui32 key : set) {
        Y_ABORT_UNLESS(key != 0);
        Y_ABORT_UNLESS(snapshot.insert(key).second);
    }
    return snapshot;
}

void CheckDenseSet(const TDenseHashSet<ui32, THash<ui32>, 50, 3>& set, const std::unordered_set<ui32>& model) {
    Y_ABORT_UNLESS(set.Size() == model.size());
    Y_ABORT_UNLESS(set.Empty() == model.empty());
    Y_ABORT_UNLESS(SnapshotDenseSet(set) == model);

    for (ui32 key = 1; key <= 128; ++key) {
        Y_ABORT_UNLESS(set.Has(key) == model.contains(key));
    }
}

void RunDenseHash(TInput& input) {
    TDenseHash<ui32, ui32, THash<ui32>, 50, 3> map(0, 8);
    TDenseHashSet<ui32, THash<ui32>, 50, 3> set(0, 8);
    std::unordered_map<ui32, ui32> mapModel;
    std::unordered_set<ui32> setModel;

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const ui32 key = Key(input);
        const ui32 value = Value(input);

        switch (op % 12) {
            case 0: {
                const auto result = map.insert(std::make_pair(key, value));
                const auto expected = mapModel.emplace(key, value);
                Y_ABORT_UNLESS(result.second == expected.second);
                Y_ABORT_UNLESS(result.first->first == key);
                Y_ABORT_UNLESS(result.first->second == mapModel.at(key));
                break;
            }
            case 1:
                map[key] = value;
                mapModel[key] = value;
                break;
            case 2:
                if (mapModel.contains(key)) {
                    map.at(key) ^= value;
                    mapModel[key] ^= value;
                }
                break;
            case 3:
                map.Clear();
                mapModel.clear();
                break;
            case 4:
                map.MakeEmpty(1 + (value % 128));
                mapModel.clear();
                break;
            case 5:
                map.ReserveSpace(value % 160);
                break;
            case 6:
                map.Grow(1 + (value % 256));
                break;
            case 7: {
                const bool inserted = set.Insert(key);
                const bool expected = setModel.insert(key).second;
                Y_ABORT_UNLESS(inserted == expected);
                break;
            }
            case 8:
                set.Clear();
                setModel.clear();
                break;
            case 9:
                set.MakeEmpty(1 + (value % 128));
                setModel.clear();
                break;
            case 10: {
                TDenseHash<ui32, ui32, THash<ui32>, 50, 3> copy = map;
                Y_ABORT_UNLESS(copy == map);
                auto copySnapshot = SnapshotDenseMap(copy);
                Y_ABORT_UNLESS(copySnapshot == mapModel);
                break;
            }
            case 11: {
                size_t count = 0;
                for (auto it = set.begin(); it != set.end(); ++it) {
                    Y_ABORT_UNLESS(setModel.contains(*it));
                    ++count;
                }
                Y_ABORT_UNLESS(count == setModel.size());
                break;
            }
        }

        CheckDenseMap(map, mapModel);
        CheckDenseSet(set, setModel);
    }
}

template <class TMap>
std::unordered_map<ui32, ui32> SnapshotConcurrentMap(const TMap& map) {
    std::unordered_map<ui32, ui32> snapshot;
    for (const auto& bucket : map.Buckets) {
        for (const auto& [key, value] : bucket.GetMap()) {
            Y_ABORT_UNLESS(snapshot.emplace(key, value).second);
        }
    }
    return snapshot;
}

void CheckConcurrentMap(const TConcurrentHashMap<ui32, ui32, 8>& map, const std::unordered_map<ui32, ui32>& model) {
    Y_ABORT_UNLESS(map.ApproximateSize() == model.size());
    Y_ABORT_UNLESS(SnapshotConcurrentMap(map) == model);

    for (ui32 key = 1; key <= 128; ++key) {
        const bool expected = model.contains(key);
        Y_ABORT_UNLESS(map.Has(key) == expected);

        ui32 result = 0xA5A5A5A5u;
        Y_ABORT_UNLESS(map.Get(key, result) == expected);
        if (expected) {
            Y_ABORT_UNLESS(result == model.at(key));
            Y_ABORT_UNLESS(map.Get(key) == model.at(key));
        } else {
            Y_ABORT_UNLESS(result == 0xA5A5A5A5u);
        }
    }
}

void RunConcurrentHash(TInput& input) {
    TConcurrentHashMap<ui32, ui32, 8> map;
    std::unordered_map<ui32, ui32> model;

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const ui32 key = Key(input);
        const ui32 value = Value(input);

        switch (op % 10) {
            case 0:
                map.Insert(key, value);
                model[key] = value;
                break;
            case 1: {
                const ui32 actual = map.InsertIfAbsent(key, value);
                const auto [it, inserted] = model.emplace(key, value);
                (void)inserted;
                Y_ABORT_UNLESS(actual == it->second);
                break;
            }
            case 2: {
                bool initialized = false;
                const ui32 actual = map.InsertIfAbsentWithInit(key, [&] {
                    initialized = true;
                    return value;
                });
                const bool hadKey = model.contains(key);
                const auto [it, inserted] = model.emplace(key, value);
                (void)inserted;
                Y_ABORT_UNLESS(initialized != hadKey);
                Y_ABORT_UNLESS(actual == it->second);
                break;
            }
            case 3: {
                const ui32 actual = map.EmplaceIfAbsent(key, value);
                const auto [it, inserted] = model.emplace(key, value);
                (void)inserted;
                Y_ABORT_UNLESS(actual == it->second);
                break;
            }
            case 4:
                if (!model.contains(key)) {
                    map.InsertUnique(key, value);
                    model.emplace(key, value);
                }
                break;
            case 5: {
                ui32 exchanged = value;
                const ui32 old = model.contains(key) ? model[key] : 0;
                map.Exchange(key, exchanged);
                model[key] = value;
                Y_ABORT_UNLESS(exchanged == old);
                break;
            }
            case 6:
                if (model.contains(key)) {
                    const ui32 removed = map.Remove(key);
                    Y_ABORT_UNLESS(removed == model.at(key));
                    model.erase(key);
                }
                break;
            case 7: {
                ui32 removed = 0xA5A5A5A5u;
                const bool actual = map.TryRemove(key, removed);
                const auto it = model.find(key);
                Y_ABORT_UNLESS(actual == (it != model.end()));
                if (it != model.end()) {
                    Y_ABORT_UNLESS(removed == it->second);
                    model.erase(it);
                } else {
                    Y_ABORT_UNLESS(removed == 0xA5A5A5A5u);
                }
                break;
            }
            case 8: {
                const ui32 mask = value & 15;
                map.Retain([&] (const auto& item) {
                    return (item.first & 15) == mask;
                });
                for (auto it = model.begin(); it != model.end();) {
                    if ((it->first & 15) == mask) {
                        ++it;
                    } else {
                        it = model.erase(it);
                    }
                }
                break;
            }
            case 9:
                map.Retain([] (const auto&) {
                    return false;
                });
                model.clear();
                break;
        }

        CheckConcurrentMap(map, model);
    }
}

struct TIntToShard {
    int operator()(ui32 value) const {
        return value % 16;
    }
};

using TShardedHashSet = NYT::TShardedSet<ui32, 16, TIntToShard>;

void CheckShardedSet(const TShardedHashSet& set, const std::unordered_set<ui32>& model) {
    Y_ABORT_UNLESS(set.size() == model.size());
    Y_ABORT_UNLESS(set.empty() == model.empty());

    std::unordered_set<ui32> snapshot;
    for (ui32 value : set) {
        Y_ABORT_UNLESS(model.contains(value));
        Y_ABORT_UNLESS(snapshot.insert(value).second);
    }
    Y_ABORT_UNLESS(snapshot == model);

    std::array<std::unordered_set<ui32>, 16> modelShards;
    for (ui32 value : model) {
        modelShards[value % 16].insert(value);
    }
    for (int shard = 0; shard < 16; ++shard) {
        const auto& actualShard = set.Shard(shard);
        Y_ABORT_UNLESS(actualShard.size() == modelShards[shard].size());
        for (ui32 value : modelShards[shard]) {
            Y_ABORT_UNLESS(actualShard.contains(value));
        }
    }

    for (ui32 key = 1; key <= 128; ++key) {
        Y_ABORT_UNLESS(set.contains(key) == model.contains(key));
        Y_ABORT_UNLESS(set.count(key) == (model.contains(key) ? 1 : 0));
    }

    if (!model.empty()) {
        Y_ABORT_UNLESS(model.contains(set.front()));
    }
}

void RunShardedSet(TInput& input) {
    TShardedHashSet set;
    std::unordered_set<ui32> model;

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const ui32 key = Key(input);
        const ui32 value = Value(input);

        switch (op % 7) {
            case 0: {
                const auto [it, inserted] = set.insert(key);
                const bool expected = model.insert(key).second;
                Y_ABORT_UNLESS(inserted == expected);
                Y_ABORT_UNLESS(*it == key);
                break;
            }
            case 1: {
                const bool actual = set.erase(key);
                const bool expected = model.erase(key) != 0;
                Y_ABORT_UNLESS(actual == expected);
                break;
            }
            case 2:
                set.clear();
                model.clear();
                break;
            case 3:
                set.MutableShard(value % 16).reserve(1 + (value % 128));
                break;
            case 4:
                for (ui32 current = 1; current <= 128; ++current) {
                    if ((current & 7) == (value & 7)) {
                        set.insert(current);
                        model.insert(current);
                    }
                }
                break;
            case 5:
                for (ui32 current = 1; current <= 128; ++current) {
                    if ((current & 7) != (value & 7)) {
                        set.erase(current);
                        model.erase(current);
                    }
                }
                break;
            case 6: {
                size_t count = 0;
                for (auto it = set.cbegin(); it != set.cend(); ++it) {
                    Y_ABORT_UNLESS(model.contains(*it));
                    ++count;
                }
                Y_ABORT_UNLESS(count == model.size());
                break;
            }
        }

        CheckShardedSet(set, model);
    }
}

TInstant Seconds(ui64 seconds) {
    return TInstant::Zero() + TDuration::Seconds(seconds);
}

void ExpireModel(std::unordered_map<ui32, ui64>& model, ui64 now) {
    for (auto it = model.begin(); it != model.end();) {
        if (it->second <= now) {
            it = model.erase(it);
        } else {
            ++it;
        }
    }
}

void CheckExpiringSet(const NYT::TExpiringSet<ui32>& set, const std::unordered_map<ui32, ui64>& model) {
    Y_ABORT_UNLESS(set.GetSize() == static_cast<int>(model.size()));
    for (ui32 key = 1; key <= 128; ++key) {
        Y_ABORT_UNLESS(set.Contains(key) == model.contains(key));
    }
}

void RunExpiringSet(TInput& input) {
    NYT::TExpiringSet<ui32> set;
    std::unordered_map<ui32, ui64> model;

    ui64 now = 0;
    ui64 ttl = 1 + (input.Byte() % 16);
    set.SetTtl(TDuration::Seconds(ttl));

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        now += input.Byte() % 4;
        const ui32 key = Key(input);
        const ui32 value = Value(input);

        switch (op % 8) {
            case 0:
                ttl = value % 32;
                set.SetTtl(TDuration::Seconds(ttl));
                break;
            case 1:
                set.Insert(Seconds(now), key);
                ExpireModel(model, now);
                model[key] = now + ttl;
                break;
            case 2: {
                TVector<ui32> items;
                const size_t count = 1 + (value % 8);
                items.reserve(count);
                for (size_t index = 0; index < count; ++index) {
                    items.push_back(1 + ((key + index * 17 + value) % 128));
                }
                set.InsertMany(Seconds(now), items);
                ExpireModel(model, now);
                for (ui32 item : items) {
                    model[item] = now + ttl;
                }
                break;
            }
            case 3:
                set.Remove(key);
                model.erase(key);
                break;
            case 4:
                set.Expire(Seconds(now));
                ExpireModel(model, now);
                break;
            case 5:
                set.Clear();
                model.clear();
                break;
            case 6:
                now += value % 32;
                set.Expire(Seconds(now));
                ExpireModel(model, now);
                break;
            case 7:
                Y_ABORT_UNLESS(set.Contains(key) == model.contains(key));
                break;
        }

        CheckExpiringSet(set, model);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput input(data, size);
    if (input.Empty()) {
        return 0;
    }

    switch (input.Byte() % 4) {
        case 0:
            RunDenseHash(input);
            break;
        case 1:
            RunConcurrentHash(input);
            break;
        case 2:
            RunShardedSet(input);
            break;
        case 3:
            RunExpiringSet(input);
            break;
    }

    return 0;
}
