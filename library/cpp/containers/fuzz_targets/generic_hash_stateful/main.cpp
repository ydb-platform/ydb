#include <util/generic/hash.h>
#include <util/generic/hash_set.h>
#include <util/system/types.h>
#include <util/system/yassert.h>

#include <algorithm>
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

struct TCollisionHash {
    size_t operator()(ui32 value) const noexcept {
        return value & 3;
    }
};

ui32 Key(TInput& input) {
    return input.Int() % 96;
}

ui32 Value(TInput& input) {
    return input.Int();
}

template <class TMap>
std::unordered_map<ui32, ui32> SnapshotMap(const TMap& map) {
    std::unordered_map<ui32, ui32> snapshot;
    for (const auto& [key, value] : map) {
        Y_ABORT_UNLESS(snapshot.emplace(key, value).second);
    }
    return snapshot;
}

template <class TSet>
std::unordered_set<ui32> SnapshotSet(const TSet& set) {
    std::unordered_set<ui32> snapshot;
    for (ui32 value : set) {
        Y_ABORT_UNLESS(snapshot.insert(value).second);
    }
    return snapshot;
}

template <class TMap, class TModel>
void CheckMap(const TMap& map, const TModel& model) {
    Y_ABORT_UNLESS(map.size() == model.size());
    Y_ABORT_UNLESS(map.empty() == model.empty());
    Y_ABORT_UNLESS(static_cast<bool>(map) == !model.empty());
    const auto snapshot = SnapshotMap(map);
    Y_ABORT_UNLESS(snapshot.size() == model.size());
    for (const auto& [key, value] : model) {
        auto it = snapshot.find(key);
        Y_ABORT_UNLESS(it != snapshot.end());
        Y_ABORT_UNLESS(it->second == value);
    }

    size_t bucketItems = 0;
    for (size_t bucket = 0; bucket < map.bucket_count(); ++bucket) {
        bucketItems += map.bucket_size(bucket);
    }
    Y_ABORT_UNLESS(bucketItems == model.size());

    for (ui32 key = 0; key < 96; ++key) {
        const bool expected = model.contains(key);
        Y_ABORT_UNLESS(map.contains(key) == expected);
        Y_ABORT_UNLESS(map.count(key) == (expected ? 1 : 0));

        auto it = map.find(key);
        if (expected) {
            Y_ABORT_UNLESS(it != map.end());
            Y_ABORT_UNLESS(it->first == key);
            Y_ABORT_UNLESS(it->second == model.at(key));
            Y_ABORT_UNLESS(map.at(key) == model.at(key));
            auto [rangeBegin, rangeEnd] = map.equal_range(key);
            Y_ABORT_UNLESS(rangeBegin != rangeEnd);
            Y_ABORT_UNLESS(rangeBegin->first == key);
            ++rangeBegin;
            Y_ABORT_UNLESS(rangeBegin == rangeEnd);
        } else {
            Y_ABORT_UNLESS(it == map.end());
            auto [rangeBegin, rangeEnd] = map.equal_range(key);
            Y_ABORT_UNLESS(rangeBegin == rangeEnd);
        }
    }
}

template <class TSet, class TModel>
void CheckSet(const TSet& set, const TModel& model) {
    Y_ABORT_UNLESS(set.size() == model.size());
    Y_ABORT_UNLESS(set.empty() == model.empty());
    Y_ABORT_UNLESS(static_cast<bool>(set) == !model.empty());
    const auto snapshot = SnapshotSet(set);
    Y_ABORT_UNLESS(snapshot.size() == model.size());
    for (ui32 value : model) {
        Y_ABORT_UNLESS(snapshot.contains(value));
    }

    size_t bucketItems = 0;
    for (size_t bucket = 0; bucket < set.bucket_count(); ++bucket) {
        bucketItems += set.bucket_size(bucket);
    }
    Y_ABORT_UNLESS(bucketItems == model.size());

    for (ui32 key = 0; key < 96; ++key) {
        const bool expected = model.contains(key);
        Y_ABORT_UNLESS(set.contains(key) == expected);
        Y_ABORT_UNLESS(set.count(key) == (expected ? 1 : 0));

        auto it = set.find(key);
        if (expected) {
            Y_ABORT_UNLESS(it != set.end());
            Y_ABORT_UNLESS(*it == key);
            auto [rangeBegin, rangeEnd] = set.equal_range(key);
            Y_ABORT_UNLESS(rangeBegin != rangeEnd);
            Y_ABORT_UNLESS(*rangeBegin == key);
            ++rangeBegin;
            Y_ABORT_UNLESS(rangeBegin == rangeEnd);
        } else {
            Y_ABORT_UNLESS(it == set.end());
            auto [rangeBegin, rangeEnd] = set.equal_range(key);
            Y_ABORT_UNLESS(rangeBegin == rangeEnd);
        }
    }
}

template <class TMap, class TModel>
void CheckMapCopyMove(const TMap& map, const TModel& model) {
    TMap copy(map);
    CheckMap(copy, model);
    Y_ABORT_UNLESS(copy == map);

    TMap assigned;
    assigned = copy;
    CheckMap(assigned, model);

    TMap moved(std::move(copy));
    CheckMap(moved, model);

    TMap moveAssigned;
    moveAssigned = std::move(moved);
    CheckMap(moveAssigned, model);
}

template <class TSet, class TModel>
void CheckSetCopyMove(const TSet& set, const TModel& model) {
    TSet copy(set);
    CheckSet(copy, model);
    Y_ABORT_UNLESS(copy == set);

    TSet assigned;
    assigned = copy;
    CheckSet(assigned, model);

    TSet moved(std::move(copy));
    CheckSet(moved, model);

    TSet moveAssigned;
    moveAssigned = std::move(moved);
    CheckSet(moveAssigned, model);
}

template <class THash>
void RunMap(TInput& input) {
    using TMap = THashMap<ui32, ui32, THash>;
    TMap maps[2];
    std::unordered_map<ui32, ui32, THash> models[2];

    const size_t steps = std::min<size_t>(input.Byte() + 1, 224);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const size_t slot = input.Byte() & 1;
        TMap& map = maps[slot];
        auto& model = models[slot];
        const ui8 op = input.Byte();
        const ui32 key = Key(input);
        const ui32 value = Value(input);

        switch (op % 15) {
            case 0: {
                const auto actual = map.insert({key, value});
                const auto expected = model.emplace(key, value);
                Y_ABORT_UNLESS(actual.second == expected.second);
                Y_ABORT_UNLESS(actual.first->first == key);
                Y_ABORT_UNLESS(actual.first->second == expected.first->second);
                break;
            }
            case 1: {
                const auto actual = map.emplace(key, value);
                const auto expected = model.emplace(key, value);
                Y_ABORT_UNLESS(actual.second == expected.second);
                Y_ABORT_UNLESS(actual.first->second == expected.first->second);
                break;
            }
            case 2:
                map[key] = value;
                model[key] = value;
                break;
            case 3: {
                const auto actual = map.insert_or_assign(key, value);
                const auto it = model.find(key);
                const bool inserted = it == model.end();
                model[key] = value;
                Y_ABORT_UNLESS(actual.second == inserted);
                Y_ABORT_UNLESS(actual.first->second == value);
                break;
            }
            case 4: {
                const auto actual = map.try_emplace(key, value);
                const auto expected = model.emplace(key, value);
                Y_ABORT_UNLESS(actual.second == expected.second);
                Y_ABORT_UNLESS(actual.first->second == expected.first->second);
                break;
            }
            case 5: {
                const bool expected = model.erase(key) != 0;
                Y_ABORT_UNLESS(map.erase(key) == (expected ? 1 : 0));
                break;
            }
            case 6:
                if (auto it = map.find(key); it != map.end()) {
                    map.erase(it);
                    model.erase(key);
                }
                break;
            case 7:
                map.reserve(value % 160);
                break;
            case 8:
                map.clear(value % 80);
                model.clear();
                break;
            case 9:
                map.clear();
                model.clear();
                break;
            case 10:
                map.basic_clear();
                model.clear();
                break;
            case 11:
                CheckMapCopyMove(map, model);
                break;
            case 12: {
                maps[0].swap(maps[1]);
                models[0].swap(models[1]);
                break;
            }
            case 13: {
                typename TMap::insert_ctx ctx = nullptr;
                auto it = map.find(key, ctx);
                const bool existed = model.contains(key);
                Y_ABORT_UNLESS((it != map.end()) == existed);
                if (!existed && map.size() < 128) {
                    auto inserted = map.emplace_direct(ctx, key, value);
                    model.emplace(key, value);
                    Y_ABORT_UNLESS(inserted->first == key);
                }
                break;
            }
            case 14:
                if (model.contains(key)) {
                    map.at(key) ^= value;
                    model[key] ^= value;
                }
                break;
        }

        CheckMap(maps[0], models[0]);
        CheckMap(maps[1], models[1]);
    }
}

template <class THash>
void RunSet(TInput& input) {
    using TSet = THashSet<ui32, THash>;
    TSet sets[2];
    std::unordered_set<ui32, THash> models[2];

    const size_t steps = std::min<size_t>(input.Byte() + 1, 224);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const size_t slot = input.Byte() & 1;
        TSet& set = sets[slot];
        auto& model = models[slot];
        const ui8 op = input.Byte();
        const ui32 key = Key(input);
        const ui32 value = Value(input);

        switch (op % 14) {
            case 0: {
                const auto actual = set.insert(key);
                const auto expected = model.insert(key);
                Y_ABORT_UNLESS(actual.second == expected.second);
                Y_ABORT_UNLESS(*actual.first == key);
                break;
            }
            case 1: {
                const auto actual = set.emplace(key);
                const auto expected = model.emplace(key);
                Y_ABORT_UNLESS(actual.second == expected.second);
                Y_ABORT_UNLESS(*actual.first == key);
                break;
            }
            case 2: {
                const bool expected = model.erase(key) != 0;
                Y_ABORT_UNLESS(set.erase(key) == (expected ? 1 : 0));
                break;
            }
            case 3:
                if (auto it = set.find(key); it != set.end()) {
                    set.erase(it);
                    model.erase(key);
                }
                break;
            case 4:
                set.reserve(value % 160);
                break;
            case 5:
                set.clear(value % 80);
                model.clear();
                break;
            case 6:
                set.clear();
                model.clear();
                break;
            case 7:
                set.basic_clear();
                model.clear();
                break;
            case 8:
                CheckSetCopyMove(set, model);
                break;
            case 9:
                sets[0].swap(sets[1]);
                models[0].swap(models[1]);
                break;
            case 10: {
                typename TSet::insert_ctx ctx = nullptr;
                auto it = set.find(key, ctx);
                const bool existed = model.contains(key);
                Y_ABORT_UNLESS((it != set.end()) == existed);
                if (!existed && set.size() < 128) {
                    auto inserted = set.emplace_direct(ctx, key);
                    model.emplace(key);
                    Y_ABORT_UNLESS(*inserted == key);
                }
                break;
            }
            case 11: {
                std::vector<ui32> batch;
                const size_t count = 1 + (value % 8);
                batch.reserve(count);
                for (size_t index = 0; index < count; ++index) {
                    batch.push_back((key + index * 17 + value) % 96);
                }
                set.insert(batch.begin(), batch.end());
                model.insert(batch.begin(), batch.end());
                break;
            }
            case 12:
            case 13:
                Y_ABORT_UNLESS(set.contains(key) == model.contains(key));
                break;
        }

        CheckSet(sets[0], models[0]);
        CheckSet(sets[1], models[1]);
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
            RunMap<THash<ui32>>(input);
            break;
        case 1:
            RunSet<THash<ui32>>(input);
            break;
        case 2:
            RunMap<TCollisionHash>(input);
            break;
        case 3:
            RunSet<TCollisionHash>(input);
            break;
    }
    return 0;
}
