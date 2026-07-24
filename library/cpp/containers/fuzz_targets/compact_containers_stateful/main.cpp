#include <library/cpp/containers/compact_vector/compact_vector.h>
#include <library/cpp/containers/stack_vector/stack_vec.h>
#include <library/cpp/yt/compact_containers/compact_flat_map.h>
#include <library/cpp/yt/compact_containers/compact_flat_set.h>
#include <library/cpp/yt/compact_containers/compact_heap.h>
#include <library/cpp/yt/compact_containers/compact_queue.h>
#include <library/cpp/yt/compact_containers/compact_set.h>
#include <library/cpp/yt/compact_containers/compact_vector.h>

#include <util/system/types.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <cstddef>
#include <cstdint>
#include <deque>
#include <map>
#include <queue>
#include <set>
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

int Value(TInput& input) {
    return static_cast<int>(input.Int() % 257) - 128;
}

size_t Count(TInput& input, size_t maxValue) {
    return maxValue == 0 ? 0 : input.Int() % (maxValue + 1);
}

template <class TVector>
void CheckVector(const TVector& vector, const std::vector<int>& model) {
    Y_ABORT_UNLESS(vector.size() == model.size());
    Y_ABORT_UNLESS(vector.empty() == model.empty());
    Y_ABORT_UNLESS(vector.capacity() >= vector.size());
    Y_ABORT_UNLESS(std::equal(vector.begin(), vector.end(), model.begin(), model.end()));
    if (!model.empty()) {
        Y_ABORT_UNLESS(vector.front() == model.front());
        Y_ABORT_UNLESS(vector.back() == model.back());
    }
    std::vector<int> reversed(vector.rbegin(), vector.rend());
    std::vector<int> expected(model.rbegin(), model.rend());
    Y_ABORT_UNLESS(reversed == expected);
}

void RunYtCompactVector(TInput& input) {
    NYT::TCompactVector<int, 4> vectors[2];
    std::vector<int> models[2];

    const size_t steps = std::min<size_t>(input.Byte() + 1, 224);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const size_t slot = input.Byte() & 1;
        auto& vector = vectors[slot];
        auto& model = models[slot];
        const ui8 op = input.Byte();
        const int value = Value(input);

        switch (op % 15) {
            case 0:
                if (model.size() < 64) {
                    vector.push_back(value);
                    model.push_back(value);
                }
                break;
            case 1:
                if (model.size() < 64) {
                    vector.emplace_back(value);
                    model.push_back(value);
                }
                break;
            case 2:
                if (!model.empty()) {
                    vector.pop_back();
                    model.pop_back();
                }
                break;
            case 3: {
                const size_t size = Count(input, 64);
                vector.resize(size);
                model.resize(size);
                break;
            }
            case 4: {
                const size_t size = Count(input, 64);
                vector.resize(size, value);
                model.resize(size, value);
                break;
            }
            case 5:
                vector.reserve(Count(input, 80));
                break;
            case 6:
                if (model.size() < 64) {
                    const size_t pos = Count(input, model.size());
                    auto it = vector.insert(vector.begin() + pos, value);
                    model.insert(model.begin() + pos, value);
                    Y_ABORT_UNLESS(std::distance(vector.begin(), it) == static_cast<ptrdiff_t>(pos));
                }
                break;
            case 7:
                if (model.size() < 64) {
                    const size_t pos = Count(input, model.size());
                    const size_t count = std::min<size_t>(Count(input, 4), 64 - model.size());
                    vector.insert(vector.begin() + pos, count, value);
                    model.insert(model.begin() + pos, count, value);
                }
                break;
            case 8:
                if (!model.empty()) {
                    const size_t pos = Count(input, model.size() - 1);
                    vector.erase(vector.begin() + pos);
                    model.erase(model.begin() + pos);
                }
                break;
            case 9:
                if (!model.empty()) {
                    const size_t first = Count(input, model.size());
                    const size_t last = first + Count(input, model.size() - first);
                    vector.erase(vector.begin() + first, vector.begin() + last);
                    model.erase(model.begin() + first, model.begin() + last);
                }
                break;
            case 10:
                vector.clear();
                model.clear();
                break;
            case 11:
                vector.shrink_to_small();
                break;
            case 12:
                vectors[0].swap(vectors[1]);
                models[0].swap(models[1]);
                break;
            case 13: {
                NYT::TCompactVector<int, 4> copy(vector);
                CheckVector(copy, model);
                NYT::TCompactVector<int, 2> otherN(copy);
                Y_ABORT_UNLESS(std::equal(otherN.begin(), otherN.end(), model.begin(), model.end()));
                break;
            }
            case 14: {
                NYT::TCompactVector<int, 4> moved(std::move(vector));
                CheckVector(moved, model);
                vector = moved;
                break;
            }
        }

        CheckVector(vectors[0], models[0]);
        CheckVector(vectors[1], models[1]);
    }
}

void CheckLegacyVector(const TCompactVector<int>& vector, const std::vector<int>& model) {
    Y_ABORT_UNLESS(vector.size() == model.size());
    Y_ABORT_UNLESS(vector.Empty() == model.empty());
    Y_ABORT_UNLESS(vector.Capacity() >= vector.size());
    Y_ABORT_UNLESS(std::equal(vector.begin(), vector.end(), model.begin(), model.end()));
    if (!model.empty()) {
        Y_ABORT_UNLESS(vector.Back() == model.back());
    }
}

void RunLegacyCompactVector(TInput& input) {
    TCompactVector<int> vectors[2];
    std::vector<int> models[2];

    const size_t steps = std::min<size_t>(input.Byte() + 1, 160);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const size_t slot = input.Byte() & 1;
        auto& vector = vectors[slot];
        auto& model = models[slot];
        const ui8 op = input.Byte();
        const int value = Value(input);

        switch (op % 9) {
            case 0:
                if (model.size() < 64) {
                    vector.PushBack(value);
                    model.push_back(value);
                }
                break;
            case 1:
                vector.Reserve(Count(input, 80));
                break;
            case 2:
                if (model.size() < 64) {
                    const size_t pos = Count(input, model.size());
                    vector.Insert(vector.Begin() + pos, value);
                    model.insert(model.begin() + pos, value);
                }
                break;
            case 3:
                if (!model.empty()) {
                    const size_t pos = Count(input, model.size() - 1);
                    vector.erase(vector.begin() + pos);
                    model.erase(model.begin() + pos);
                }
                break;
            case 4:
                vector.Clear();
                model.clear();
                break;
            case 5:
                vectors[0].Swap(vectors[1]);
                models[0].swap(models[1]);
                break;
            case 6: {
                TCompactVector<int> copy(vector);
                CheckLegacyVector(copy, model);
                break;
            }
            case 7: {
                TCompactVector<int> assigned;
                assigned = vector;
                CheckLegacyVector(assigned, model);
                break;
            }
            case 8:
                Y_ABORT_UNLESS(static_cast<bool>(vector) == !model.empty());
                break;
        }

        CheckLegacyVector(vectors[0], models[0]);
        CheckLegacyVector(vectors[1], models[1]);
    }
}

void RunStackVec(TInput& input) {
    TStackVec<int, 4> vector;
    std::vector<int> model;

    const size_t steps = std::min<size_t>(input.Byte() + 1, 160);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const int value = Value(input);

        switch (op % 9) {
            case 0:
                if (model.size() < 64) {
                    vector.push_back(value);
                    model.push_back(value);
                }
                break;
            case 1:
                if (!model.empty()) {
                    vector.pop_back();
                    model.pop_back();
                }
                break;
            case 2: {
                const size_t size = Count(input, 64);
                vector.resize(size, value);
                model.resize(size, value);
                break;
            }
            case 3:
                vector.reserve(Count(input, 80));
                break;
            case 4:
                if (model.size() < 64) {
                    const size_t pos = Count(input, model.size());
                    vector.insert(vector.begin() + pos, value);
                    model.insert(model.begin() + pos, value);
                }
                break;
            case 5:
                if (!model.empty()) {
                    const size_t pos = Count(input, model.size() - 1);
                    vector.erase(vector.begin() + pos);
                    model.erase(model.begin() + pos);
                }
                break;
            case 6:
                vector.clear();
                model.clear();
                break;
            case 7: {
                TStackVec<int, 4> copy(vector);
                Y_ABORT_UNLESS(std::equal(copy.begin(), copy.end(), model.begin(), model.end()));
                break;
            }
            case 8: {
                TStackVec<int, 4> assigned;
                assigned = vector;
                Y_ABORT_UNLESS(std::equal(assigned.begin(), assigned.end(), model.begin(), model.end()));
                break;
            }
        }

        CheckVector(vector, model);
        Y_ABORT_UNLESS(vector.capacity() >= 4);
    }
}

template <class TMap>
void CheckFlatMap(const TMap& map, const std::map<int, int>& model) {
    Y_ABORT_UNLESS(map.size() == model.size());
    Y_ABORT_UNLESS(map.ssize() == static_cast<int>(model.size()));
    Y_ABORT_UNLESS(map.empty() == model.empty());

    auto mit = model.begin();
    for (const auto& [key, value] : map) {
        Y_ABORT_UNLESS(mit != model.end());
        Y_ABORT_UNLESS(key == mit->first);
        Y_ABORT_UNLESS(value == mit->second);
        ++mit;
    }
    Y_ABORT_UNLESS(mit == model.end());

    for (int key = -32; key <= 32; ++key) {
        auto actual = map.find(key);
        auto expected = model.find(key);
        Y_ABORT_UNLESS((actual != map.end()) == (expected != model.end()));
        Y_ABORT_UNLESS(map.contains(key) == (expected != model.end()));
        if (expected != model.end()) {
            Y_ABORT_UNLESS(actual->second == expected->second);
        }
        Y_ABORT_UNLESS(std::distance(map.begin(), map.lower_bound(key)) == std::distance(model.begin(), model.lower_bound(key)));
        Y_ABORT_UNLESS(std::distance(map.begin(), map.upper_bound(key)) == std::distance(model.begin(), model.upper_bound(key)));
    }
}

void RunCompactFlatMap(TInput& input) {
    using TMap = NYT::TCompactFlatMap<int, int, 4>;
    TMap maps[2];
    std::map<int, int> models[2];

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const size_t slot = input.Byte() & 1;
        auto& map = maps[slot];
        auto& model = models[slot];
        const ui8 op = input.Byte();
        const int key = Value(input) % 33;
        const int value = Value(input);

        switch (op % 12) {
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
                break;
            }
            case 2:
                map[key] = value;
                model[key] = value;
                break;
            case 3:
                map.erase(key);
                model.erase(key);
                break;
            case 4:
                if (!model.empty()) {
                    const size_t pos = Count(input, model.size() - 1);
                    auto it = map.begin() + pos;
                    const int eraseKey = it->first;
                    map.erase(it);
                    model.erase(eraseKey);
                }
                break;
            case 5:
                if (!model.empty()) {
                    const size_t first = Count(input, model.size());
                    const size_t last = first + Count(input, model.size() - first);
                    auto begin = map.begin() + first;
                    auto end = map.begin() + last;
                    std::vector<int> erasedKeys;
                    for (auto it = begin; it != end; ++it) {
                        erasedKeys.push_back(it->first);
                    }
                    map.erase(begin, end);
                    for (int erasedKey : erasedKeys) {
                        model.erase(erasedKey);
                    }
                }
                break;
            case 6:
                map.reserve(Count(input, 80));
                break;
            case 7:
                map.shrink_to_small();
                break;
            case 8:
                map.clear();
                model.clear();
                break;
            case 9: {
                std::vector<std::pair<int, int>> batch;
                const size_t count = 1 + Count(input, 6);
                for (size_t index = 0; index < count; ++index) {
                    batch.push_back({(key + static_cast<int>(index) * 7) % 33, value + static_cast<int>(index)});
                }
                map.insert(batch.begin(), batch.end());
                for (const auto& item : batch) {
                    model.emplace(item);
                }
                break;
            }
            case 10: {
                TMap copy(map);
                CheckFlatMap(copy, model);
                break;
            }
            case 11: {
                TMap moved(std::move(map));
                CheckFlatMap(moved, model);
                map = moved;
                break;
            }
        }

        CheckFlatMap(maps[0], models[0]);
        CheckFlatMap(maps[1], models[1]);
    }
}

template <class TSet>
void CheckSetLike(const TSet& set, const std::set<int>& model, bool hasSSize) {
    Y_ABORT_UNLESS(set.size() == model.size());
    if constexpr (requires { set.ssize(); }) {
        Y_ABORT_UNLESS(set.ssize() == static_cast<int>(model.size()));
    } else {
        Y_ABORT_UNLESS(!hasSSize);
    }
    Y_ABORT_UNLESS(set.empty() == model.empty());

    std::vector<int> actual(set.begin(), set.end());
    std::vector<int> expected(model.begin(), model.end());
    Y_ABORT_UNLESS(actual == expected);

    for (int key = -32; key <= 32; ++key) {
        Y_ABORT_UNLESS(set.contains(key) == model.contains(key));
        Y_ABORT_UNLESS(set.count(key) == (model.contains(key) ? 1 : 0));
    }
}

void RunCompactFlatSet(TInput& input) {
    using TSet = NYT::TCompactFlatSet<int, 4>;
    TSet sets[2];
    std::set<int> models[2];

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const size_t slot = input.Byte() & 1;
        auto& set = sets[slot];
        auto& model = models[slot];
        const ui8 op = input.Byte();
        const int value = Value(input) % 33;

        switch (op % 11) {
            case 0: {
                const auto actual = set.insert(value);
                const auto expected = model.insert(value);
                Y_ABORT_UNLESS(actual.second == expected.second);
                break;
            }
            case 1:
                Y_ABORT_UNLESS(set.erase(value) == (model.erase(value) != 0));
                break;
            case 2:
                if (!model.empty()) {
                    const size_t pos = Count(input, model.size() - 1);
                    auto it = set.begin() + pos;
                    const int erased = *it;
                    set.erase(it);
                    model.erase(erased);
                }
                break;
            case 3:
                if (!model.empty()) {
                    const size_t first = Count(input, model.size());
                    const size_t last = first + Count(input, model.size() - first);
                    auto begin = set.begin() + first;
                    auto end = set.begin() + last;
                    std::vector<int> erased(begin, end);
                    set.erase(begin, end);
                    for (int item : erased) {
                        model.erase(item);
                    }
                }
                break;
            case 4:
                set.reserve(Count(input, 80));
                break;
            case 5:
                set.shrink_to_small();
                break;
            case 6:
                set.clear();
                model.clear();
                break;
            case 7: {
                std::vector<int> batch;
                const size_t count = 1 + Count(input, 6);
                for (size_t index = 0; index < count; ++index) {
                    batch.push_back((value + static_cast<int>(index) * 5) % 33);
                }
                set.insert(batch.begin(), batch.end());
                model.insert(batch.begin(), batch.end());
                break;
            }
            case 8: {
                TSet copy(set);
                CheckSetLike(copy, model, true);
                break;
            }
            case 9: {
                TSet moved(std::move(set));
                CheckSetLike(moved, model, true);
                set = moved;
                break;
            }
            case 10:
                Y_ABORT_UNLESS(std::distance(set.begin(), set.lower_bound(value)) == std::distance(model.begin(), model.lower_bound(value)));
                Y_ABORT_UNLESS(std::distance(set.begin(), set.upper_bound(value)) == std::distance(model.begin(), model.upper_bound(value)));
                break;
        }

        CheckSetLike(sets[0], models[0], true);
        CheckSetLike(sets[1], models[1], true);
    }
}

void RunCompactSet(TInput& input) {
    using TSet = NYT::TCompactSet<int, 4>;
    TSet set;
    std::set<int> model;

    const size_t steps = std::min<size_t>(input.Byte() + 1, 160);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const int value = Value(input) % 33;

        switch (op % 7) {
            case 0: {
                const auto actual = set.insert(value);
                const auto expected = model.insert(value);
                Y_ABORT_UNLESS(actual.second == expected.second);
                break;
            }
            case 1:
                Y_ABORT_UNLESS(set.erase(value) == (model.erase(value) != 0));
                break;
            case 2:
                set.clear();
                model.clear();
                break;
            case 3: {
                std::vector<int> batch;
                const size_t count = 1 + Count(input, 6);
                for (size_t index = 0; index < count; ++index) {
                    batch.push_back((value + static_cast<int>(index) * 5) % 33);
                }
                set.insert(batch.begin(), batch.end());
                model.insert(batch.begin(), batch.end());
                break;
            }
            case 4: {
                TSet copy(set);
                CheckSetLike(copy, model, false);
                Y_ABORT_UNLESS(copy == set);
                break;
            }
            case 5:
                if (!model.empty()) {
                    Y_ABORT_UNLESS(set.front() == *model.begin());
                }
                break;
            case 6:
                Y_ABORT_UNLESS(set.contains(value) == model.contains(value));
                break;
        }

        CheckSetLike(set, model, false);
    }
}

void RunCompactQueue(TInput& input) {
    NYT::TCompactQueue<int, 4> queue;
    std::deque<int> model;

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const int value = Value(input);

        switch (op % 5) {
            case 0:
            case 1:
                if (model.size() < 96) {
                    queue.Push(value);
                    model.push_back(value);
                }
                break;
            case 2:
                if (!model.empty()) {
                    Y_ABORT_UNLESS(queue.Pop() == model.front());
                    model.pop_front();
                }
                break;
            case 3:
                if (!model.empty()) {
                    Y_ABORT_UNLESS(queue.Front() == model.front());
                }
                break;
            case 4: {
                auto copy = queue;
                Y_ABORT_UNLESS(copy.Size() == model.size());
                if (!model.empty()) {
                    Y_ABORT_UNLESS(copy.Front() == model.front());
                }
                break;
            }
        }

        Y_ABORT_UNLESS(queue.Size() == model.size());
        Y_ABORT_UNLESS(queue.Empty() == model.empty());
        Y_ABORT_UNLESS(queue.Capacity() >= queue.Size());
        if (!model.empty()) {
            Y_ABORT_UNLESS(queue.Front() == model.front());
        }
    }
}

void CheckHeap(const NYT::TCompactHeap<int, 3>& heap, const std::multiset<int>& model) {
    Y_ABORT_UNLESS(heap.size() == model.size());
    Y_ABORT_UNLESS(heap.empty() == model.empty());
    Y_ABORT_UNLESS(heap.capacity() >= heap.size());
    if (!model.empty()) {
        Y_ABORT_UNLESS(heap.get_min() == *model.begin());
    }

    std::vector<int> actual(heap.begin(), heap.end());
    std::sort(actual.begin(), actual.end());
    std::vector<int> expected(model.begin(), model.end());
    Y_ABORT_UNLESS(actual == expected);
}

void RunCompactHeap(TInput& input) {
    NYT::TCompactHeap<int, 3> heaps[2];
    std::multiset<int> models[2];

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const size_t slot = input.Byte() & 1;
        auto& heap = heaps[slot];
        auto& model = models[slot];
        const ui8 op = input.Byte();
        const int value = Value(input);

        switch (op % 8) {
            case 0:
            case 1:
                if (model.size() < 96) {
                    heap.push(value);
                    model.insert(value);
                }
                break;
            case 2:
                if (!model.empty()) {
                    heap.pop();
                    model.erase(model.begin());
                }
                break;
            case 3:
                if (!model.empty()) {
                    const int actual = heap.extract_min();
                    const int expected = *model.begin();
                    model.erase(model.begin());
                    Y_ABORT_UNLESS(actual == expected);
                }
                break;
            case 4:
                if (!model.empty()) {
                    Y_ABORT_UNLESS(heap.get_min() == *model.begin());
                }
                break;
            case 5:
                heap.shrink_to_small();
                break;
            case 6:
                heaps[0].swap(heaps[1]);
                models[0].swap(models[1]);
                break;
            case 7: {
                auto copy = heap;
                CheckHeap(copy, model);
                break;
            }
        }

        CheckHeap(heaps[0], models[0]);
        CheckHeap(heaps[1], models[1]);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    TInput input(data, size);
    if (input.Empty()) {
        return 0;
    }

    switch (input.Byte() % 8) {
        case 0:
            RunYtCompactVector(input);
            break;
        case 1:
            RunLegacyCompactVector(input);
            break;
        case 2:
            RunStackVec(input);
            break;
        case 3:
            RunCompactFlatMap(input);
            break;
        case 4:
            RunCompactFlatSet(input);
            break;
        case 5:
            RunCompactSet(input);
            break;
        case 6:
            RunCompactQueue(input);
            break;
        case 7:
            RunCompactHeap(input);
            break;
    }

    return 0;
}
