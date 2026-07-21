#include <library/cpp/cache/cache.h>
#include <library/cpp/yt/containers/intrusive_linked_list.h>
#include <library/cpp/yt/containers/ordered_hash_map.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <utility>

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

struct TLinkedItem {
    int Id = 0;
    NYT::TIntrusiveLinkedListNode<TLinkedItem> Link;
};

struct TLinkedItemToNode {
    NYT::TIntrusiveLinkedListNode<TLinkedItem>* operator()(TLinkedItem* item) const {
        return &item->Link;
    }
};

using TLinkedList = NYT::TIntrusiveLinkedList<TLinkedItem, TLinkedItemToNode>;

void CheckLinkedList(
    const TLinkedList& list,
    const std::array<TLinkedItem, 32>& items,
    const TVector<int>& model,
    const std::array<bool, 32>& inList)
{
    Y_ENSURE(static_cast<size_t>(list.GetSize()) == model.size());
    Y_ENSURE((list.GetFront() == nullptr) == model.empty());
    Y_ENSURE((list.GetBack() == nullptr) == model.empty());
    if (!model.empty()) {
        Y_ENSURE(list.GetFront() == &items[model.front()]);
        Y_ENSURE(list.GetBack() == &items[model.back()]);
    }

    std::array<bool, 32> seen{};
    const TLinkedItem* previous = nullptr;
    const TLinkedItem* current = list.GetFront();
    for (size_t position = 0; position < model.size(); ++position) {
        Y_ENSURE(current);
        Y_ENSURE(current == &items[model[position]]);
        Y_ENSURE(current->Link.Prev == previous);
        Y_ENSURE(!seen[current->Id]);
        Y_ENSURE(inList[current->Id]);
        seen[current->Id] = true;
        previous = current;
        current = current->Link.Next;
    }
    Y_ENSURE(current == nullptr);

    for (size_t index = 0; index < inList.size(); ++index) {
        Y_ENSURE(inList[index] == seen[index]);
    }
}

void RunIntrusiveLinkedList(TInput& input)
{
    std::array<TLinkedItem, 32> items{};
    for (size_t index = 0; index < items.size(); ++index) {
        items[index].Id = static_cast<int>(index);
    }

    TLinkedList list;
    TVector<int> model;
    std::array<bool, 32> inList{};

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 raw = input.Byte();
        const int index = input.Byte() % items.size();

        switch (raw % 8) {
            case 0:
                if (!inList[index]) {
                    list.PushBack(&items[index]);
                    model.push_back(index);
                    inList[index] = true;
                }
                break;
            case 1:
                if (!inList[index]) {
                    list.PushFront(&items[index]);
                    model.insert(model.begin(), index);
                    inList[index] = true;
                }
                break;
            case 2:
                if (inList[index]) {
                    list.Remove(&items[index]);
                    model.erase(std::find(model.begin(), model.end(), index));
                    inList[index] = false;
                }
                break;
            case 3:
                if (!model.empty()) {
                    inList[model.front()] = false;
                    model.erase(model.begin());
                    list.PopFront();
                }
                break;
            case 4:
                if (!model.empty()) {
                    inList[model.back()] = false;
                    model.pop_back();
                    list.PopBack();
                }
                break;
            case 5:
                list.Clear();
                model.clear();
                inList.fill(false);
                break;
            case 6:
                if (!model.empty()) {
                    const int front = model.front();
                    list.Remove(&items[front]);
                    model.erase(model.begin());
                    inList[front] = false;
                    list.PushBack(&items[front]);
                    model.push_back(front);
                    inList[front] = true;
                }
                break;
            default:
                if (!model.empty()) {
                    const int back = model.back();
                    list.Remove(&items[back]);
                    model.pop_back();
                    inList[back] = false;
                    list.PushFront(&items[back]);
                    model.insert(model.begin(), back);
                    inList[back] = true;
                }
                break;
        }

        CheckLinkedList(list, items, model, inList);
    }
}

using TOrderedMap = NYT::TOrderedHashMap<ui8, ui32>;

void CheckOrderedMap(const TOrderedMap& map, const TVector<std::pair<ui8, ui32>>& model)
{
    Y_ENSURE(map.size() == model.size());
    auto it = map.begin();
    for (const auto& expected : model) {
        Y_ENSURE(it != map.end());
        Y_ENSURE(it->first == expected.first);
        Y_ENSURE(it->second == expected.second);
        Y_ENSURE(map.contains(expected.first));
        auto found = map.find(expected.first);
        Y_ENSURE(found != map.end());
        Y_ENSURE(found->second == expected.second);
        ++it;
    }
    Y_ENSURE(it == map.end());
}

void RunOrderedHashMap(TInput& input)
{
    TOrderedMap map;
    TVector<std::pair<ui8, ui32>> model;

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const ui8 key = input.Byte() % 48;
        const ui32 value = input.Int();
        auto modelIt = std::find_if(model.begin(), model.end(), [key](const auto& item) {
            return item.first == key;
        });

        switch (op % 9) {
            case 0: {
                auto [it, inserted] = map.emplace(key, value);
                Y_ENSURE(it != map.end());
                if (modelIt == model.end()) {
                    Y_ENSURE(inserted);
                    model.emplace_back(key, value);
                } else {
                    Y_ENSURE(!inserted);
                    Y_ENSURE(it->second == modelIt->second);
                }
                break;
            }
            case 1:
                map[key] = value;
                if (modelIt == model.end()) {
                    model.emplace_back(key, value);
                } else {
                    modelIt->second = value;
                }
                break;
            case 2: {
                const size_t erased = map.erase(key);
                if (modelIt == model.end()) {
                    Y_ENSURE(erased == 0);
                } else {
                    Y_ENSURE(erased == 1);
                    model.erase(modelIt);
                }
                break;
            }
            case 3: {
                auto it = map.find(key);
                if (it != map.end()) {
                    map.erase(it);
                    Y_ENSURE(modelIt != model.end());
                    model.erase(modelIt);
                }
                break;
            }
            case 4:
                Y_ENSURE(map.contains(key) == (modelIt != model.end()));
                break;
            case 5: {
                TOrderedMap copy(map);
                CheckOrderedMap(copy, model);
                copy.clear();
                Y_ENSURE(copy.size() == 0);
                Y_ENSURE(copy.begin() == copy.end());
                break;
            }
            case 6: {
                TOrderedMap assigned;
                assigned = map;
                CheckOrderedMap(assigned, model);
                break;
            }
            case 7:
                map.clear();
                model.clear();
                break;
            default:
                if (!model.empty()) {
                    const size_t offset = value % model.size();
                    auto it = map.begin();
                    for (size_t i = 0; i < offset; ++i) {
                        ++it;
                    }
                    Y_ENSURE(it->first == model[offset].first);
                    Y_ENSURE(it->second == model[offset].second);
                }
                break;
        }

        CheckOrderedMap(map, model);
    }
}

struct TStringSizeProvider {
    size_t operator()(const ui32& value) const {
        return 1 + (value % 7);
    }
};

template <class TCache>
void CheckCache(TCache& cache, const TVector<std::pair<ui8, ui32>>& model)
{
    Y_ENSURE(cache.Size() == model.size());
    for (const auto& [key, value] : model) {
        auto it = cache.FindWithoutPromote(key);
        Y_ENSURE(it != cache.End());
        Y_ENSURE(*it == value);
    }
}

template <class TCache>
void SyncCacheModel(TCache& cache, TVector<std::pair<ui8, ui32>>& model)
{
    model.clear();
    for (ui8 key = 0; key < 32; ++key) {
        auto it = cache.FindWithoutPromote(key);
        if (it != cache.End()) {
            model.emplace_back(key, *it);
        }
    }
}

template <class TCache>
void RunCacheList(TInput& input, TCache& cache)
{
    TVector<std::pair<ui8, ui32>> model;

    const size_t steps = std::min<size_t>(input.Byte() + 1, 128);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const ui8 key = input.Byte() % 32;
        const ui32 value = input.Int();
        auto modelIt = std::find_if(model.begin(), model.end(), [key](const auto& item) {
            return item.first == key;
        });

        switch (op % 7) {
            case 0:
                cache.Insert(key, value);
                break;
            case 1:
                cache.Update(key, value);
                break;
            case 2: {
                auto it = cache.Find(key);
                if (it != cache.End()) {
                    Y_ENSURE(modelIt != model.end());
                }
                break;
            }
            case 3: {
                ui32 picked = 0;
                const bool found = cache.PickOut(key, &picked);
                Y_ENSURE(found == (modelIt != model.end()));
                if (found) {
                    Y_ENSURE(picked == modelIt->second);
                    model.erase(modelIt);
                }
                break;
            }
            case 4:
                cache.SetMaxSize(1 + (value % 24));
                break;
            case 5:
                cache.Clear();
                model.clear();
                break;
            default:
                cache.Reserve(value % 64);
                break;
        }

        SyncCacheModel(cache, model);
        CheckCache(cache, model);
    }
}

void RunCacheLists(TInput& input)
{
    TLRUCache<ui8, ui32, TNoopDelete, TStringSizeProvider> lru(16, false, TStringSizeProvider());
    RunCacheList(input, lru);

    TLFUCache<ui8, ui32, TNoopDelete, std::allocator<TLFUList<ui8, ui32, TStringSizeProvider>::TItem>, TStringSizeProvider> lfu(16, false, TStringSizeProvider());
    RunCacheList(input, lfu);
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size)
{
    TInput input(data, size);
    RunIntrusiveLinkedList(input);
    RunOrderedHashMap(input);
    RunCacheLists(input);
    return 0;
}
