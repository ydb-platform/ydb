#include <ydb/core/util/intrusive_heap.h>

#include <contrib/libs/libfuzzer/include/fuzzer/FuzzedDataProvider.h>

#include <util/system/yassert.h>

#include <array>
#include <set>

namespace {

constexpr size_t MaxOps = 192;
constexpr size_t HeapItems = 32;

struct THeapNode {
    int Value = 0;
    size_t Id = 0;
    size_t HeapIndex = size_t(-1);
    bool Active = false;

    struct THeapIndex {
        size_t& operator()(THeapNode& item) const {
            return item.HeapIndex;
        }
    };

    struct TCompare {
        bool operator()(const THeapNode& lhs, const THeapNode& rhs) const {
            return lhs.Value != rhs.Value ? lhs.Value < rhs.Value : lhs.Id < rhs.Id;
        }
    };
};

using THeap = NKikimr::TIntrusiveHeap<THeapNode, THeapNode::THeapIndex, THeapNode::TCompare>;

void CheckHeap(THeap& heap, std::array<THeapNode, HeapItems>& items) {
    size_t active = 0;
    const THeapNode* minNode = nullptr;
    std::set<size_t> seenIndexes;

    for (auto& item : items) {
        Y_ABORT_UNLESS(heap.Has(&item) == item.Active);
        if (!item.Active) {
            Y_ABORT_UNLESS(item.HeapIndex == size_t(-1));
            continue;
        }
        ++active;
        Y_ABORT_UNLESS(item.HeapIndex < heap.Size());
        Y_ABORT_UNLESS(seenIndexes.insert(item.HeapIndex).second);
        if (!minNode || THeapNode::TCompare{}(item, *minNode)) {
            minNode = &item;
        }
    }

    Y_ABORT_UNLESS(heap.Size() == active);
    Y_ABORT_UNLESS(heap.Empty() == (active == 0));
    Y_ABORT_UNLESS(bool(heap) == (active != 0));
    Y_ABORT_UNLESS(heap.Top() == minNode);
}

void ExerciseIntrusiveHeap(FuzzedDataProvider& fdp) {
    THeap heap;
    std::array<THeapNode, HeapItems> items;
    for (size_t i = 0; i < items.size(); ++i) {
        items[i].Value = static_cast<int>(i);
        items[i].Id = i;
    }

    const size_t ops = fdp.ConsumeIntegralInRange<size_t>(0, MaxOps);
    for (size_t i = 0; i < ops; ++i) {
        auto& item = items[fdp.ConsumeIntegralInRange<size_t>(0, items.size() - 1)];
        switch (fdp.ConsumeIntegralInRange<unsigned>(0, 5)) {
            case 0:
                if (!item.Active) {
                    item.Value = fdp.ConsumeIntegralInRange<int>(-128, 128);
                    heap.Add(&item);
                    item.Active = true;
                }
                break;
            case 1:
                if (item.Active) {
                    heap.Remove(&item);
                    item.Active = false;
                }
                break;
            case 2:
                if (item.Active) {
                    item.Value = fdp.ConsumeIntegralInRange<int>(-128, 128);
                    heap.Update(&item);
                }
                break;
            case 3:
                if (item.Active) {
                    item.Value = fdp.ConsumeIntegralInRange<int>(-128, item.Value);
                    heap.SiftUp(&item);
                }
                break;
            case 4:
                if (item.Active) {
                    item.Value = fdp.ConsumeIntegralInRange<int>(item.Value, 128);
                    heap.SiftDown(&item);
                }
                break;
            default:
                heap.Clear();
                for (auto& node : items) {
                    node.Active = false;
                }
                break;
        }

        CheckHeap(heap, items);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size) {
    FuzzedDataProvider fdp(data, size);
    ExerciseIntrusiveHeap(fdp);

    return 0;
}
