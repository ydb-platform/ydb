#include <library/cpp/yt/memory/chunked_memory_pool.h>
#include <library/cpp/yt/memory/free_list.h>

#include <util/generic/vector.h>
#include <util/system/yassert.h>

#include <algorithm>
#include <array>
#include <cstddef>
#include <cstdint>
#include <cstring>
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

struct TFreeItem
    : public NYT::TFreeListItemBase<TFreeItem>
{
    int Id = 0;
};

using TFreeItems = std::array<TFreeItem, 64>;
using TFreeList = NYT::TFreeList<TFreeItem>;

TFreeItem* BuildChain(TFreeItems& items, std::array<ui8, 64>& owner, ui8 target, int start, int count, TFreeItem** tail)
{
    TFreeItem* head = nullptr;
    TFreeItem* previous = nullptr;
    *tail = nullptr;

    for (int offset = 0; offset < static_cast<int>(items.size()) && count > 0; ++offset) {
        const int index = (start + offset) % items.size();
        if (owner[index] != 0) {
            continue;
        }
        owner[index] = target;
        items[index].Next.store(nullptr, std::memory_order::release);
        if (!head) {
            head = &items[index];
        }
        if (previous) {
            previous->Next.store(&items[index], std::memory_order::release);
        }
        previous = &items[index];
        *tail = &items[index];
        --count;
    }

    return head;
}

int DrainChain(TFreeItem* head, std::array<ui8, 64>& owner, ui8 expectedOwner, ui8 newOwner)
{
    std::array<bool, 64> seen{};
    int count = 0;
    while (head) {
        auto* next = head->Next.load(std::memory_order::acquire);
        Y_ENSURE(head->Id >= 0 && head->Id < static_cast<int>(owner.size()));
        Y_ENSURE(!seen[head->Id]);
        seen[head->Id] = true;
        Y_ENSURE(owner[head->Id] == expectedOwner);
        owner[head->Id] = newOwner;
        head->Next.store(nullptr, std::memory_order::release);
        head = next;
        ++count;
    }
    return count;
}

int CountOwned(const std::array<ui8, 64>& owner, ui8 value)
{
    return std::count(owner.begin(), owner.end(), value);
}

void CleanupList(TFreeList& list, std::array<ui8, 64>& owner, ui8 expectedOwner)
{
    auto* head = list.ExtractAll();
    DrainChain(head, owner, expectedOwner, 0);
}

void RunFreeList(TInput& input)
{
    TFreeItems items{};
    for (size_t index = 0; index < items.size(); ++index) {
        items[index].Id = static_cast<int>(index);
    }
    std::array<ui8, 64> owner{}; // 0 = outside, 1 = first list, 2 = second list
    TFreeList first;
    TFreeList second;

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const int start = input.Byte() % items.size();
        const int count = 1 + (input.Byte() % 5);

        switch (op % 9) {
            case 0: {
                TFreeItem* tail = nullptr;
                auto* head = BuildChain(items, owner, 1, start, 1, &tail);
                if (head) {
                    first.Put(head);
                }
                break;
            }
            case 1: {
                TFreeItem* tail = nullptr;
                auto* head = BuildChain(items, owner, 1, start, count, &tail);
                if (head) {
                    first.Put(head, tail);
                }
                break;
            }
            case 2: {
                auto* item = first.Extract();
                if (item) {
                    Y_ENSURE(owner[item->Id] == 1);
                    owner[item->Id] = 0;
                }
                break;
            }
            case 3:
                CleanupList(first, owner, 1);
                break;
            case 4: {
                TFreeItem* tail = nullptr;
                auto* head = BuildChain(items, owner, 2, start, count, &tail);
                if (head) {
                    const int limit = input.Byte() % 16;
                    const bool put = second.PutIf(head, tail, [&] (TFreeItem* current) {
                        int currentCount = 0;
                        while (current) {
                            ++currentCount;
                            current = current->Next.load(std::memory_order::acquire);
                        }
                        return currentCount < limit;
                    });
                    if (!put) {
                        DrainChain(head, owner, 2, 0);
                    }
                }
                break;
            }
            case 5:
                first.Append(second);
                for (auto& value : owner) {
                    if (value == 2) {
                        value = 1;
                    }
                }
                break;
            case 6:
                second.Append(first);
                for (auto& value : owner) {
                    if (value == 1) {
                        value = 2;
                    }
                }
                break;
            case 7: {
                TFreeList moved(std::move(first));
                CleanupList(moved, owner, 1);
                break;
            }
            default: {
                auto* item = second.Extract();
                if (item) {
                    Y_ENSURE(owner[item->Id] == 2);
                    owner[item->Id] = 0;
                }
                break;
            }
        }

        Y_ENSURE(first.IsEmpty() == (CountOwned(owner, 1) == 0));
        Y_ENSURE(second.IsEmpty() == (CountOwned(owner, 2) == 0));
    }

    CleanupList(first, owner, 1);
    CleanupList(second, owner, 2);
    Y_ENSURE(CountOwned(owner, 1) == 0);
    Y_ENSURE(CountOwned(owner, 2) == 0);
}

struct TAllocation {
    ui8 Pool = 0;
    char* Ptr = nullptr;
    size_t Size = 0;
    ui8 Pattern = 0;
    bool Live = false;
};

void FillAllocation(const TAllocation& allocation)
{
    if (allocation.Live && allocation.Size != 0) {
        std::memset(allocation.Ptr, allocation.Pattern, allocation.Size);
    }
}

void CheckAllocation(const TAllocation& allocation)
{
    if (!allocation.Live) {
        return;
    }
    for (size_t index = 0; index < allocation.Size; ++index) {
        Y_ENSURE(static_cast<ui8>(allocation.Ptr[index]) == allocation.Pattern);
    }
}

void CheckPoolStats(const std::array<NYT::TChunkedMemoryPool*, 3>& pools)
{
    for (const auto* pool : pools) {
        Y_ENSURE(pool->GetCapacity() >= pool->GetSize());
    }
}

void DropPoolAllocations(TVector<TAllocation>& allocations, ui8 pool)
{
    for (auto& allocation : allocations) {
        if (allocation.Pool == pool) {
            allocation.Live = false;
        }
    }
}

void MovePoolAllocations(TVector<TAllocation>& allocations, ui8 from, ui8 to)
{
    for (auto& allocation : allocations) {
        if (allocation.Live && allocation.Pool == from) {
            allocation.Pool = to;
        }
    }
}

void RunChunkedMemoryPool(TInput& input)
{
    NYT::TChunkedMemoryPool first(NYT::NullRefCountedTypeCookie, 64);
    NYT::TChunkedMemoryPool second(NYT::NullRefCountedTypeCookie, 128);
    NYT::TChunkedMemoryPool third(NYT::NullRefCountedTypeCookie, 256);
    std::array<NYT::TChunkedMemoryPool*, 3> pools = {&first, &second, &third};
    TVector<TAllocation> allocations;
    allocations.reserve(128);

    const size_t steps = std::min<size_t>(input.Byte() + 1, 192);
    for (size_t step = 0; step < steps && !input.Empty(); ++step) {
        const ui8 op = input.Byte();
        const ui8 poolIndex = input.Byte() % pools.size();
        const size_t size = input.Byte() % 257;
        const ui8 pattern = input.Byte();

        switch (op % 8) {
            case 0: {
                char* ptr = pools[poolIndex]->AllocateUnaligned(size);
                allocations.push_back({poolIndex, ptr, size, pattern, true});
                FillAllocation(allocations.back());
                break;
            }
            case 1: {
                const int align = 1 << (input.Byte() % 6);
                char* ptr = pools[poolIndex]->AllocateAligned(size, align);
                Y_ENSURE(reinterpret_cast<uintptr_t>(ptr) % align == 0);
                allocations.push_back({poolIndex, ptr, size, pattern, true});
                FillAllocation(allocations.back());
                break;
            }
            case 2: {
                const int count = input.Byte() % 32;
                auto* ptr = pools[poolIndex]->AllocateUninitialized<ui64>(count);
                allocations.push_back({poolIndex, reinterpret_cast<char*>(ptr), sizeof(ui64) * static_cast<size_t>(count), pattern, true});
                FillAllocation(allocations.back());
                break;
            }
            case 3:
                for (auto it = allocations.rbegin(); it != allocations.rend(); ++it) {
                    if (it->Live && it->Pool == poolIndex) {
                        pools[poolIndex]->Free(it->Ptr, it->Ptr + it->Size);
                        it->Live = false;
                        break;
                    }
                }
                break;
            case 4:
                pools[poolIndex]->Clear();
                DropPoolAllocations(allocations, poolIndex);
                break;
            case 5:
                pools[poolIndex]->Purge();
                DropPoolAllocations(allocations, poolIndex);
                break;
            case 6: {
                const ui8 from = input.Byte() % pools.size();
                if (from != poolIndex) {
                    pools[poolIndex]->Absorb(std::move(*pools[from]));
                    MovePoolAllocations(allocations, from, poolIndex);
                }
                break;
            }
            default:
                if (allocations.size() > 128) {
                    allocations.erase(
                        std::remove_if(allocations.begin(), allocations.end(), [] (const TAllocation& allocation) {
                            return !allocation.Live;
                        }),
                        allocations.end());
                }
                break;
        }

        for (const auto& allocation : allocations) {
            CheckAllocation(allocation);
        }
        CheckPoolStats(pools);
    }
}

} // namespace

extern "C" int LLVMFuzzerTestOneInput(const ui8* data, size_t size)
{
    TInput input(data, size);
    RunFreeList(input);
    RunChunkedMemoryPool(input);
    return 0;
}
