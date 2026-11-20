#include "shared_data.h"

#include "memory_tracker.h"

#include <util/system/sys_alloc.h>
#include <util/system/sanitizers.h>

namespace NActors {

    static constexpr char MemoryLabelSharedData[] = "Tablet/TSharedData/Buffers";

    char* TSharedData::Allocate(size_t size) {
        char* data = nullptr;
        if (size > 0) {
            if (size >= MaxDataSize) {
                throw std::length_error("Allocate size overflow");
            }
            auto allocSize = OverheadSize + size;
            char* raw = reinterpret_cast<char*>(y_allocate(allocSize));

            auto* privateHeader = reinterpret_cast<TPrivateHeader*>(raw);
            privateHeader->AllocSize = allocSize;
            NActors::NMemory::TLabel<MemoryLabelSharedData>::Add(allocSize);

            auto* header = reinterpret_cast<THeader*>(raw + PrivateHeaderSize);
            new (header) THeader(nullptr);

            data = raw + OverheadSize;
            NSan::Poison(data, size);
        }
        return data;
    }

    void TSharedData::Deallocate(char* data) noexcept {
        if (data) {
            char* raw = data - OverheadSize;

            auto* privateHeader = reinterpret_cast<TPrivateHeader*>(raw);
            NActors::NMemory::TLabel<MemoryLabelSharedData>::Sub(privateHeader->AllocSize);

            auto* header = reinterpret_cast<THeader*>(raw + PrivateHeaderSize);
            Y_DEBUG_ABORT_UNLESS(header->Owner.load(std::memory_order_relaxed) == nullptr);
            header->~THeader();

            y_deallocate(raw);
        }
    }

    bool TSharedData::TrySetBuiltinOwner(IOwner& owner) const noexcept {
        if (!Data_) {
            return false;
        }
        IOwner* expected = nullptr;
        return Header()->Owner.compare_exchange_strong(
            expected, &owner, std::memory_order_acq_rel, std::memory_order_acquire);
    }

    size_t TSharedData::IOwner::BuiltinAllocationSize(const char* data) noexcept {
        return reinterpret_cast<const TPrivateHeader*>(data - OverheadSize)->AllocSize;
    }

    void TSharedData::IOwner::DeallocateBuiltin(char* data) noexcept {
        auto* header = reinterpret_cast<THeader*>(data - HeaderSize);
        header->Owner.store(nullptr, std::memory_order_relaxed);
        TSharedData::Deallocate(data);
    }
}
