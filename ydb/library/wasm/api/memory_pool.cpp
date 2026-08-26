#include "memory_pool.h"

#include "compartment.h"
#include "pointer.h"

#include <utility>

#include <util/system/align.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

TWebAssemblyMemoryPool::TWebAssemblyMemoryPool()
    : Compartment_(GetCurrentCompartment())
{ }

TWebAssemblyMemoryPool::TWebAssemblyMemoryPool(IWebAssemblyCompartment* compartment)
    : Compartment_(compartment)
{ }

Y_WEAK TWebAssemblyMemoryPool::~TWebAssemblyMemoryPool()
{
    try {
        Clear();
    } catch (...) {
        // FreeBytes may throw; never throw from dtor.
    }
}

TWebAssemblyMemoryPool::TWebAssemblyMemoryPool(TWebAssemblyMemoryPool&& other) noexcept
    : Compartment_(std::exchange(other.Compartment_, nullptr))
    , Size_(std::exchange(other.Size_, 0))
    , Allocations_(std::move(other.Allocations_))
{ }

TWebAssemblyMemoryPool& TWebAssemblyMemoryPool::operator=(TWebAssemblyMemoryPool&& other) noexcept
{
    if (this != &other) {
        try {
            Clear();
        } catch (...) {
            // FreeBytes may throw; never throw from noexcept move-assignment.
        }
        Compartment_ = std::exchange(other.Compartment_, nullptr);
        Size_ = std::exchange(other.Size_, 0);
        Allocations_ = std::move(other.Allocations_);
    }
    return *this;
}

void TWebAssemblyMemoryPool::Clear()
{
    if (Compartment_) {
        for (const auto& address : Allocations_) {
            Compartment_->FreeBytes(address);
        }
    }
    Allocations_.clear();
    Size_ = 0;
}

size_t TWebAssemblyMemoryPool::GetSize() const
{
    return Size_;
}

size_t TWebAssemblyMemoryPool::GetCapacity() const
{
    return Size_;
}

bool TWebAssemblyMemoryPool::HasCompartment() const
{
    return Compartment_;
}

char* TWebAssemblyMemoryPool::AllocateUnaligned(size_t size)
{
    auto offset = Compartment_->AllocateBytes(size);
    Allocations_.push_back(offset);
    Size_ += size;
    return std::bit_cast<char*>(offset);
}

char* TWebAssemblyMemoryPool::AllocateAligned(size_t size, int align)
{
    uintptr_t unaligned = Compartment_->AllocateBytes(size + align);
    Allocations_.push_back(unaligned);
    auto aligned = AlignUp(std::bit_cast<char*>(unaligned), align);
    Size_ += size + align;
    return std::bit_cast<char*>(aligned);
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
