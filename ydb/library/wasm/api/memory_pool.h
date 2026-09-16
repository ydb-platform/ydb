#pragma once

#include "compartment.h"

#include <vector>

#include <util/generic/noncopyable.h>

namespace NYdb::NWasm {

////////////////////////////////////////////////////////////////////////////////

//! Manages allocations with respect to the difference between
//! memory addresses inside the VM and inside the host process.
//! We intentionally define a separate class to work with memory pools.
class TWebAssemblyMemoryPool
    : public TNonCopyable
{
public:
    TWebAssemblyMemoryPool();
    explicit TWebAssemblyMemoryPool(IWebAssemblyCompartment* compartment);

    Y_WEAK ~TWebAssemblyMemoryPool();
    TWebAssemblyMemoryPool(TWebAssemblyMemoryPool&& other) noexcept;
    TWebAssemblyMemoryPool& operator=(TWebAssemblyMemoryPool&& other) noexcept;

    char* AllocateUnaligned(size_t size);
    char* AllocateAligned(size_t size, int align = 8);

    void Clear();

    size_t GetSize() const;
    size_t GetCapacity() const;

    bool HasCompartment() const;

private:
    IWebAssemblyCompartment* Compartment_ = nullptr;
    size_t Size_ = 0;
    std::vector<uintptr_t> Allocations_;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYdb::NWasm
