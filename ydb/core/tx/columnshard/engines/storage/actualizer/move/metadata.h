#pragma once

#include <util/system/types.h>

#include <cstddef>

namespace NKikimr::NOlap::NActualizer {

// Shared by all granules in one tablet metadata pass.
struct TMoveDataMetadataBudget {
    size_t PortionsLeft = 1024;
    ui64 Memory = 0;
    ui64 RequestedPortions = 0;
    const ui64 MemoryLimit;

    explicit TMoveDataMetadataBudget(ui64 memoryLimit)
        : MemoryLimit(memoryLimit)
    {
    }

    bool IsExhausted() const noexcept {
        // Allow the first portion even if it exceeds the soft memory limit.
        return !PortionsLeft || (RequestedPortions && Memory >= MemoryLimit);
    }
};

}   // namespace NKikimr::NOlap::NActualizer
