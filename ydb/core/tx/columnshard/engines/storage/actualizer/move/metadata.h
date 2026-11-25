#pragma once

#include <util/generic/hash.h>
#include <util/system/types.h>

#include <cstddef>
#include <memory>

namespace NKikimr::NOlap {
class TWrittenPortionInfo;
using TWrittenPortionsById = THashMap<ui64, std::shared_ptr<TWrittenPortionInfo>>;
}

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
        if (PortionsLeft == 0) {
            return true;
        }
        // Apply the memory limit after the first request so an oversized portion can make progress.
        return RequestedPortions != 0 && Memory >= MemoryLimit;
    }
};

}   // namespace NKikimr::NOlap::NActualizer
