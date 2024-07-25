#pragma once

#include <ydb/library/actors/core/defs.h>

namespace NKikimr::NMemory {

struct TProcessMemoryInfo {
    ui64 AllocatedMemory;
    ui64 AllocatorCachesMemory;
    std::optional<ui64> AnonRss;
    std::optional<ui64> CGroupLimit;
};

struct IProcessMemoryInfoProvider : public TThrRefBase {
    virtual TProcessMemoryInfo Get() const = 0;
};

struct TProcessMemoryInfoProvider : public IProcessMemoryInfoProvider {
    TProcessMemoryInfo Get() const override;
};

}