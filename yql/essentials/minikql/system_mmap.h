#pragma once

#include <util/generic/fwd.h>
#include <util/generic/yexception.h>

#include <cstddef>
#include <expected>

namespace NKikimr {

class TSystemMmap {
public:
    std::expected<void*, TSystemError> Mmap(size_t size);
    std::expected<void, TSystemError> Munmap(void* addr, size_t size);
    std::expected<void, TSystemError> Freeze(void* addr, size_t size);
    std::expected<void, TSystemError> Unfreeze(void* addr, size_t size);

    static TSystemMmap& GetInstance();
};

size_t GetMemoryMapsCount();
TString GetMemoryMapsString();

} // namespace NKikimr
