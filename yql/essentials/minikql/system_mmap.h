#pragma once

#include <util/generic/fwd.h>

#include <cstddef>

namespace NKikimr {

class TSystemMmap {
public:
    void* Mmap(size_t size);
    int Munmap(void* addr, size_t size) noexcept;

    static TSystemMmap& GetInstance();
};

size_t GetMemoryMapsCount();
TString GetMemoryMapsString();

} // namespace NKikimr
