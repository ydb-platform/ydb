#pragma once

#include <cstddef>
#include <functional>

namespace NKikimr {

class TFakeMmap {
public:
    std::function<void*(size_t size)> OnMmap;
    std::function<void(void* addr, size_t size)> OnMunmap;

    void* Mmap(size_t size);
    int Munmap(void* addr, size_t size) noexcept;

    static TFakeMmap& GetInstance();
};

} // namespace NKikimr
