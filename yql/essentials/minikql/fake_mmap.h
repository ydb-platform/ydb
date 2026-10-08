#pragma once

#include <util/generic/yexception.h>

#include <cstddef>
#include <expected>
#include <functional>

namespace NKikimr {

class TFakeMmap {
public:
    std::function<std::expected<void*, TSystemError>(size_t size)> OnMmap;
    std::function<std::expected<void, TSystemError>(void* addr, size_t size)> OnMunmap;
    std::function<std::expected<void, TSystemError>(void* addr, size_t size)> OnFreeze;
    std::function<std::expected<void, TSystemError>(void* addr, size_t size)> OnUnfreeze;

    std::expected<void*, TSystemError> Mmap(size_t size);
    std::expected<void, TSystemError> Munmap(void* addr, size_t size) noexcept;
    std::expected<void, TSystemError> Freeze(void* addr, size_t size) noexcept;
    std::expected<void, TSystemError> Unfreeze(void* addr, size_t size) noexcept;

    static TFakeMmap& GetInstance();
};

} // namespace NKikimr
