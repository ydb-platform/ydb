#pragma once

#include <cstddef>

#include <util/system/compiler.h>
#include <util/system/yassert.h>

#if defined(_asan_enabled_)
    #include <sanitizer/asan_interface.h>
#endif

namespace NKikimr {

inline constexpr size_t ALLOCATION_REDZONE_SIZE = 16;
inline constexpr size_t ASAN_EXTRA_ALLOCATION_SPACE = ALLOCATION_REDZONE_SIZE * 2;

constexpr void* SanitizerMarkInvalid(void* addr, size_t size) {
#if defined(_asan_enabled_)
    if (addr == nullptr) {
        return nullptr;
    }
    __asan_poison_memory_region(addr, size);
#else // defined(_asan_enabled_)
    Y_UNUSED(addr, size);
#endif
    return addr;
}

constexpr void* SanitizerMarkValid(void* addr, size_t size) {
#if defined(_asan_enabled_)
    if (addr == nullptr) {
        return nullptr;
    }
    __asan_unpoison_memory_region(addr, size);
#else // defined(_asan_enabled_)
    Y_UNUSED(addr, size);
#endif
    return addr;
}

constexpr size_t GetSizeToAlloc(size_t size) {
#if defined(_asan_enabled_)
    if (size == 0) {
        return 0;
    }
    return size + 2 * ALLOCATION_REDZONE_SIZE;
#else // defined(_asan_enabled_)
    return size;
#endif
}

constexpr const void* GetOriginalAllocatedObject(const void* ptr, size_t size) {
#if defined(_asan_enabled_)
    if (size == 0) {
        return ptr;
    }
    return (char*)ptr - ALLOCATION_REDZONE_SIZE;
#else // defined(_asan_enabled_)
    Y_UNUSED(size);
    return ptr;
#endif
}

constexpr void* WrapPointerWithRedZones(void* ptr, size_t extendedSizeWithRedzone) {
#if defined(_asan_enabled_)
    if (extendedSizeWithRedzone == 0) {
        return ptr;
    }
    SanitizerMarkInvalid(ptr, extendedSizeWithRedzone);
    SanitizerMarkValid((char*)ptr + ALLOCATION_REDZONE_SIZE, extendedSizeWithRedzone - 2 * ALLOCATION_REDZONE_SIZE);
    return (char*)ptr + ALLOCATION_REDZONE_SIZE;
#else // defined(_asan_enabled_)
    Y_UNUSED(extendedSizeWithRedzone);
    return ptr;
#endif
}

constexpr const void* UnwrapPointerWithRedZones(const void* ptr, size_t size) {
#if defined(_asan_enabled_)
    if (size == 0) {
        return ptr;
    }
    SanitizerMarkInvalid((char*)ptr - ALLOCATION_REDZONE_SIZE, 2 * ALLOCATION_REDZONE_SIZE + size);
    return (char*)ptr - ALLOCATION_REDZONE_SIZE;
#else // defined(_asan_enabled_)
    Y_UNUSED(size);
    return ptr;
#endif
}

} // namespace NKikimr
