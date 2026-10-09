#pragma once

#include <cstddef>
#include <cstdint>
#include <new>

// Bounded example-module storage, not an allocator ABI or tenant RSS limit.
namespace NYdb::NWasm::NServices::NExampleAllocator {
inline constexpr unsigned Slots = 32;
alignas(16) inline unsigned char Arena[Slots][65536];
inline bool Used[Slots];
inline uint64_t LiveObjects = 0;
} // namespace NYdb::NWasm::NServices::NExampleAllocator

void* operator new(std::size_t size) {
    using namespace NYdb::NWasm::NServices::NExampleAllocator;
    if (size > sizeof(Arena[0]))
        __builtin_trap();
    for (unsigned i = 0; i < Slots; ++i) {
        if (!Used[i]) {
            Used[i] = true;
            ++LiveObjects;
            return Arena[i];
        }
    }
    __builtin_trap();
}
void* operator new(std::size_t size, std::align_val_t alignment) {
    if (static_cast<size_t>(alignment) > 16)
        __builtin_trap();
    return ::operator new(size);
}
void operator delete(void* pointer) noexcept {
    using namespace NYdb::NWasm::NServices::NExampleAllocator;
    if (!pointer)
        return;
    const auto offset = reinterpret_cast<uintptr_t>(pointer) - reinterpret_cast<uintptr_t>(Arena);
    const auto index = offset / sizeof(Arena[0]);
    if (index >= Slots || offset % sizeof(Arena[0]) || !Used[index])
        __builtin_trap();
    Used[index] = false;
    --LiveObjects;
}
void operator delete(void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void operator delete(void* pointer, std::size_t, std::align_val_t) noexcept {
    ::operator delete(pointer);
}
void* operator new[](std::size_t size) {
    return ::operator new(size);
}
void* operator new[](std::size_t size, std::align_val_t alignment) {
    return ::operator new(size, alignment);
}
void operator delete[](void* pointer) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::size_t) noexcept {
    ::operator delete(pointer);
}
void operator delete[](void* pointer, std::align_val_t alignment) noexcept {
    ::operator delete(pointer, alignment);
}
void operator delete[](void* pointer, std::size_t size, std::align_val_t alignment) noexcept {
    ::operator delete(pointer, size, alignment);
}
