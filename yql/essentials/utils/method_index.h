#pragma once
#include <util/system/platform.h>
#include <cstring>
#include <cstdint>

namespace NYql {

size_t GetMethodPtrIndex(uintptr_t ptr);

template<typename Method>
inline size_t GetMethodIndex(Method method) {
    uintptr_t ptr;
    std::memcpy(&ptr, &method, sizeof(uintptr_t));
    return GetMethodPtrIndex(ptr);
}

template<typename Method>
inline uintptr_t GetMethodPtr(Method method) {
    uintptr_t ptr;
    std::memcpy(&ptr, &method, sizeof(uintptr_t));
    return ptr;
}

}