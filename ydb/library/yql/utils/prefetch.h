#pragma once

namespace NYql {

inline void PrefetchForRead(const void* ptr) {
    __builtin_prefetch(ptr, 0, 3);
}

inline void PrefetchForWrite(void* ptr) {
    __builtin_prefetch(ptr, 1, 3);
}

} // namespace NYql
