#pragma once

#ifndef CACHE_LINE_SIZE
#define CACHE_LINE_SIZE 64
#endif

#define CONCAT(a, b) a##b
#define LABEL(a) CONCAT(UniqueName_, a)
#define UNIQUE_NAME LABEL(__LINE__)

#define CACHE_LINE_PADDING char UNIQUE_NAME[CACHE_LINE_SIZE];

static inline void* MallocAligned(size_t size, size_t alignment) {
    void** ptr = (void**)malloc(size + alignment + sizeof(size_t*));
    if (!ptr) {
        return nullptr;
    }

    size_t mask = ~(alignment - 1);
    intptr_t roundedDown = intptr_t(ptr) & mask;
    void** alignedPtr = (void**)(roundedDown + alignment);
    alignedPtr[-1] = ptr;
    return alignedPtr;
}

static inline void FreeAligned(void* ptr) {
    if (!ptr) {
        return;
    }

    void** typedPtr = (void**)ptr;
    void* originalPtr = typedPtr[-1];
    free(originalPtr);
}

static inline void* MallocCacheAligned(size_t size) {
    return MallocAligned(size, CACHE_LINE_SIZE);
}

static inline void FreeCacheAligned(void* ptr) {
    return FreeAligned(ptr);
}
