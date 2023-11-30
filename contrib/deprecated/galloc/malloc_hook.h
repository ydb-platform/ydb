#pragma once

class MallocHook {
    public:
        typedef void (*NewHook)(void* ptr, size_t size);
        typedef void (*DeleteHook)(void* ptr);

        inline static void InvokeNewHook(void*, size_t) {
        }

        inline static void InvokeDeleteHook(void*) {
        }
};
