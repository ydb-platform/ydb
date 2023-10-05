#pragma once
#include <util/memory/segmented_string_pool.h>

struct MemoryContextData;
typedef struct MemoryContextData *MemoryContext;

namespace NYql {

class TArenaMemoryContext {
public:
    TArenaMemoryContext();
    ~TArenaMemoryContext();
    static segmented_string_pool& GetCurrentPool() {
        return Current->Pool;
    }

    void Acquire();
    void Release();

private:
    segmented_string_pool Pool;
    MemoryContext PrevContext = nullptr;
    MemoryContext MyContext = nullptr;
    static __thread TArenaMemoryContext* Current;
    TArenaMemoryContext* Prev = nullptr;
};

}
