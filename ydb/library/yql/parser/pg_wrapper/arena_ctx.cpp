#include "arena_ctx.h"

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "nodes/memnodes.h"
#include "utils/memutils.h"
}

namespace NYql {

void *MyAllocSetAlloc(MemoryContext context, Size size) {
    auto fullSize = size + MAXIMUM_ALIGNOF - 1 + sizeof(void*);
    auto ptr = TArenaMemoryContext::GetCurrentPool().Allocate(fullSize);
    auto aligned = (void*)MAXALIGN(ptr + sizeof(void*));
    *(MemoryContext *)(((char *)aligned) - sizeof(void *)) = context;
    return aligned;
}

void MyAllocSetFree(MemoryContext context, void* pointer) {
}

void* MyAllocSetRealloc(MemoryContext context, void* pointer, Size size) {
    if (!size) {
        return nullptr;
    }

    void* ret = MyAllocSetAlloc(context, size);
    if (pointer) {
        memmove(ret, pointer, size);
    }

    return ret;
}

void MyAllocSetReset(MemoryContext context) {
}

void MyAllocSetDelete(MemoryContext context) {
}

Size MyAllocSetGetChunkSpace(MemoryContext context, void* pointer) {
    return 0;
}

bool MyAllocSetIsEmpty(MemoryContext context) {
    return false;
}

void MyAllocSetStats(MemoryContext context,
    MemoryStatsPrintFunc printfunc, void *passthru,
    MemoryContextCounters *totals,
    bool print_to_stderr) {
}

void MyAllocSetCheck(MemoryContext context) {
}

const MemoryContextMethods MyMethods = {
    MyAllocSetAlloc,
    MyAllocSetFree,
    MyAllocSetRealloc,
    MyAllocSetReset,
    MyAllocSetDelete,
    MyAllocSetGetChunkSpace,
    MyAllocSetIsEmpty,
    MyAllocSetStats
#ifdef MEMORY_CONTEXT_CHECKING
    ,MyAllocSetCheck
#endif
};

__thread TArenaMemoryContext* TArenaMemoryContext::Current = nullptr;

TArenaMemoryContext::TArenaMemoryContext() {
    MyContext = (MemoryContext)malloc(sizeof(MemoryContextData));
    MemoryContextCreate(MyContext,
        T_AllocSetContext,
        &MyMethods,
        nullptr,
        "arena");
    Acquire();
}

TArenaMemoryContext::~TArenaMemoryContext() {
    Release();
    free(MyContext);
}

void TArenaMemoryContext::Acquire() {
    PrevContext = CurrentMemoryContext;
    CurrentMemoryContext = MyContext;
    Prev = Current;
    Current = this;
}

void TArenaMemoryContext::Release() {
    CurrentMemoryContext = PrevContext;
    PrevContext = nullptr;
    Current = Prev;
    Prev = nullptr;
}

}
