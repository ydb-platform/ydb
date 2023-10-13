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

struct TArenaPAllocHeader {
    size_t Size;
    MemoryContext Self; // should be placed right before pointer to allocated area, see GetMemoryChunkContext
};

static_assert(sizeof(TArenaPAllocHeader) == sizeof(size_t) + sizeof(MemoryContext), "Padding is not allowed");

void *MyAllocSetAlloc(MemoryContext context, Size size) {
    auto fullSize = size + MAXIMUM_ALIGNOF - 1 + sizeof(TArenaPAllocHeader);
    auto ptr = TArenaMemoryContext::GetCurrentPool().Allocate(fullSize);
    auto aligned = (TArenaPAllocHeader*)MAXALIGN(ptr + sizeof(TArenaPAllocHeader));
    aligned[-1].Self = context;
    aligned[-1].Size = size;
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
        auto prevSize = ((const TArenaPAllocHeader*)pointer)[-1].Size;
        memmove(ret, pointer, prevSize);
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
