#include "arena_ctx.h"
#include <util/generic/yexception.h>

#define TypeName PG_TypeName
#define SortBy PG_SortBy
#undef SIZEOF_SIZE_T
extern "C" {
#include "postgres.h"
#include "nodes/memnodes.h"
#include "utils/memutils.h"
#include "utils/memutils_internal.h"
}

namespace NYql {

struct TArenaPAllocHeader {
    size_t Size;
    ui64 Self; // should be placed right before pointer to allocated area, see GetMemoryChunkContext
};

static_assert(sizeof(TArenaPAllocHeader) == sizeof(size_t) + sizeof(MemoryContext), "Padding is not allowed");

extern "C" {
extern void *ArenaAlloc(MemoryContext context, Size size);
extern void ArenaFree(void *pointer);
extern void *ArenaRealloc(void *pointer, Size size);
extern void ArenaReset(MemoryContext context);
extern void ArenaDelete(MemoryContext context);
extern MemoryContext ArenaGetChunkContext(void *pointer);
extern Size ArenaGetChunkSpace(void *pointer);
extern bool ArenaIsEmpty(MemoryContext context);
extern void ArenaStats(MemoryContext context,
						  MemoryStatsPrintFunc printfunc, void *passthru,
						  MemoryContextCounters *totals,
						  bool print_to_stderr);
#ifdef MEMORY_CONTEXT_CHECKING
extern void ArenaCheck(MemoryContext context);
#endif
}

extern "C" void *ArenaAlloc(MemoryContext context, Size size) {
    Y_UNUSED(context);
    auto fullSize = size + MAXIMUM_ALIGNOF - 1 + sizeof(TArenaPAllocHeader);
    auto ptr = TArenaMemoryContext::GetCurrentPool().Allocate(fullSize);
    auto aligned = (TArenaPAllocHeader*)MAXALIGN(ptr + sizeof(TArenaPAllocHeader));
    Y_ENSURE((ui64(context) & MEMORY_CONTEXT_METHODID_MASK) == 0);
    aligned[-1].Self = ui64(context) | MCTX_UNUSED2_ID;
    aligned[-1].Size = size;
    return aligned;
}

extern "C" void ArenaFree(void* pointer) {
    Y_UNUSED(pointer);
}

extern "C" void* ArenaRealloc(void* pointer, Size size) {
    if (!size) {
        return nullptr;
    }

    void* ret = ArenaAlloc(nullptr, size);
    if (pointer) {
        auto prevSize = ((const TArenaPAllocHeader*)pointer)[-1].Size;
        memmove(ret, pointer, prevSize);
    }

    return ret;
}

extern "C" void ArenaReset(MemoryContext context) {
    Y_UNUSED(context);
}

extern "C" void ArenaDelete(MemoryContext context) {
    Y_UNUSED(context);
}

extern "C" MemoryContext ArenaGetChunkContext(void *pointer) {
    return (MemoryContext)(((ui64*)pointer)[-1] & ~MEMORY_CONTEXT_METHODID_MASK);
}

extern "C" Size ArenaGetChunkSpace(void* pointer) {
    Y_UNUSED(pointer);
    return 0;
}

extern "C" bool ArenaIsEmpty(MemoryContext context) {
    Y_UNUSED(context);
    return false;
}

extern "C" void ArenaStats(MemoryContext context,
    MemoryStatsPrintFunc printfunc, void *passthru,
    MemoryContextCounters *totals,
    bool print_to_stderr) {
    Y_UNUSED(context);
    Y_UNUSED(printfunc);
    Y_UNUSED(passthru);
    Y_UNUSED(totals);
    Y_UNUSED(print_to_stderr);
}

extern "C" void ArenaCheck(MemoryContext context) {
    Y_UNUSED(context);
}

__thread TArenaMemoryContext* TArenaMemoryContext::Current = nullptr;

TArenaMemoryContext::TArenaMemoryContext() {
    MyContext = (MemoryContext)malloc(sizeof(MemoryContextData));
    static_assert(MEMORY_CONTEXT_METHODID_MASK < sizeof(void*));
    MemoryContextCreate(MyContext,
        T_AllocSetContext,
        MCTX_UNUSED2_ID,
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
