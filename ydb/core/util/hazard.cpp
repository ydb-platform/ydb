#include "hazard.h"

namespace NKikimr {

THazardDomain::TChunk* THazardDomain::AllocateChunk(size_t offset, size_t count) {
    size_t bytes = TChunk::PointersOffset() + sizeof(THazardPointer) * count;

    void* base = ::operator new(bytes);
    auto* chunk = new (base) TChunk(offset, count);

    auto* ptr = chunk->Pointers();
    for (size_t i = 0; i < count; ++i, ++ptr) {
        new (ptr) THazardPointer();
    }
    return chunk;
}

void THazardDomain::FreeChunk(TChunk* chunk) noexcept {
    auto* ptr = chunk->Pointers();
    for (size_t i = 0; i < chunk->Count; ++i, ++ptr) {
        ptr->~THazardPointer();
    }
    void* base = chunk;
    ::operator delete(base);
}

THazardDomain::~THazardDomain() noexcept {
    auto* chunk = GlobalList.exchange(nullptr, std::memory_order_acquire);
    while (chunk) {
        auto* next = chunk->Next;
        FreeChunk(chunk);
        chunk = next;
    }
}

THazardPointer* THazardDomain::Acquire() {
    auto& cache = LocalCache.Get();

    if (auto* ptr = cache.AcquireCached()) {
        return ptr;
    }

    if (auto* ptr = FreeList.Pop()) {
        return ptr;
    }

    // Claim a new index, it will belong to us until released
    size_t index = GlobalCount.fetch_add(1);

    // Try to find index in our chunk list
    auto* head = GlobalList.load(std::memory_order_acquire);
    while (!head || head->Offset + head->Count <= index) {
        size_t offset = head ? head->Offset + head->Count : 0;
        size_t count = head ? Min(head->Count * 2, MaxChunkSize) : MinChunkSize;
        auto* chunk = AllocateChunk(offset, count);
        chunk->Next = head;
        if (GlobalList.compare_exchange_strong(head, chunk, std::memory_order_acq_rel)) {
            head = chunk;
            break;
        }
        FreeChunk(chunk);
    }

    while (index < head->Offset) {
        // coverity[overwrite_var : FALSE]: false positive, reported to coverity
        head = head->Next;
        Y_ABORT_UNLESS(head, "Unexpected failure to find index %" PRISZT " in chunk list", index);
    }
    Y_ABORT_UNLESS(head->Offset <= index && index < head->Offset + head->Count);

    return head->Pointers() + (index - head->Offset);
}

void THazardDomain::Release(THazardPointer* ptr) noexcept {
    Y_DEBUG_ABORT_UNLESS(ptr->ProtectedPointer.load(std::memory_order_relaxed) == nullptr,
        "Make sure pointer is cleared before it is released");

    auto& cache = LocalCache.Get();

    if (cache.Size() < MaxCached) {
        cache.ReleaseCached(ptr);
        return;
    }

    FreeList.Push(ptr);
}

void THazardDomain::DrainLocalCache() {
    auto& cache = LocalCache.Get();

    while (auto* ptr = cache.AcquireCached()) {
        FreeList.Push(ptr);
    }
}

}
