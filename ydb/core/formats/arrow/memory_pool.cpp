#include "memory_pool.h"

namespace NKikimr::NArrow {

arrow::Status TMemoryPool::Allocate(int64_t size, uint8_t** out) {
    Y_ENSURE(size >= 0 && out);
    *out = (uint8_t*)NMiniKQL::MKQLArrowAllocateUntracked(size);
    UpdateAllocatedBytes(size);

    {
        TGuard<TMutex> Lock(Mutex_);
        Y_ABORT_UNLESS(Buffers_.insert(*out).second);
    }

    return arrow::Status::OK();
}

arrow::Status TMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    Y_ENSURE(old_size >= 0 && new_size >= 0 && ptr);
    auto* res = NMiniKQL::MKQLArrowAllocateUntracked(new_size);

    {
        TGuard<TMutex> Lock(Mutex_);
        Y_ABORT_UNLESS(Buffers_.insert(res).second);
        Y_ABORT_UNLESS(Buffers_.erase(*ptr));
    }

    memcpy(res, *ptr, Min(old_size, new_size));
    NMiniKQL::MKQLArrowFree(*ptr, old_size);
    *ptr = (uint8_t*)res;
    UpdateAllocatedBytes(new_size - old_size);
    return arrow::Status::OK();
}

void TMemoryPool::Free(uint8_t* buffer, int64_t size) {
    {
        TGuard<TMutex> Lock(Mutex_);
        Y_ABORT_UNLESS(Buffers_.erase(buffer));
    }

    Y_ENSURE(size >= 0);
    if (!NMiniKQL::MKQLArrowFree(buffer, size, false)) {
        // TODO: log failure
    }
    UpdateAllocatedBytes(-size);
}

void TMemoryPool::Track(const std::shared_ptr<arrow::Table>& table) {
    TGuard<TMutex> Lock(Mutex_);

    for (const auto& column: table->columns()) {
        for (const auto& chunk: column->chunks()) {
            for (const auto& buffer: chunk->data()->buffers) {
                if (buffer) {
                    if (!Buffers_.contains(buffer->data())) {
                        continue;
                    }
                    NMiniKQL::MKQLArrowTrack(buffer->data());
                }
            }
        }
    }
}

} // namespace NKikimr::NArrow

// Override weak function from contrib/arrow
namespace arrow {
    MemoryPool* default_memory_pool() {
        return NKikimr::NArrow::GetMemoryPool();
    }
} // namespace arrow
