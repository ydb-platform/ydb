#include "memory_pool.h"

#include <ydb/library/actors/core/log.h>
#include <ydb/library/services/services.pb.h>

namespace {

    const uint8_t* Unwrap(std::shared_ptr<arrow::Buffer> buffer) {
        while (buffer->parent()) {
            buffer = buffer->parent();
        }

        return buffer->data();
    }

} // namespace

namespace NKikimr::NArrow {

arrow::Status TMemoryPool::Allocate(int64_t size, uint8_t** out) {
    Y_ENSURE(size >= 0 && out);
    *out = (uint8_t*)NMiniKQL::MKQLArrowAllocateUntracked(size);
    UpdateAllocatedBytes(size);
    return arrow::Status::OK();
}

arrow::Status TMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    Y_ENSURE(old_size >= 0 && new_size >= 0 && ptr);
    auto* res = NMiniKQL::MKQLArrowAllocateUntracked(new_size);
    memcpy(res, *ptr, Min(old_size, new_size));
    NMiniKQL::MKQLArrowFree(*ptr, old_size);
    *ptr = (uint8_t*)res;
    UpdateAllocatedBytes(new_size - old_size);
    return arrow::Status::OK();
}

void TMemoryPool::Free(uint8_t* buffer, int64_t size) {
    Y_ENSURE(size >= 0);
    if (!NMiniKQL::MKQLArrowFree(buffer, size, false)) {
        ALS_DEBUG(NKikimrServices::ARROW_HELPER) << "Failed to free arrow-buffer immediately, size: " << size;
    }
    UpdateAllocatedBytes(-size);
}

void TMemoryPool::Track(const std::shared_ptr<arrow::Table>& table) {
    for (const auto& column: table->columns()) {
        for (const auto& chunk: column->chunks()) {
            for (const auto& buffer: chunk->data()->buffers) {
                if (buffer) {
                    NMiniKQL::MKQLArrowTrack(Unwrap(buffer));
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
