#include "kqp_arrow_memory_pool.h"

#include <yql/essentials/minikql/mkql_alloc.h>

#include <util/generic/yexception.h>

namespace NKikimr::NMiniKQL {

arrow::Status TArrowMemoryPool::Allocate(int64_t size, uint8_t** out) {
    Y_ENSURE(size >= 0 && out);
    *out = (uint8_t*)NMiniKQL::MKQLArrowAllocate(size);
    UpdateAllocatedBytes(size);
    return arrow::Status::OK();
}

arrow::Status TArrowMemoryPool::Reallocate(int64_t old_size, int64_t new_size, uint8_t** ptr) {
    Y_ENSURE(old_size >= 0 && new_size >= 0 && ptr);
    auto* res = NMiniKQL::MKQLArrowAllocate(new_size);
    memcpy(res, *ptr, Min(old_size, new_size));
    NMiniKQL::MKQLArrowFree(*ptr, old_size);
    *ptr = (uint8_t*)res;
    UpdateAllocatedBytes(new_size - old_size);
    return arrow::Status::OK();
}

void TArrowMemoryPool::Free(uint8_t* buffer, int64_t size) {
    Y_ENSURE(size >= 0);
    NMiniKQL::MKQLArrowFree(buffer, size);
    UpdateAllocatedBytes(-size);
}

} // namespace NKikimr::NMiniKQL
