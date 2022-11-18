#include "mkql_memory_pool.h"

namespace NKikimr::NMiniKQL {

namespace {
class TArrowMemoryPool : public arrow::MemoryPool {
public:
    explicit TArrowMemoryPool(TAllocState& allocState)
        : ArrowMemoryUsage(allocState.ArrowMemoryUsage) {
    }

    arrow::Status Allocate(int64_t size, uint8_t** out) override {
        *out = static_cast<uint8_t*>(malloc(size));
        if (auto p = ArrowMemoryUsage.lock()) {
            *p += size;
        }

        return arrow::Status();
    }

    arrow::Status Reallocate(int64_t oldSize, int64_t newSize, uint8_t** ptr) override {
        void* newPtr = malloc(newSize);
        ::memcpy(newPtr, *ptr, std::min(oldSize, newSize));
        free(*ptr);
        *ptr = static_cast<uint8_t*>(newPtr);
        if (auto p = ArrowMemoryUsage.lock()) {
            *p -= oldSize;
            *p += newSize;
        }

        return arrow::Status();
    }

    void Free(uint8_t* buffer, int64_t size) override {
        free(buffer);
        if (auto p = ArrowMemoryUsage.lock()) {
            *p -= size;
        }
    }

    int64_t bytes_allocated() const override {
        if (auto p = ArrowMemoryUsage.lock()) {
            return *p;
        }

        return 0;
    }

    int64_t max_memory() const override {
        return -1;
    }

    std::string backend_name() const override {
        return "MKQL";
    }

private:
    std::weak_ptr<std::atomic<size_t>> ArrowMemoryUsage;
};
}

std::unique_ptr <arrow::MemoryPool> MakeArrowMemoryPool(TAllocState& allocState) {
    return std::make_unique<TArrowMemoryPool>(allocState);
}

}
