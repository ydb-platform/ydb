#include "limiting_allocator.h"

#include <util/memory/pool.h>
#include <util/generic/yexception.h>

namespace {
class TLimitingAllocator: public NYql::ILimitingAllocator {
public:
    TLimitingAllocator(size_t limit, IAllocator* allocator)
        : Alloc_(allocator)
        , Limit_(limit)
    {};
    TBlock Allocate(size_t len) final {
        if (Allocated_ + len > Limit_) {
            throw std::runtime_error("Out of memory");
        }
        Allocated_ += len;
        return Alloc_->Allocate(len);
    }

    void Release(const TBlock& block) final {
        Y_ENSURE(Allocated_ >= block.Len);
        Allocated_ -= block.Len;
        Alloc_->Release(block);
    }

    size_t GetAllocatedSize() const final {
        return Allocated_;
    }

    size_t GetLimitSize() const final {
        return Limit_;
    }

private:
    IAllocator* Alloc_;
    size_t Allocated_ = 0;
    const size_t Limit_;
};
} // namespace

namespace NYql {
std::unique_ptr<ILimitingAllocator> MakeLimitingAllocator(size_t limit, IAllocator* underlying) {
    return std::make_unique<TLimitingAllocator>(limit, underlying);
}
} // namespace NYql
