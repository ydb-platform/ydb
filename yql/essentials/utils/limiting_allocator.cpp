#include "limiting_allocator.h"

#include <util/memory/pool.h>
#include <util/generic/yexception.h>

namespace {
class TLimitingAllocator : public IAllocator {
public:
    TLimitingAllocator(size_t limit, IAllocator* allocator) : Alloc_(allocator), Limit_(limit) {};
    TBlock Allocate(size_t len) override final {
        if (Allocated_ + len > Limit_) {
            throw std::runtime_error("Out of memory");
        }
        Allocated_ += len;
        return Alloc_->Allocate(len);
    }

    void Release(const TBlock& block) override final {
        Y_ENSURE(Allocated_ >= block.Len);
        Allocated_ -= block.Len;
        Alloc_->Release(block);
    }

private:
    IAllocator* Alloc_;
    size_t Allocated_ = 0;
    const size_t Limit_;
};
}

namespace NYql {
std::unique_ptr<IAllocator> MakeLimitingAllocator(size_t limit, IAllocator* underlying) {
    return std::make_unique<TLimitingAllocator>(limit, underlying);
}
}
