#pragma once

#include "block_allocator.h"

#include <util/generic/utility.h>
#include <util/generic/noncopyable.h>

namespace NReverseGeocoder {
    class TPoolAllocator: public TBlockAllocator {
    public:
        TPoolAllocator()
            : Ptr_(nullptr)
            , Size_(0)
        {
        }

        TPoolAllocator(TPoolAllocator&& a)
            : TBlockAllocator(std::forward<TBlockAllocator>(a))
            , Ptr_(nullptr)
            , Size_(0)
        {
            DoSwap(Ptr_, a.Ptr_);
            DoSwap(Size_, a.Size_);
        }

        TPoolAllocator& operator=(TPoolAllocator&& a) {
            TBlockAllocator::operator=(std::forward<TBlockAllocator>(a));
            DoSwap(Ptr_, a.Ptr_);
            DoSwap(Size_, a.Size_);
            return *this;
        }

        explicit TPoolAllocator(size_t poolSize);

        ~TPoolAllocator() override;

    private:
        char* Ptr_;
        size_t Size_;
    };

}
