#include "memory.h"
#include "pool_allocator.h"

#include <util/generic/yexception.h>

using namespace NReverseGeocoder;

NReverseGeocoder::TPoolAllocator::TPoolAllocator(size_t poolSize) {
    Ptr_ = new char[poolSize];
    Size_ = poolSize;
    Setup(Ptr_, Size_);
}

NReverseGeocoder::TPoolAllocator::~TPoolAllocator() {
    if (Ptr_)
        delete[] Ptr_;
}
