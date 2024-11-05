#ifndef STATIC_RING_QUEUE_INL_H_
#error "Direct inclusion of this file is not allowed, include static_ring_queue.h"
// For the sake of sane code completion.
#include "static_ring_queue.h"
#endif
#undef STATIC_RING_QUEUE_INL_H_

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t Capacity>
void TStaticRingQueue<T, Capacity>::Append(const T* begin, const T* end)
{
    // Determine input tail.
    if (std::distance(begin, end) > static_cast<i64>(Capacity)) {
        begin = end - Capacity;
    }

    size_t appendSize = std::distance(begin, end);

    if (Size_ > Capacity - appendSize) {
        Size_ = Capacity;
    } else {
        Size_ += appendSize;
    }

    EndOffset_ += appendSize;
    if (EndOffset_ >= Capacity) {
        EndOffset_ -= Capacity;
        YT_VERIFY(EndOffset_ < Capacity);
    }

    size_t tailSize = std::min<size_t>(EndOffset_, appendSize);

    std::copy(end - tailSize, end, Buffer_ + EndOffset_ - tailSize);
    end -= tailSize;
    std::copy(begin, end, Buffer_ + Capacity - (end - begin));
}

template <class T, size_t Capacity>
void TStaticRingQueue<T, Capacity>::CopyTailTo(size_t copySize, T* begin) const
{
    YT_VERIFY(copySize <= Size_);

    if (copySize > EndOffset_) {
        size_t tailSize = copySize - EndOffset_;
        std::copy(Buffer_ + Capacity - tailSize, Buffer_ + Capacity, begin);
        std::copy(Buffer_, Buffer_ + EndOffset_, begin + tailSize);
    } else {
        std::copy(Buffer_ + EndOffset_ - copySize, Buffer_ + EndOffset_, begin);
    }
}

template <class T, size_t Capacity>
size_t TStaticRingQueue<T, Capacity>::Size() const
{
    return Size_;
}

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT
