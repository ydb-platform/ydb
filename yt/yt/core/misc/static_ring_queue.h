#pragma once
#include "common.h"

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

template <class T, size_t Capacity>
class TStaticRingQueue
{
public:
    void Append(const T* begin, const T* end);
    void CopyTailTo(size_t copySize, T* begin) const;
    size_t Size() const;

private:
    T Buffer_[Capacity];
    size_t EndOffset_ = 0;
    size_t Size_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define STATIC_RING_QUEUE_INL_H_
#include "static_ring_queue-inl.h"
#undef STATIC_RING_QUEUE_INL_H_
