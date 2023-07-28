#pragma once

#include "public.h"

#include <library/cpp/yt/memory/public.h>

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Single producer single consumer lock-free queue.
template <class T>
class TSpscQueue
{
public:
    TSpscQueue(const TSpscQueue&) = delete;
    void operator=(const TSpscQueue&) = delete;

    TSpscQueue();
    ~TSpscQueue();

    Y_FORCE_INLINE void Push(T&& element);

    Y_FORCE_INLINE T* Front() const;
    Y_FORCE_INLINE void Pop();

    Y_FORCE_INLINE bool IsEmpty() const;

private:
    struct TNode;

    static constexpr size_t BufferSize = 128;

    mutable TNode* Head_;
    TNode* Tail_;
    size_t Offset_ = 0;
    mutable size_t CachedCount_ = 0;

    // Avoid false sharing.
    char Padding[CacheLineSize - 4 * sizeof(void*)];

    std::atomic<size_t> Count_ = 0;
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define SPSC_QUEUE_INL_H_
#include "spsc_queue-inl.h"
#undef SPSC_QUEUE_INL_H_
