#pragma once

#include "public.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Multiple producer single consumer lock-free queue.
/*!
 *  Internally implemented by a pair of lock-free stack (head) and
 *  linked-list of popped-but-not-yet-dequeued items (tail).
 *
 *  Additionally supports draining during shutdown.
 *
 *  The proper shutdown sequence is as follows:
 *  1) #DrainConsumer must be called by the consumer thread.
 *  2) #DrainProducer must be called by each producer thread
 *  that has just enqueued some items into the queue.
 */
template <class T>
class TMpscQueue final
{
public:
    TMpscQueue(const TMpscQueue&) = delete;
    void operator=(const TMpscQueue&) = delete;

    TMpscQueue() = default;
    ~TMpscQueue();

    void Enqueue(const T& value);
    void Enqueue(T&& value);

    bool TryDequeue(T* value);

    bool IsEmpty() const;

    void DrainConsumer();
    void DrainProducer();

private:
    struct TNode;

    std::atomic<TNode*> Head_ = nullptr;
    TNode* Tail_ = nullptr;

    void DoEnqueue(TNode* node);
    void DeleteNodeList(TNode* node);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MPSC_QUEUE_INL_H_
#include "mpsc_queue-inl.h"
#undef MPSC_QUEUE_INL_H_
