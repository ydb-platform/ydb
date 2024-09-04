#pragma once

#include "public.h"

#include <atomic>

namespace NYT {

////////////////////////////////////////////////////////////////////////////////

//! Multiple producer single consumer lock-free stack.
template <class T>
class TMpscStack
{
public:
    TMpscStack(const TMpscStack&) = delete;
    void operator=(const TMpscStack&) = delete;

    TMpscStack() = default;
    ~TMpscStack();

    void Enqueue(const T& value);
    void Enqueue(T&& value);

    bool TryDequeue(T* value);
    std::vector<T> DequeueAll(bool reverse = false);
    template <class F>
    bool DequeueAll(bool reverse, F&& functor);

    template <class F>
    void FilterElements(F&& functor);

    bool IsEmpty() const;

private:
    struct TNode;

    std::atomic<TNode*> Head_ = nullptr;

    void DoEnqueue(TNode* head, TNode* tail);

    template <class F>
    bool DoDequeueAll(bool reverse, F&& functor);
};

////////////////////////////////////////////////////////////////////////////////

} // namespace NYT

#define MPSC_STACK_INL_H_
#include "mpsc_stack-inl.h"
#undef MPSC_STACK_INL_H_
